"""Thin coordinator over a core agent and project-scoped agent subprocesses."""

from __future__ import annotations

import asyncio
import uuid
import json
import re
import signal as signal_module
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, TYPE_CHECKING

from nanobot.agent.loop import AgentLoop
from nanobot.agent.tools.batch_delegate import DelegateProjectsBatchTool
from nanobot.agent.tools.batch_status import GetBatchDelegationStatusTool
from nanobot.agent.tools.delegate import DelegateProjectTaskTool
from nanobot.agent.tools.describe_provider_interfaces import DescribeProviderInterfacesTool
from nanobot.agent.tools.register_contract_function_dependency import RegisterContractFunctionDependencyTool
from nanobot.agent.tools.list_project_scopes import ListProjectScopesTool
from nanobot.agent.tools.request_contract_stub import RequestContractStubTool
from nanobot.agent.tools.workflow_state import ManageWorkflowStateTool
from nanobot.bus.events import InboundMessage, OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.config.loader import get_data_dir
from nanobot.providers.base import LLMProvider
from nanobot.session.manager import SessionManager
from nanobot.workflow import (
    ContractRecord,
    DecisionRecord,
    DependencyEdgeRecord,
    WorkflowStore,
    WorkItemRecord,
)

if TYPE_CHECKING:
    from nanobot.config.schema import ExecToolConfig
    from nanobot.cron.service import CronService


@dataclass(frozen=True)
class ProjectScopeRegistration:
    """Fixed registry entry describing one project-agent scope."""

    path: str
    owner: str = ""
    description: str = ""
    prompt_hint: str = ""
    tags: tuple[str, ...] = ()


@dataclass
class ProjectSubprocessHandle:
    """Handle for a running project agent subprocess."""

    project: str
    process: asyncio.subprocess.Process
    pid: int
    started_at: str
    scope_hint: str = ""
    reader_task: asyncio.Task[None] | None = None
    pending_requests: dict[str, "PendingProjectRequest"] = field(default_factory=dict)


@dataclass
class PendingProjectRequest:
    """Tracked request routed through a project subprocess."""

    request_id: str
    project: str
    channel: str
    chat_id: str
    session_key: str
    submitted_at: str
    task_preview: str
    publish_completion_message: bool = True
    future: asyncio.Future[dict[str, Any]] | None = None


@dataclass
class PendingProjectDecision:
    """A decision requested by a project subprocess and waiting for a user reply."""

    decision_id: str
    request_id: str
    project: str
    channel: str
    chat_id: str
    session_key: str
    prompt: str
    options: list[str] = field(default_factory=list)


class CoreAgentManager:
    """Coordinate a core agent and project-scoped agent subprocesses."""

    def __init__(
        self,
        bus: MessageBus,
        provider: LLMProvider,
        workspace: Path,
        model: str | None = None,
        max_iterations: int = 20,
        brave_api_key: str | None = None,
        exec_config: ExecToolConfig | None = None,
        cron_service: CronService | None = None,
        restrict_to_workspace: bool = False,
        session_manager: SessionManager | None = None,
        allowed_project_scopes: list[str] | None = None,
        project_registry: list[Any] | None = None,
        workflow_store: WorkflowStore | None = None,
        scheduler_reconcile_interval_seconds: float = 0.5, # 3.0,
        scheduler_max_concurrent_dispatches: int = 4,
        revalidation_chain_enabled: bool = False,
        revalidation_chain_edge_types: list[str] | None = None,
        revalidation_chain_max_depth: int = 2,
        decision_sla_seconds: int = 1800,
        decision_sla_block_scope: str = "module",
        decision_queue_impact_weight: int = 10,
        decision_queue_age_weight: int = 1,
        decision_default_degradation: str = "wait",
    ):
        from nanobot.config.schema import ExecToolConfig

        self.bus = bus
        self.provider = provider
        self.workspace = workspace.expanduser().resolve()
        self.model = model
        self.max_iterations = max_iterations
        self.brave_api_key = brave_api_key
        self.exec_config = exec_config or ExecToolConfig()
        self.cron_service = cron_service
        self.restrict_to_workspace = restrict_to_workspace
        self.sessions = session_manager or SessionManager(self.workspace)
        self.workflow_store = workflow_store or WorkflowStore(get_data_dir() / "workflow" / "state.db")
        self.project_loops: dict[str, AgentLoop] = {}
        self._project_subprocesses: dict[str, ProjectSubprocessHandle] = {}
        self._pending_project_decisions: dict[str, PendingProjectDecision] = {}
        self._next_subprocess_req_id: int = 0
        self._config_path: str | None = None
        self._worker_provider_type: str = "litellm"  # "litellm" or "scripted"
        self._running_batch_tasks: dict[str, asyncio.Task[None]] = {}
        self._batch_status: dict[str, dict[str, Any]] = {}
        self._reconciler_interval_seconds = max(0.2, float(scheduler_reconcile_interval_seconds))
        self._scheduler_max_concurrent_dispatches = max(1, int(scheduler_max_concurrent_dispatches))
        self._revalidation_chain_enabled = bool(revalidation_chain_enabled)
        edge_types = [str(item).strip() for item in (revalidation_chain_edge_types or ["requires_contract"]) if str(item).strip()]
        self._revalidation_chain_edge_types = set(edge_types or ["requires_contract"])
        self._revalidation_chain_max_depth = max(1, int(revalidation_chain_max_depth))
        self._decision_sla_seconds = max(0, int(decision_sla_seconds))
        scope = str(decision_sla_block_scope or "module").strip().lower()
        if scope not in {"module", "all", "none"}:
            scope = "module"
        self._decision_sla_block_scope = scope
        self._decision_queue_impact_weight = max(1, int(decision_queue_impact_weight))
        self._decision_queue_age_weight = max(1, int(decision_queue_age_weight))
        default_degradation = str(decision_default_degradation or "wait").strip().lower()
        if default_degradation not in {"wait", "stub", "continue_partial"}:
            default_degradation = "wait"
        self._decision_default_degradation = default_degradation
        self._decision_sla_blocked_modules: set[str] = set()
        self._decision_sla_global_block = False
        self._metrics_snapshot_interval_seconds = 60.0
        self._last_metrics_snapshot_at: str | None = None
        self._reconciler_task: asyncio.Task[None] | None = None
        self._running_work_item_dispatches: dict[str, asyncio.Task[None]] = {}
        self._project_runtime_attributes: dict[str, dict[str, Any]] = {}
        self._scope_exclude_names = {
            ".git",
            ".hg",
            ".svn",
            ".venv",
            "venv",
            "__pycache__",
            "node_modules",
            "dist",
            "build",
            ".mypy_cache",
            ".pytest_cache",
            ".ruff_cache",
        }
        self._configured_project_scopes = allowed_project_scopes or []
        self._project_registry = self._build_project_registry(
            configured_entries=project_registry,
            allowed_scopes=self._configured_project_scopes,
        )
        self._allowed_project_scopes = set(self._project_registry)

        self.core_loop = AgentLoop(
            bus=bus,
            provider=provider,
            workspace=self.workspace,
            model=model,
            max_iterations=max_iterations,
            brave_api_key=brave_api_key,
            exec_config=self.exec_config,
            cron_service=cron_service,
            restrict_to_workspace=restrict_to_workspace,
            session_manager=self.sessions,
            agent_role="core",
            scope_hint=f"Whole workspace rooted at {self.workspace}",
        )
        self.core_loop.tools.register(DelegateProjectTaskTool(self))
        self.core_loop.tools.register(DelegateProjectsBatchTool(self))
        self.core_loop.tools.register(GetBatchDelegationStatusTool(self))
        self.core_loop.tools.register(ListProjectScopesTool(self))
        self.core_loop.tools.register(DescribeProviderInterfacesTool(self))
        self.core_loop.tools.register(RegisterContractFunctionDependencyTool(self))
        self.core_loop.tools.register(RequestContractStubTool(self))
        self.core_loop.tools.register(ManageWorkflowStateTool(self, actor_role="core"))

    @staticmethod
    def _to_snake_case(value: str) -> str:
        value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
        value = re.sub(r"[^a-zA-Z0-9]+", "_", value)
        return value.strip("_").lower() or "contract"

    def _resolve_scope_file_path(self, scope_path: Path, relative_path: str) -> Path:
        target = (scope_path / relative_path).resolve()
        try:
            within_scope = target.is_relative_to(scope_path)
        except AttributeError:
            within_scope = str(target).startswith(str(scope_path))

        if not within_scope:
            raise ValueError("stub_relative_path must stay within the consumer project scope")
        return target

    def request_contract_stub(
        self,
        consumer_project: str,
        provider_project: str,
        interface_name: str,
        contract_spec: dict[str, Any] | None = None,
        stub_relative_path: str | None = None,
        stub_content: str | None = None,
        consumer_work_item_id: str | None = None,
        provider_work_item_id: str | None = None,
    ) -> str:
        """Create a contract request and caller-side stub without asking provider to fully implement."""
        interface_name = interface_name.strip()
        if not interface_name:
            raise ValueError("interface_name cannot be empty")

        consumer_normalized, consumer_scope_path = self._normalize_project(consumer_project)
        provider_normalized, _ = self._normalize_project(provider_project)

        spec = contract_spec or {}
        relative_path = (stub_relative_path or f"contracts/{self._to_snake_case(interface_name)}_stub.py").strip().replace("\\", "/")
        if not relative_path:
            raise ValueError("stub_relative_path cannot be empty")

        consumer_linked_work_item = (consumer_work_item_id or "").strip()
        if not consumer_linked_work_item:
            candidate = self._find_latest_work_item_for_module(consumer_normalized)
            if candidate:
                consumer_linked_work_item = candidate.id
        if not consumer_linked_work_item:
            created_work_item = self.create_work_item(
                {
                    "module": consumer_normalized,
                    "goal": f"Integrate cross-module contract stub for {interface_name}",
                    "status": "blocked",
                    "owner_agent": f"{consumer_normalized}-agent",
                    "decision_required": False,
                    "decision_type": "",
                    "metadata": {
                        "flow": "contract_request_stub_only",
                        "interface_name": interface_name,
                        "provider_module": provider_normalized,
                    },
                }
            )
            consumer_linked_work_item = created_work_item.id

        provider_linked_work_item = (provider_work_item_id or "").strip()
        if not provider_linked_work_item:
            candidate = self._find_latest_work_item_for_module(provider_normalized)
            if candidate:
                provider_linked_work_item = candidate.id
        if not provider_linked_work_item:
            created_provider_item = self.create_work_item(
                {
                    "module": provider_normalized,
                    "goal": f"Review and implement contract {interface_name} for {consumer_normalized}",
                    "status": "requested",
                    "owner_agent": f"{provider_normalized}-agent",
                    "decision_required": False,
                    "decision_type": "",
                    "metadata": {
                        "flow": "contract_request_stub_only",
                        "interface_name": interface_name,
                        "consumer_module": consumer_normalized,
                    },
                }
            )
            provider_linked_work_item = created_provider_item.id

        stub_path = self._resolve_scope_file_path(consumer_scope_path, relative_path)
        stub_path.parent.mkdir(parents=True, exist_ok=True)

        if not stub_content:
            spec_json = json.dumps(spec, ensure_ascii=False, indent=2)
            stub_content = (
                f"\"\"Auto-generated contract stub for '{interface_name}'.\"\"\"\n\n"
                "from __future__ import annotations\n\n"
                "from typing import Any\n\n"
                f"CONTRACT_PROVIDER = \"{provider_normalized}\"\n"
                f"CONTRACT_INTERFACE = \"{interface_name}\"\n"
                f"CONTRACT_SPEC = {spec_json}\n\n"
                f"class {interface_name}Stub:\n"
                "    \"\"\"Caller-side stub that unblocks consumer work before provider implementation.\"\"\"\n\n"
                "    def invoke(self, payload: dict[str, Any]) -> dict[str, Any]:\n"
                "        raise NotImplementedError(\n"
                "            \"Contract requested from provider module; implementation pending. \"\n"
                "            \"Replace this stub once provider contract is accepted and implemented.\"\n"
                "        )\n"
            )

        stub_path.write_text(stub_content, encoding="utf-8")
        contract = self.create_contract(
            {
                "provider_module": provider_normalized,
                "consumer_module": consumer_normalized,
                "interface_name": interface_name,
                "status": "requested",
                "spec": spec,
                "stub_path": f"{consumer_normalized}/{relative_path}" if not relative_path.startswith(consumer_normalized + "/") else relative_path,
                "work_item_id": provider_linked_work_item,
                "consumer_work_item_id": consumer_linked_work_item,
                "provider_work_item_id": provider_linked_work_item,
                "metadata": {
                    "flow": "contract_request_stub_only",
                    "stub_path": str(stub_path),
                },
            }
        )

        self._emit_workflow_event(
            "workflow.stub.generated",
            {
                "contract_id": contract.id,
                "consumer_project": consumer_normalized,
                "provider_project": provider_normalized,
                "interface_name": interface_name,
                "stub_path": str(stub_path),
            },
        )
        self._emit_workflow_event(
            "contract.requested_for_provider",
            {
                "contract_id": contract.id,
                "interface_name": interface_name,
                "consumer_project": consumer_normalized,
                "provider_project": provider_normalized,
                "consumer_work_item_id": consumer_linked_work_item,
                "provider_work_item_id": provider_linked_work_item,
                "stub_path": str(stub_path),
                "status": "requested",
            },
        )
        return (
            f"Contract request created: {contract.id} ({consumer_normalized} -> {provider_normalized}, interface={interface_name}). "
            f"Caller-side stub generated at {stub_path}. "
            f"Provider work item linked: {provider_linked_work_item}. No provider implementation was requested."
        )

    def _emit_workflow_event(self, event_type: str, payload: dict[str, Any]) -> None:
        """Emit workflow events to the bus so UI/schedulers can react in real time."""
        message = {
            "type": event_type,
            "payload": payload,
        }
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        loop.create_task(
            self.bus.publish_outbound(
                msg=OutboundMessage(
                    channel="workflow",
                    chat_id="orchestrator",
                    content=json.dumps(message, ensure_ascii=False),
                    metadata={"event_type": event_type, **payload},
                )
            )
        )

    @staticmethod
    def _unique_list(values: list[str]) -> list[str]:
        seen: set[str] = set()
        result: list[str] = []
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            result.append(value)
        return result

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _parse_iso_datetime(value: str | None) -> datetime | None:
        if not value:
            return None
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @classmethod
    def _seconds_between(cls, start_iso: str | None, end_iso: str | None = None) -> float | None:
        start = cls._parse_iso_datetime(start_iso)
        end = cls._parse_iso_datetime(end_iso) if end_iso else datetime.now(timezone.utc)
        if start is None or end is None:
            return None
        return max(0.0, (end - start).total_seconds())

    @staticmethod
    def _average(values: list[float]) -> float:
        if not values:
            return 0.0
        return sum(values) / len(values)

    @staticmethod
    def _normalize_degradation_mode(value: Any, default: str = "wait") -> str:
        mode = str(value or "").strip().lower() or default
        if mode not in {"wait", "stub", "continue_partial"}:
            return default
        return mode

    def _decision_age_seconds(self, decision: DecisionRecord, now_iso: str | None = None) -> float:
        age = self._seconds_between(decision.created_at, now_iso or self._now_iso())
        return float(age or 0.0)

    def _decision_impact_size(self, work_item: WorkItemRecord | None) -> int:
        if work_item is None:
            return 1
        downstream_edges = self.list_dependency_edges(
            filters={"target_work_item_id": work_item.id, "status": "active"},
            limit=500,
        )
        unresolved_contracts = self.list_contracts(
            filters={"work_item_id": work_item.id},
            limit=200,
        )
        unresolved_contract_count = len(
            [contract for contract in unresolved_contracts if contract.status not in {"accepted", "implemented", "completed"}]
        )
        # Always include the decision owner work item itself as affected surface.
        return 1 + len(downstream_edges) + unresolved_contract_count

    def get_decision_queue(self, limit: int = 200) -> list[dict[str, Any]]:
        now_iso = self._now_iso()
        pending = self.list_decisions(filters={"status": "pending"}, limit=limit)
        in_review = self.list_decisions(filters={"status": "in_review"}, limit=limit)
        candidates = pending + in_review

        entries: list[dict[str, Any]] = []
        for decision in candidates:
            work_item = self.get_work_item(decision.work_item_id)
            impact_size = self._decision_impact_size(work_item)
            age_seconds = self._decision_age_seconds(decision, now_iso)
            overdue = bool(self._decision_sla_seconds > 0 and age_seconds >= self._decision_sla_seconds)
            work_item_priority = int(work_item.priority) if work_item else 0
            priority_score = (
                impact_size * self._decision_queue_impact_weight
                + int(age_seconds // 60) * self._decision_queue_age_weight
                + work_item_priority
                + (10_000 if overdue else 0)
            )
            entries.append(
                {
                    "decision_id": decision.id,
                    "work_item_id": decision.work_item_id,
                    "module": work_item.module if work_item else "",
                    "decision_type": decision.decision_type,
                    "status": decision.status,
                    "age_seconds": round(age_seconds, 3),
                    "impact_size": impact_size,
                    "work_item_priority": work_item_priority,
                    "sla_seconds": self._decision_sla_seconds,
                    "sla_overdue": overdue,
                    "priority_score": int(priority_score),
                    "degradation_mode": self._resolve_work_item_degradation_strategy(work_item, [decision]),
                }
            )

        entries.sort(
            key=lambda item: (
                -int(item["priority_score"]),
                -int(item["impact_size"]),
                -float(item["age_seconds"]),
                str(item["decision_id"]),
            )
        )
        for index, item in enumerate(entries, start=1):
            item["queue_rank"] = index
        return entries[:limit]

    def _resolve_work_item_degradation_strategy(
        self,
        work_item: WorkItemRecord | None,
        pending_decisions: list[DecisionRecord],
    ) -> str:
        mode = self._decision_default_degradation
        if work_item is not None:
            scheduler_meta = dict(work_item.metadata.get("scheduler", {}))
            configured = scheduler_meta.get("decision_degradation")
            if configured is not None:
                mode = self._normalize_degradation_mode(configured, default=mode)

        for decision in pending_decisions:
            meta = dict(decision.metadata or {})
            configured = meta.get("degradation_mode") or meta.get("degradation_policy")
            if configured is not None:
                mode = self._normalize_degradation_mode(configured, default=mode)

        return mode

    def _refresh_decision_controls(self, limit: int = 500) -> dict[str, Any]:
        queue = self.get_decision_queue(limit=limit)
        overdue_entries = [item for item in queue if bool(item.get("sla_overdue"))]
        blocked_modules = {str(item.get("module") or "").strip() for item in overdue_entries if str(item.get("module") or "").strip()}

        self._decision_sla_blocked_modules = blocked_modules if self._decision_sla_block_scope == "module" else set()
        self._decision_sla_global_block = bool(overdue_entries) and self._decision_sla_block_scope == "all"

        summary = {
            "decision_sla_seconds": self._decision_sla_seconds,
            "decision_sla_block_scope": self._decision_sla_block_scope,
            "pending_count": len(queue),
            "overdue_count": len(overdue_entries),
            "blocked_modules": sorted(self._decision_sla_blocked_modules),
            "global_blocked": self._decision_sla_global_block,
            "queue": queue,
        }
        self._emit_workflow_event("workflow.decision.queue.updated", summary)
        return summary

    @staticmethod
    def _is_contract_terminal(status: str) -> bool:
        return status in {"accepted", "implemented", "completed", "rejected", "invalidated", "deprecated", "superseded"}

    @staticmethod
    def _is_decision_terminal(status: str) -> bool:
        return status in {"approved", "rejected", "completed"}

    @staticmethod
    def _contract_function_key(function: dict[str, Any]) -> str:
        name = str(function.get("name") or "").strip()
        if name:
            return name
        sig = str(function.get("sig") or "").strip()
        return sig

    def _normalize_contract_functions(
        self,
        raw_functions: list[dict[str, Any]] | None,
        *,
        consumer_module: str,
        implementer_work_item_id: str,
        existing_functions: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        existing_functions = [dict(item) for item in (existing_functions or []) if isinstance(item, dict)]
        existing_by_key = {
            self._contract_function_key(item): dict(item)
            for item in existing_functions
            if self._contract_function_key(item)
        }
        order = [self._contract_function_key(item) for item in existing_functions if self._contract_function_key(item)]

        normalized: dict[str, dict[str, Any]] = {}
        for key, item in existing_by_key.items():
            normalized[key] = dict(item)

        for item in raw_functions or []:
            if not isinstance(item, dict):
                continue
            key = self._contract_function_key(item)
            if not key:
                continue
            previous = normalized.get(key, {})
            consumer_modules = self._unique_list(
                [str(module_id).strip() for module_id in previous.get("consumer_modules", []) if str(module_id).strip()]
                + [str(module_id).strip() for module_id in item.get("consumer_modules", []) if str(module_id).strip()]
                + ([consumer_module] if consumer_module else [])
            )
            normalized[key] = {
                "name": str(item.get("name") or previous.get("name") or "").strip() or key,
                "sig": str(item.get("sig") or previous.get("sig") or "").strip(),
                "desc": str(item.get("desc") or previous.get("desc") or "").strip(),
                "impl_status": str(item.get("impl_status") or previous.get("impl_status") or "").strip(),
                "impl_latest_work_item_id": str(
                    item.get("impl_latest_work_item_id")
                    or implementer_work_item_id
                    or previous.get("impl_latest_work_item_id")
                    or ""
                ).strip(),
                "consumer_modules": consumer_modules,
            }
            if key not in order:
                order.append(key)

        result: list[dict[str, Any]] = []
        for key in order:
            item = normalized.get(key)
            if item is None:
                continue
            if not item.get("consumer_modules") and consumer_module:
                item = {**item, "consumer_modules": [consumer_module]}
            result.append(item)
        return result

    def _incoming_contract_functions(
        self,
        fields: dict[str, Any],
        *,
        consumer_module: str,
        implementer_work_item_id: str,
        existing_functions: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        if "functions" in fields:
            raw_functions = fields.get("functions") or []
        else:
            spec = dict(fields.get("spec") or {})
            raw_functions = spec.get("functions") if isinstance(spec.get("functions"), list) else None
        return self._normalize_contract_functions(
            raw_functions,
            consumer_module=consumer_module,
            implementer_work_item_id=implementer_work_item_id,
            existing_functions=existing_functions,
        )

    def _effective_contract_functions(self, contract: ContractRecord) -> list[dict[str, Any]]:
        spec = dict(contract.spec or {})
        spec_functions = spec.get("functions") if isinstance(spec.get("functions"), list) else None
        return self._normalize_contract_functions(
            contract.functions or spec_functions,
            consumer_module=contract.consumer_module,
            implementer_work_item_id=contract.work_item_id,
        )

    @staticmethod
    def _is_contract_function_resolved(status: str) -> bool:
        return str(status or "").strip() in {"accepted", "implemented", "completed"}

    def _find_contract_function_entry(self, contract: ContractRecord, function_name: str) -> tuple[int, dict[str, Any], list[dict[str, Any]]]:
        normalized_name = str(function_name or "").strip()
        if not normalized_name:
            raise ValueError("function_name cannot be empty")

        functions = self._effective_contract_functions(contract)
        for index, item in enumerate(functions):
            if self._contract_function_key(item) == normalized_name:
                return index, dict(item), functions
        raise ValueError(f"Function '{normalized_name}' not found in contract {contract.id}")

    def register_contract_function_dependency(
        self,
        *,
        contract_id: str,
        function_name: str,
        dependent_work_item_id: str,
    ) -> ContractRecord:
        contract = self.get_contract(contract_id)
        if contract is None:
            raise ValueError(f"Unknown contract id: {contract_id}")

        dependent_work_item = self.get_work_item(dependent_work_item_id)
        if dependent_work_item is None:
            raise ValueError(f"Unknown work item id: {dependent_work_item_id}")

        function_index, function_entry, functions = self._find_contract_function_entry(contract, function_name)
        consumer_modules = self._unique_list(
            [str(module_id).strip() for module_id in function_entry.get("consumer_modules", []) if str(module_id).strip()]
            + [dependent_work_item.module]
        )
        impl_latest_work_item_id = str(
            function_entry.get("impl_latest_work_item_id") or contract.work_item_id or ""
        ).strip()
        updated_function = {
            **function_entry,
            "impl_latest_work_item_id": impl_latest_work_item_id,
            "consumer_modules": consumer_modules,
        }
        functions[function_index] = updated_function

        updated_contract = self.update_contract(
            contract.id,
            {
                "functions": functions,
                "auto_create_dependency_edge": False,
            },
        )

        if self._is_contract_function_resolved(updated_function.get("impl_status", "")):
            return updated_contract
        if not impl_latest_work_item_id:
            raise ValueError(
                f"Contract function '{function_name}' has no impl_latest_work_item_id to depend on"
            )
        if impl_latest_work_item_id == dependent_work_item.id:
            return updated_contract

        existing_edges = self.list_dependency_edges(
            filters={
                "source_work_item_id": dependent_work_item.id,
                "target_work_item_id": impl_latest_work_item_id,
                "edge_type": "requires_contract_function",
            },
            limit=1,
        )
        if existing_edges:
            current = existing_edges[0]
            existing_metadata = dict(current.metadata or {})
            existing_contract_ids = [str(item).strip() for item in existing_metadata.get("contract_ids", []) if str(item).strip()]
            primary_contract_id = str(existing_metadata.get("contract_id") or "").strip()
            contract_ids = self._unique_list(existing_contract_ids + ([primary_contract_id] if primary_contract_id else []) + [contract.id])
            function_names = self._unique_list(
                [str(item).strip() for item in existing_metadata.get("contract_function_names", []) if str(item).strip()]
                + [self._contract_function_key(updated_function)]
            )
            interface_names = self._unique_list(
                [str(item).strip() for item in existing_metadata.get("interface_names", []) if str(item).strip()]
                + [contract.interface_name]
            )
            next_metadata = {
                **existing_metadata,
                "contract_id": primary_contract_id or contract.id,
                "contract_ids": contract_ids,
                "contract_function_names": function_names,
                "interface_name": str(existing_metadata.get("interface_name") or contract.interface_name),
                "interface_names": interface_names,
            }
            self.update_dependency_edge(current.id, {"status": "active", "metadata": next_metadata})
        else:
            self.create_dependency_edge(
                {
                    "source_work_item_id": dependent_work_item.id,
                    "target_work_item_id": impl_latest_work_item_id,
                    "edge_type": "requires_contract_function",
                    "status": "active",
                    "metadata": {
                        "contract_id": contract.id,
                        "contract_ids": [contract.id],
                        "contract_function_names": [self._contract_function_key(updated_function)],
                        "interface_name": contract.interface_name,
                        "interface_names": [contract.interface_name],
                    },
                }
            )

        return self.get_contract(contract.id) or updated_contract

    def _mergeable_contract_candidate(self, fields: dict[str, Any]) -> ContractRecord | None:
        """Find an existing pending contract request that can absorb a duplicate request."""
        status = str(fields.get("status", "requested"))
        if status not in {"requested", "draft", "in_review"}:
            return None

        if not bool(fields.get("dedupe_request", True)):
            return None

        provider_module = str(fields.get("provider_module") or "")
        consumer_module = str(fields.get("consumer_module") or "")
        interface_name = str(fields.get("interface_name") or "")
        requested_version = int(fields.get("version", 1))

        if not provider_module or not consumer_module or not interface_name:
            return None

        candidates = self.list_contracts(
            filters={
                "provider_module": provider_module,
                "consumer_module": consumer_module,
                "interface_name": interface_name,
            },
            limit=200,
        )
        for candidate in candidates:
            if candidate.status not in {"requested", "draft", "in_review"}:
                continue
            if int(candidate.version) != requested_version:
                continue
            return candidate
        return None

    def _merge_contract_request(self, existing: ContractRecord, fields: dict[str, Any]) -> ContractRecord:
        """Merge duplicated contract requests into one canonical pending contract."""
        metadata = dict(existing.metadata)

        incoming_work_item_id = str(fields.get("work_item_id") or "").strip()
        incoming_implementer_work_item_id = str(fields.get("provider_work_item_id") or incoming_work_item_id).strip()
        incoming_consumer_work_item_id = str(fields.get("consumer_work_item_id") or "").strip()
        incoming_provider_work_item_id = str(fields.get("provider_work_item_id") or "").strip()

        merged_work_item_ids = self._unique_list(
            [str(item).strip() for item in metadata.get("merged_work_item_ids", []) if str(item).strip()]
            + ([existing.work_item_id] if str(existing.work_item_id).strip() else [])
            + ([incoming_implementer_work_item_id] if incoming_implementer_work_item_id else [])
        )
        merged_consumer_work_item_ids = self._unique_list(
            [str(item).strip() for item in metadata.get("merged_consumer_work_item_ids", []) if str(item).strip()]
            + ([incoming_consumer_work_item_id] if incoming_consumer_work_item_id else [])
        )
        merged_provider_work_item_ids = self._unique_list(
            [str(item).strip() for item in metadata.get("merged_provider_work_item_ids", []) if str(item).strip()]
            + ([existing.work_item_id] if str(existing.work_item_id).strip() else [])
            + ([incoming_provider_work_item_id] if incoming_provider_work_item_id else [])
        )

        merged_request_count = int(metadata.get("merged_request_count", 1)) + 1
        merged_at = datetime.now().isoformat()
        incoming_request_source = dict(fields.get("request_source") or {})
        if not incoming_request_source:
            for key in ("session_key", "channel", "chat_id", "requested_by"):
                value = str(fields.get(key) or "").strip()
                if value:
                    incoming_request_source[key] = value

        merge_audit = dict(metadata.get("merge_audit") or {})
        entries = [dict(item) for item in merge_audit.get("entries", []) if isinstance(item, dict)]
        next_seq = len(entries) + 1
        entry = {
            "seq": next_seq,
            "merged_at": merged_at,
            "canonical_contract_id": existing.id,
            "incoming_request": {
                "provider_module": str(fields.get("provider_module") or existing.provider_module),
                "consumer_module": str(fields.get("consumer_module") or existing.consumer_module),
                "interface_name": str(fields.get("interface_name") or existing.interface_name),
                "version": int(fields.get("version", existing.version)),
                "status": str(fields.get("status", "requested")),
                "work_item_id": incoming_implementer_work_item_id,
                "consumer_work_item_id": incoming_consumer_work_item_id,
                "provider_work_item_id": incoming_provider_work_item_id,
                "source": incoming_request_source,
            },
        }
        entries.append(entry)
        merge_audit.update(
            {
                "canonical_contract_id": existing.id,
                "first_seen_at": str(merge_audit.get("first_seen_at") or existing.created_at),
                "last_merged_at": merged_at,
                "total_merged_requests": merged_request_count,
                "entries": entries,
            }
        )

        metadata.update(
            {
                "merged_request_count": merged_request_count,
                "merged_work_item_ids": merged_work_item_ids,
                "merged_consumer_work_item_ids": merged_consumer_work_item_ids,
                "merged_provider_work_item_ids": merged_provider_work_item_ids,
                "dedupe_mode": "request_merge",
                "last_merged_at": merged_at,
                "merge_audit": merge_audit,
            }
        )

        changes: dict[str, Any] = {
            "metadata": metadata,
        }
        incoming_spec = dict(fields.get("spec", {}))
        if not existing.spec and incoming_spec:
            changes["spec"] = incoming_spec
        incoming_functions = self._incoming_contract_functions(
            fields,
            consumer_module=str(fields.get("consumer_module") or existing.consumer_module),
            implementer_work_item_id=incoming_implementer_work_item_id or existing.work_item_id,
            existing_functions=existing.functions,
        )
        if incoming_functions:
            changes["functions"] = incoming_functions
        incoming_stub_path = str(fields.get("stub_path") or "")
        if not existing.stub_path and incoming_stub_path:
            changes["stub_path"] = incoming_stub_path
        incoming_implementation_path = str(fields.get("implementation_path") or "")
        if not existing.implementation_path and incoming_implementation_path:
            changes["implementation_path"] = incoming_implementation_path
        if not str(existing.work_item_id).strip() and incoming_implementer_work_item_id:
            changes["work_item_id"] = incoming_implementer_work_item_id

        updated = self.workflow_store.update_contract(existing.id, **changes)
        self._emit_workflow_event("workflow.contract.updated", self._serialize_workflow_record(updated))
        self._apply_contract_side_effects(updated, fields)
        self._emit_workflow_event(
            "workflow.contract.request.deduped",
            {
                "contract_id": updated.id,
                "provider_module": updated.provider_module,
                "consumer_module": updated.consumer_module,
                "interface_name": updated.interface_name,
                "version": updated.version,
                "merged_request_count": merged_request_count,
                "merged_at": merged_at,
                "merge_entry_seq": next_seq,
                "incoming_work_item_id": incoming_work_item_id,
                "incoming_consumer_work_item_id": incoming_consumer_work_item_id,
                "incoming_provider_work_item_id": incoming_provider_work_item_id,
                "incoming_request_source": incoming_request_source,
            },
        )
        return updated

    def _find_latest_work_item_for_module(self, module: str) -> WorkItemRecord | None:
        items = self.list_work_items(filters={"module": module}, limit=50)
        for item in items:
            if item.status not in {"completed", "done", "cancelled"}:
                return item
        return items[0] if items else None

    @staticmethod
    def _is_work_item_terminal(status: str) -> bool:
        return status in {"completed", "done", "cancelled"}

    @staticmethod
    def _is_work_item_dependency_released(status: str) -> bool:
        return status == "ready" or status in {"completed", "done", "cancelled"}

    def _work_item_execution_has_ended(self, work_item: WorkItemRecord) -> bool:
        return work_item.status != "running" and work_item.id not in self._running_work_item_dispatches

    def _unresolved_impl_contract_ids(self, work_item: WorkItemRecord) -> list[str]:
        unresolved: list[str] = []
        for contract_id in work_item.impl_on_contracts:
            contract = self.get_contract(contract_id)
            if contract is None or not self._is_contract_terminal(contract.status):
                unresolved.append(contract_id)
        return unresolved

    def _all_impl_contracts_resolved(self, work_item: WorkItemRecord) -> bool:
        if not work_item.impl_on_contracts:
            return False
        for contract_id in work_item.impl_on_contracts:
            contract = self.get_contract(contract_id)
            if contract is None or not self._is_contract_resolved(contract.status):
                return False
        return True

    def _link_contract_to_work_item(self, work_item_id: str, contract_id: str) -> WorkItemRecord | None:
        if not work_item_id or not contract_id:
            return None
        work_item = self.get_work_item(work_item_id)
        if work_item is None:
            return None
        desired = self._unique_list([item for item in work_item.impl_on_contracts if item] + [contract_id])
        if desired == list(work_item.impl_on_contracts):
            return work_item
        return self.update_work_item(work_item.id, {"impl_on_contracts": desired})

    def _unlink_contract_from_work_item(self, work_item_id: str, contract_id: str) -> WorkItemRecord | None:
        if not work_item_id or not contract_id:
            return None
        work_item = self.get_work_item(work_item_id)
        if work_item is None:
            return None
        desired = [item for item in work_item.impl_on_contracts if item and item != contract_id]
        if desired == list(work_item.impl_on_contracts):
            return work_item
        return self.update_work_item(work_item.id, {"impl_on_contracts": desired})

    def _sync_contract_implementer_membership(
        self,
        contract: ContractRecord,
        *,
        previous_work_item_id: str = "",
    ) -> None:
        current_work_item_id = str(contract.work_item_id or "").strip()
        previous_work_item_id = str(previous_work_item_id or "").strip()
        if previous_work_item_id and previous_work_item_id != current_work_item_id:
            self._unlink_contract_from_work_item(previous_work_item_id, contract.id)
        if current_work_item_id:
            self._link_contract_to_work_item(current_work_item_id, contract.id)

    def _edge_should_be_active(self, edge: DependencyEdgeRecord) -> bool:
        target = self.get_work_item(edge.target_work_item_id)
        if target is None:
            return False
        return not self._is_work_item_dependency_released(target.status)

    def _sync_dependency_edges_for_target(self, work_item_id: str) -> None:
        edges = self.list_dependency_edges(filters={"target_work_item_id": work_item_id}, limit=2000)
        for edge in edges:
            desired_status = "active" if self._edge_should_be_active(edge) else "inactive"
            if desired_status == edge.status:
                continue
            self.update_dependency_edge(edge.id, {"status": desired_status})

    def _resolve_contract_consumer_work_item_id(self, contract: ContractRecord, fields: dict[str, Any]) -> str:
        consumer_work_item_id = str(fields.get("consumer_work_item_id") or "").strip()
        if consumer_work_item_id:
            return consumer_work_item_id

        merged = [
            str(item).strip()
            for item in dict(contract.metadata or {}).get("merged_consumer_work_item_ids", [])
            if str(item).strip()
        ]
        if merged:
            return merged[-1]

        candidate = self._find_latest_work_item_for_module(contract.consumer_module)
        return candidate.id if candidate else ""

    def _edge_blocks_work_item(self, edge: DependencyEdgeRecord) -> bool:
        return edge.status == "active"

    def _sync_work_item_dependency_blockers(self, work_item_id: str) -> WorkItemRecord | None:
        work_item = self.get_work_item(work_item_id)
        if work_item is None:
            return None

        edges = self.list_dependency_edges(filters={"source_work_item_id": work_item_id}, limit=500)
        edge_ids = {edge.id for edge in edges}
        blocking_edge_ids = [edge.id for edge in edges if self._edge_blocks_work_item(edge)]
        manual_blockers = [entry for entry in work_item.blocked_by if entry not in edge_ids]
        desired_blocked_by = self._unique_list(manual_blockers + blocking_edge_ids)

        if desired_blocked_by == list(work_item.blocked_by):
            return work_item

        return self.update_work_item(work_item.id, {"blocked_by": desired_blocked_by})

    def _sync_contract_implementation_work_item(self, contract: ContractRecord) -> WorkItemRecord | None:
        implementer_work_item_id = str(contract.work_item_id or "").strip()
        if not implementer_work_item_id:
            return None

        work_item = self.get_work_item(implementer_work_item_id)
        if work_item is None:
            return None

        desired_status = work_item.status
        unresolved_contract_ids = self._unresolved_impl_contract_ids(work_item)
        execution_ended = self._work_item_execution_has_ended(work_item)
        if self._is_contract_invalidation_status(contract.status) and not self._all_impl_contracts_resolved(work_item):
            if work_item.status not in {"in_progress", "blocked", "waiting_decision", "running"}:
                desired_status = "in_progress"
        elif unresolved_contract_ids:
            if execution_ended and work_item.status not in {"blocked", "waiting_decision", "running"}:
                desired_status = "blocked"
        elif execution_ended and self._all_impl_contracts_resolved(work_item):
            if not self._is_work_item_terminal(work_item.status):
                desired_status = "completed"
        elif execution_ended and work_item.status == "blocked":
            desired_status = "ready"

        if desired_status == work_item.status:
            return work_item

        return self.update_work_item(work_item.id, {"status": desired_status})

    def _mark_contract_edges_validated(self, contract: ContractRecord) -> None:
        for edge in self._linked_contract_edges(contract.id):
            metadata = dict(edge.metadata)
            next_metadata = {
                **metadata,
                "contract_id": contract.id,
                "revalidation_required": False,
                "validated_contract_version": contract.version,
            }
            if next_metadata != metadata:
                self.update_dependency_edge(edge.id, {"metadata": next_metadata})

    def _module_requires_contract_followup(self, contract: ContractRecord, edges: list[DependencyEdgeRecord]) -> bool:
        for edge in edges:
            metadata = dict(edge.metadata)
            required_raw = metadata.get("required_contract_version")
            required_version = int(required_raw) if str(required_raw or "").strip() else 0
            validated_raw = metadata.get("validated_contract_version")
            validated_version = int(validated_raw) if str(validated_raw or "").strip() else 0
            if required_version > 0 and contract.version < required_version:
                return True
            if validated_version > 0 and validated_version < contract.version:
                return True
        return False

    def _ensure_contract_module_followup(
        self,
        contract: ContractRecord,
        module: str,
        reason: str,
        dependent_work_item_ids: list[str],
    ) -> WorkItemRecord:
        open_items = self.list_work_items(filters={"module": module}, limit=200)
        for item in open_items:
            if item.status in {"completed", "done", "cancelled"}:
                continue
            metadata = dict(item.metadata or {})
            if metadata.get("flow") != "contract_module_revalidation":
                continue
            if str(metadata.get("contract_id") or "") != contract.id:
                continue
            return item

        return self.create_work_item(
            {
                "module": module,
                "goal": f"Revalidate module {module} for contract {contract.interface_name}",
                "status": "proposed",
                "owner_agent": f"{module}-agent",
                "metadata": {
                    "flow": "contract_module_revalidation",
                    "contract_id": contract.id,
                    "contract_version": contract.version,
                    "provider_module": contract.provider_module,
                    "consumer_module": contract.consumer_module,
                    "interface_name": contract.interface_name,
                    "reason": reason,
                    "dependent_work_item_ids": dependent_work_item_ids,
                },
            }
        )

    def _sync_contract_blocking(self, contract: ContractRecord) -> None:
        """Apply contract lifecycle to the implementing work item and linked dependency blockers."""
        self._sync_contract_implementer_membership(contract)
        updated_work_item = self._sync_contract_implementation_work_item(contract)
        if updated_work_item is not None:
            self._emit_workflow_event("workflow.work_item.updated", self._serialize_workflow_record(updated_work_item))

        for edge in self._linked_contract_edges(contract.id):
            self._sync_work_item_dependency_blockers(edge.source_work_item_id)

    @staticmethod
    def _is_contract_resolved(status: str) -> bool:
        return status in {"accepted", "implemented", "completed"}

    @staticmethod
    def _is_contract_invalidation_status(status: str) -> bool:
        return status in {"requested", "draft", "invalidated", "deprecated", "superseded", "rejected"}

    def _linked_contract_edges(self, contract_id: str, limit: int = 2000) -> list[DependencyEdgeRecord]:
        edges = self.list_dependency_edges(limit=limit)
        linked: list[DependencyEdgeRecord] = []
        for edge in edges:
            primary_contract_id = str(edge.metadata.get("contract_id") or "")
            related_contract_ids = {
                str(item).strip()
                for item in edge.metadata.get("contract_ids", [])
                if str(item).strip()
            }
            if primary_contract_id == contract_id or contract_id in related_contract_ids:
                linked.append(edge)
        return linked

    def _resolve_contract_path(self, value: str) -> Path:
        raw = (value or "").strip().replace("\\", "/")
        candidate = Path(raw)
        if candidate.is_absolute():
            return candidate
        return (self.workspace / raw).resolve()

    def _run_contract_revalidation_checks(self, contract: ContractRecord) -> dict[str, Any]:
        """Run minimal executable checks before clearing revalidation markers."""
        errors: list[str] = []
        checks: list[dict[str, Any]] = []

        resolved = self._is_contract_resolved(contract.status)
        checks.append({"name": "contract_resolved", "ok": resolved, "status": contract.status})
        if not resolved:
            errors.append(f"contract status is not resolved: {contract.status}")

        # todo: remove this check
        edges = self._linked_contract_edges(contract.id)
        for edge in edges:
            required_version_raw = edge.metadata.get("required_contract_version")
            required_version = int(required_version_raw) if str(required_version_raw or "").strip() else 0
            ok = required_version <= 0 or contract.version >= required_version
            checks.append(
                {
                    "name": "edge_required_version",
                    "edge_id": edge.id,
                    "ok": ok,
                    "required_version": required_version,
                    "contract_version": contract.version,
                }
            )
            if not ok:
                errors.append(
                    f"edge {edge.id} requires contract version {required_version}, got {contract.version}"
                )

        if contract.stub_path:
            stub_path = self._resolve_contract_path(contract.stub_path)
            exists = stub_path.exists()
            checks.append({"name": "stub_path_exists", "ok": exists, "path": str(stub_path)})
            if not exists:
                errors.append(f"stub path does not exist: {stub_path}")

        if contract.implementation_path:
            impl_path = self._resolve_contract_path(contract.implementation_path)
            exists = impl_path.exists()
            checks.append({"name": "implementation_path_exists", "ok": exists, "path": str(impl_path)})
            if not exists:
                errors.append(f"implementation path does not exist: {impl_path}")

        return {
            "passed": not errors,
            "errors": errors,
            "checks": checks,
            "edge_count": len(edges),
        }

    def _propagate_contract_invalidation(self, contract: ContractRecord, reason: str) -> None:
        """Mark linked edges for revalidation and create module follow-up work items when compatibility changed."""
        edges = self._linked_contract_edges(contract.id)
        affected_modules: dict[str, list[DependencyEdgeRecord]] = {}

        for edge in edges:
            next_metadata = {
                **edge.metadata,
                "contract_id": contract.id,
                "revalidation_required": True,
                "revalidation_reason": reason,
                "required_contract_version": contract.version,
            }
            updated_edge = self.update_dependency_edge(edge.id, {"metadata": next_metadata, "status": "active"})
            source_item = self.get_work_item(updated_edge.source_work_item_id)
            if source_item is None:
                continue
            affected_modules.setdefault(source_item.module, []).append(updated_edge)
            self._sync_work_item_dependency_blockers(source_item.id)

        implementation_item = self._sync_contract_implementation_work_item(contract)
        if implementation_item is not None:
            self._emit_workflow_event("workflow.work_item.updated", self._serialize_workflow_record(implementation_item))

        followups: list[dict[str, Any]] = []
        for module, module_edges in affected_modules.items():
            if not self._module_requires_contract_followup(contract, module_edges):
                continue
            followup = self._ensure_contract_module_followup(
                contract,
                module,
                reason,
                [edge.source_work_item_id for edge in module_edges],
            )
            followups.append({"module": module, "work_item_id": followup.id})

        self._emit_workflow_event(
            "workflow.contract.version.invalidated",
            {
                "contract_id": contract.id,
                "version": contract.version,
                "reason": reason,
                "affected_edge_count": len(edges),
                "affected_modules": sorted(affected_modules),
                "followups": followups,
            },
        )

    def _clear_contract_revalidation(self, contract: ContractRecord) -> None:
        """Mark linked edges validated again after executable checks pass."""
        report = self._run_contract_revalidation_checks(contract)
        if not report["passed"]:
            self._emit_workflow_event(
                "workflow.contract.revalidation.failed",
                {
                    "contract_id": contract.id,
                    "version": contract.version,
                    "status": contract.status,
                    "errors": report["errors"],
                    "checks": report["checks"],
                    "edge_count": report["edge_count"],
                },
            )
            return

        self._mark_contract_edges_validated(contract)
        implementation_item = self._sync_contract_implementation_work_item(contract)
        if implementation_item is not None:
            self._emit_workflow_event("workflow.work_item.updated", self._serialize_workflow_record(implementation_item))

        for edge in self._linked_contract_edges(contract.id):
            self._sync_work_item_dependency_blockers(edge.source_work_item_id)

        self._emit_workflow_event(
            "workflow.contract.revalidation.passed",
            {
                "contract_id": contract.id,
                "version": contract.version,
                "status": contract.status,
                "checks": report["checks"],
                "edge_count": report["edge_count"],
            },
        )

    def _apply_contract_version_invalidation(
        self,
        previous: ContractRecord,
        updated: ContractRecord,
        changes: dict[str, Any],
    ) -> None:
        """Propagate version invalidation and request downstream revalidation."""
        explicit_invalidate = bool(changes.get("invalidate_dependents", False))
        version_changed = updated.version != previous.version
        regressed_from_resolved = self._is_contract_resolved(previous.status) and self._is_contract_invalidation_status(updated.status)

        if explicit_invalidate or version_changed or regressed_from_resolved:
            if explicit_invalidate:
                reason = "manual_invalidation"
            elif version_changed:
                reason = f"version_changed:{previous.version}->{updated.version}"
            else:
                reason = f"status_regressed:{previous.status}->{updated.status}"
            self._propagate_contract_invalidation(updated, reason)
            return

        if self._is_contract_resolved(updated.status):
            self._clear_contract_revalidation(updated)

    def _apply_contract_side_effects(self, contract: ContractRecord, fields: dict[str, Any]) -> None:
        """Auto-link contract lifecycle with dependency edges and implementing work items."""
        auto_edge = bool(fields.get("auto_create_dependency_edge", True))
        consumer_work_item_id = self._resolve_contract_consumer_work_item_id(contract, fields)

        if auto_edge:
            provider_work_item_id = str(fields.get("provider_work_item_id") or contract.work_item_id or "")

            if not provider_work_item_id:
                candidate = self._find_latest_work_item_for_module(contract.provider_module)
                provider_work_item_id = candidate.id if candidate else ""

            if consumer_work_item_id and provider_work_item_id:
                existing_edges = self.list_dependency_edges(
                    filters={
                        "source_work_item_id": consumer_work_item_id,
                        "target_work_item_id": provider_work_item_id,
                        "edge_type": "requires_contract",
                    },
                    limit=1,
                )
                if existing_edges:
                    edge = self.update_dependency_edge(
                        existing_edges[0].id,
                        {
                            "status": "active",
                            "metadata": {**existing_edges[0].metadata, "contract_id": contract.id},
                        },
                    )
                else:
                    edge = self.create_dependency_edge(
                        {
                            "source_work_item_id": consumer_work_item_id,
                            "target_work_item_id": provider_work_item_id,
                            "edge_type": "requires_contract",
                            "status": "active",
                            "metadata": {"contract_id": contract.id},
                        }
                    )
                self._emit_workflow_event("workflow.dependency_edge.upserted", self._serialize_workflow_record(edge))

                if self._is_contract_resolved(contract.status):
                    self._mark_contract_edges_validated(contract)

        self._sync_contract_blocking(contract)

    def _apply_decision_side_effects(self, decision: DecisionRecord) -> None:
        """Update work item lifecycle automatically when decisions complete."""
        if decision.status not in {"approved", "rejected", "completed"}:
            return

        work_item = self.get_work_item(decision.work_item_id)
        if not work_item:
            return

        updates: dict[str, Any] = {
            "decision_required": False,
        }
        if work_item.decision_type == decision.decision_type:
            updates["decision_type"] = ""

        if decision.status in {"approved", "completed"}:
            if work_item.status in {"waiting_decision", "proposed", "blocked"} and not work_item.blocked_by:
                updates["status"] = "ready"
        elif decision.status == "rejected":
            updates["status"] = "blocked"

        updated = self.update_work_item(work_item.id, updates)
        self._emit_workflow_event(
            "workflow.decision.applied",
            {
                "decision": self._serialize_workflow_record(decision),
                "work_item": self._serialize_workflow_record(updated),
            },
        )

    def run_workflow_scheduler_tick(self, limit: int = 200) -> dict[str, Any]:
        """Reconcile workflow state and return transitions for UI/scheduler loops."""
        decision_control = self._refresh_decision_controls(limit=max(200, limit * 3))
        work_items = self.list_work_items(limit=limit)
        transitions: list[dict[str, Any]] = []

        for work_item in work_items:
            blockers: list[str] = []
            decisions = self.list_decisions(filters={"work_item_id": work_item.id}, limit=100)
            pending_decisions = [decision for decision in decisions if decision.status in {"pending", "in_review"}]
            if pending_decisions:
                blockers.append("pending_decision")

            pending_ages = [self._decision_age_seconds(decision) for decision in pending_decisions]
            max_pending_age = max(pending_ages) if pending_ages else 0.0
            sla_overdue = bool(
                pending_decisions
                and self._decision_sla_seconds > 0
                and max_pending_age >= self._decision_sla_seconds
            )
            if sla_overdue:
                blockers.append("pending_decision_sla")

            edges = self.list_dependency_edges(filters={"source_work_item_id": work_item.id}, limit=100)
            blocking_edge_ids = [edge.id for edge in edges if self._edge_blocks_work_item(edge)]
            blockers.extend([f"dependency:{edge_id}" for edge_id in blocking_edge_ids])

            unresolved_impl_contract_ids = self._unresolved_impl_contract_ids(work_item)
            blockers.extend([f"impl_contract:{contract_id}" for contract_id in unresolved_impl_contract_ids])
            if work_item.impl_on_contracts and not self._work_item_execution_has_ended(work_item):
                blockers.append("execution:running")

            edge_ids = {edge.id for edge in edges}
            manual_blockers = [entry for entry in work_item.blocked_by if entry not in edge_ids]
            desired_blocked_by = self._unique_list(manual_blockers + blocking_edge_ids)
            blockers.extend([f"manual:{entry}" for entry in manual_blockers])

            desired_status = work_item.status
            desired_decision_required = bool(pending_decisions)
            degradation_mode = self._resolve_work_item_degradation_strategy(work_item, pending_decisions)
            non_decision_blockers = [item for item in blockers if item not in {"pending_decision", "pending_decision_sla"}]
            if desired_decision_required:
                if degradation_mode == "continue_partial" and not non_decision_blockers:
                    desired_status = "ready"
                elif degradation_mode == "stub":
                    desired_status = "blocked"
                else:
                    desired_status = "waiting_decision"
            elif blockers:
                desired_status = "blocked"
            elif work_item.status in {"proposed", "blocked", "waiting_decision"}:
                desired_status = "ready"

            if (
                desired_status != work_item.status
                or desired_decision_required != work_item.decision_required
                or desired_blocked_by != work_item.blocked_by
            ):
                scheduler_updates: dict[str, Any] = {"state": "reconciled"}
                if desired_status == "ready":
                    scheduler_updates["ready_since"] = work_item.metadata.get("scheduler", {}).get("ready_since", self._now_iso())
                scheduler_updates["decision_degradation_mode"] = degradation_mode
                scheduler_updates["decision_pending_max_age_seconds"] = round(max_pending_age, 3)
                scheduler_updates["decision_sla_overdue"] = sla_overdue
                updated = self.update_work_item(
                    work_item.id,
                    {
                        "status": desired_status,
                        "decision_required": desired_decision_required,
                        "blocked_by": desired_blocked_by,
                        "metadata": self._next_work_item_metadata(work_item, **scheduler_updates),
                    },
                )
                transition = {
                    "work_item_id": work_item.id,
                    "from_status": work_item.status,
                    "to_status": updated.status,
                    "blockers": blockers,
                }
                transitions.append(transition)
                self._emit_workflow_event("workflow.scheduler.transition", transition)

        summary = {
            "checked": len(work_items),
            "transitions": transitions,
            "decision_control": {
                "pending_count": decision_control["pending_count"],
                "overdue_count": decision_control["overdue_count"],
                "decision_sla_seconds": decision_control["decision_sla_seconds"],
                "decision_sla_block_scope": decision_control["decision_sla_block_scope"],
                "blocked_modules": decision_control["blocked_modules"],
                "global_blocked": decision_control["global_blocked"],
            },
        }
        self._emit_workflow_event("workflow.scheduler.tick", summary)
        return summary

    def _next_work_item_metadata(self, work_item: WorkItemRecord, **updates: Any) -> dict[str, Any]:
        metadata = dict(work_item.metadata)
        scheduler_meta = dict(metadata.get("scheduler", {}))
        scheduler_meta.update(updates)
        metadata["scheduler"] = scheduler_meta
        return metadata

    def get_observability_metrics(self, limit: int = 500) -> dict[str, Any]:
        work_items = self.list_work_items(limit=limit)
        contracts = self.list_contracts(limit=limit)
        decisions = self.list_decisions(limit=limit)
        decision_queue = self.get_decision_queue(limit=limit)

        now_iso = self._now_iso()
        agent_modules = sorted({item.module for item in work_items})
        agents: list[dict[str, Any]] = []

        for module in agent_modules:
            module_items = [item for item in work_items if item.module == module]
            ready_items = [item for item in module_items if item.status == "ready"]
            running_items = [item for item in module_items if item.status == "running"]
            scheduler_entries = [dict(item.metadata.get("scheduler", {})) for item in module_items]
            attempts = sum(int(entry.get("dispatch_attempt_count", 0)) for entry in scheduler_entries)
            failures = sum(int(entry.get("dispatch_failure_count", 0)) for entry in scheduler_entries)
            successes = sum(int(entry.get("dispatch_success_count", 0)) for entry in scheduler_entries)
            queue_waits = [float(entry.get("last_queue_wait_seconds", 0.0)) for entry in scheduler_entries if entry.get("last_queue_wait_seconds") is not None]
            durations = [float(entry.get("last_dispatch_duration_seconds", 0.0)) for entry in scheduler_entries if entry.get("last_dispatch_duration_seconds") is not None]

            agents.append(
                {
                    "module": module,
                    "queue_length": len(ready_items),
                    "running_count": len(running_items),
                    "dispatch_attempts": attempts,
                    "dispatch_successes": successes,
                    "dispatch_failures": failures,
                    "failure_rate": round(failures / attempts, 4) if attempts else 0.0,
                    "dispatch_latency_seconds_avg": round(self._average(queue_waits), 3),
                    "dispatch_duration_seconds_avg": round(self._average(durations), 3),
                }
            )

        contract_durations: list[float] = []
        contract_items: list[dict[str, Any]] = []
        for contract in contracts:
            lifecycle = dict(contract.metadata.get("lifecycle", {}))
            resolved_at = str(lifecycle.get("resolved_at") or "").strip() or None
            duration = self._seconds_between(contract.created_at, resolved_at or now_iso)
            if duration is None:
                continue
            contract_durations.append(duration)
            contract_items.append(
                {
                    "contract_id": contract.id,
                    "provider_module": contract.provider_module,
                    "consumer_module": contract.consumer_module,
                    "interface_name": contract.interface_name,
                    "version": contract.version,
                    "status": contract.status,
                    "lifecycle_seconds": round(duration, 3),
                    "resolved": bool(resolved_at),
                }
            )

        decision_durations: list[float] = []
        decision_items: list[dict[str, Any]] = []
        for decision in decisions:
            lifecycle = dict(decision.metadata.get("lifecycle", {}))
            resolved_at = str(lifecycle.get("resolved_at") or "").strip() or None
            duration = self._seconds_between(decision.created_at, resolved_at or now_iso)
            if duration is None:
                continue
            decision_durations.append(duration)
            decision_items.append(
                {
                    "decision_id": decision.id,
                    "work_item_id": decision.work_item_id,
                    "decision_type": decision.decision_type,
                    "status": decision.status,
                    "turnaround_seconds": round(duration, 3),
                    "resolved": bool(resolved_at),
                }
            )

        return {
            "generated_at": now_iso,
            "agents": sorted(agents, key=lambda item: (item["queue_length"], item["failure_rate"], item["module"]), reverse=True),
            "contracts": {
                "count": len(contract_items),
                "average_lifecycle_seconds": round(self._average(contract_durations), 3),
                "max_lifecycle_seconds": round(max(contract_durations), 3) if contract_durations else 0.0,
                "items": contract_items,
            },
            "decisions": {
                "count": len(decision_items),
                "pending_count": len(decision_queue),
                "overdue_count": len([item for item in decision_queue if bool(item.get("sla_overdue"))]),
                "decision_sla_seconds": self._decision_sla_seconds,
                "average_turnaround_seconds": round(self._average(decision_durations), 3),
                "max_turnaround_seconds": round(max(decision_durations), 3) if decision_durations else 0.0,
                "items": decision_items,
                "queue": decision_queue,
            },
        }

    def list_observability_snapshots(self, limit: int = 120, snapshot_type: str = "observability") -> list[dict[str, Any]]:
        snapshots = self.workflow_store.list_metrics_snapshots(snapshot_type=snapshot_type, limit=limit)
        return [
            {
                "id": snapshot.id,
                "snapshot_type": snapshot.snapshot_type,
                "generated_at": snapshot.generated_at,
                "created_at": snapshot.created_at,
                "metrics": snapshot.metrics,
            }
            for snapshot in snapshots
        ]

    def get_observability_timeseries(
        self,
        *,
        hours: int = 24,
        bucket_minutes: int = 5,
        snapshot_type: str = "observability",
        modules: list[str] | None = None,
    ) -> dict[str, Any]:
        safe_hours = max(1, min(hours, 24 * 14))
        safe_bucket_minutes = max(1, min(bucket_minutes, 60))
        normalized_modules = sorted({str(item).strip().lower() for item in (modules or []) if str(item).strip()})
        module_filter = set(normalized_modules)
        now_dt = datetime.now(timezone.utc)
        start_dt = now_dt - timedelta(hours=safe_hours)
        bucket_seconds = safe_bucket_minutes * 60
        # Snapshot interval is 1 minute by default. Load a bit extra to absorb restarts/gaps.
        snapshot_limit = max(120, min(5000, safe_hours * 90))

        snapshots = self.list_observability_snapshots(limit=snapshot_limit, snapshot_type=snapshot_type)
        buckets: dict[int, dict[str, Any]] = {}
        available_modules: set[str] = set()
        module_buckets: dict[str, dict[int, dict[str, Any]]] = {}

        for row in snapshots:
            metrics = dict(row.get("metrics") or {})
            generated_at = str(row.get("generated_at") or metrics.get("generated_at") or "")
            dt = self._parse_iso_datetime(generated_at)
            if dt is None:
                continue
            if dt < start_dt or dt > now_dt:
                continue

            ts = int(dt.timestamp())
            bucket_epoch = ts - (ts % bucket_seconds)

            all_agents = [dict(item) for item in metrics.get("agents", []) if isinstance(item, dict)]
            for agent in all_agents:
                module_name = str(agent.get("module", "")).strip().lower()
                if module_name:
                    available_modules.add(module_name)

            agents = all_agents
            if module_filter:
                agents = [item for item in all_agents if str(item.get("module", "")).strip().lower() in module_filter]

            decisions = dict(metrics.get("decisions") or {})
            contracts = dict(metrics.get("contracts") or {})

            attempts = sum(int(item.get("dispatch_attempts", 0)) for item in agents)
            failures = sum(int(item.get("dispatch_failures", 0)) for item in agents)
            queue_total = sum(int(item.get("queue_length", 0)) for item in agents)
            running_total = sum(int(item.get("running_count", 0)) for item in agents)
            failure_rate = (failures / attempts) if attempts else self._average([float(item.get("failure_rate", 0.0)) for item in agents])
            dispatch_latency_avg = self._average([float(item.get("dispatch_latency_seconds_avg", 0.0)) for item in agents])

            pending_count = int(decisions.get("pending_count", 0))
            overdue_count = int(decisions.get("overdue_count", 0))
            decision_turnaround = float(decisions.get("average_turnaround_seconds", 0.0))

            contract_lifecycle = float(contracts.get("average_lifecycle_seconds", 0.0))
            contract_count = int(contracts.get("count", 0))

            bucket = buckets.setdefault(
                bucket_epoch,
                {
                    "samples": 0,
                    "agents_queue_total": [],
                    "agents_running_total": [],
                    "agents_failure_rate": [],
                    "agents_dispatch_latency": [],
                    "decisions_pending": [],
                    "decisions_overdue": [],
                    "decisions_turnaround": [],
                    "contracts_lifecycle": [],
                    "contracts_count": [],
                },
            )
            bucket["samples"] += 1
            bucket["agents_queue_total"].append(float(queue_total))
            bucket["agents_running_total"].append(float(running_total))
            bucket["agents_failure_rate"].append(float(failure_rate))
            bucket["agents_dispatch_latency"].append(float(dispatch_latency_avg))
            bucket["decisions_pending"].append(float(pending_count))
            bucket["decisions_overdue"].append(float(overdue_count))
            bucket["decisions_turnaround"].append(float(decision_turnaround))
            bucket["contracts_lifecycle"].append(float(contract_lifecycle))
            bucket["contracts_count"].append(float(contract_count))

            module_groups: dict[str, list[dict[str, Any]]] = {}
            for agent in all_agents:
                module_name = str(agent.get("module", "")).strip().lower()
                if not module_name:
                    continue
                if module_filter and module_name not in module_filter:
                    continue
                module_groups.setdefault(module_name, []).append(agent)

            for module_name, grouped_agents in module_groups.items():
                per_module = module_buckets.setdefault(module_name, {})
                module_bucket = per_module.setdefault(
                    bucket_epoch,
                    {
                        "queue": [],
                        "running": [],
                        "failure_rate": [],
                        "dispatch_latency": [],
                    },
                )
                module_attempts = sum(int(item.get("dispatch_attempts", 0)) for item in grouped_agents)
                module_failures = sum(int(item.get("dispatch_failures", 0)) for item in grouped_agents)
                module_queue = sum(int(item.get("queue_length", 0)) for item in grouped_agents)
                module_running = sum(int(item.get("running_count", 0)) for item in grouped_agents)
                module_failure_rate = (
                    (module_failures / module_attempts)
                    if module_attempts
                    else self._average([float(item.get("failure_rate", 0.0)) for item in grouped_agents])
                )
                module_dispatch_latency = self._average([float(item.get("dispatch_latency_seconds_avg", 0.0)) for item in grouped_agents])

                module_bucket["queue"].append(float(module_queue))
                module_bucket["running"].append(float(module_running))
                module_bucket["failure_rate"].append(float(module_failure_rate))
                module_bucket["dispatch_latency"].append(float(module_dispatch_latency))

        ordered_epochs = sorted(buckets.keys())
        points: list[dict[str, Any]] = []
        series = {
            "agents_queue_length": [],
            "agents_failure_rate": [],
            "agents_dispatch_latency_seconds": [],
            "decisions_pending": [],
            "decisions_overdue": [],
            "contracts_average_lifecycle_seconds": [],
        }
        module_series: dict[str, dict[str, list[dict[str, Any]]]] = {
            module_name: {
                "agents_queue_length": [],
                "agents_failure_rate": [],
                "agents_dispatch_latency_seconds": [],
            }
            for module_name in sorted(module_buckets.keys())
        }

        for epoch in ordered_epochs:
            bucket = buckets[epoch]
            bucket_dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
            point = {
                "bucket_start": bucket_dt.isoformat(),
                "samples": int(bucket["samples"]),
                "agents": {
                    "queue_length_avg": round(self._average(bucket["agents_queue_total"]), 3),
                    "queue_length_max": round(max(bucket["agents_queue_total"]), 3) if bucket["agents_queue_total"] else 0.0,
                    "running_count_avg": round(self._average(bucket["agents_running_total"]), 3),
                    "failure_rate_avg": round(self._average(bucket["agents_failure_rate"]), 6),
                    "dispatch_latency_seconds_avg": round(self._average(bucket["agents_dispatch_latency"]), 3),
                },
                "decisions": {
                    "pending_count_avg": round(self._average(bucket["decisions_pending"]), 3),
                    "pending_count_max": round(max(bucket["decisions_pending"]), 3) if bucket["decisions_pending"] else 0.0,
                    "overdue_count_avg": round(self._average(bucket["decisions_overdue"]), 3),
                    "average_turnaround_seconds_avg": round(self._average(bucket["decisions_turnaround"]), 3),
                },
                "contracts": {
                    "average_lifecycle_seconds_avg": round(self._average(bucket["contracts_lifecycle"]), 3),
                    "count_avg": round(self._average(bucket["contracts_count"]), 3),
                },
            }
            points.append(point)

            series["agents_queue_length"].append({"t": point["bucket_start"], "v": point["agents"]["queue_length_avg"]})
            series["agents_failure_rate"].append({"t": point["bucket_start"], "v": point["agents"]["failure_rate_avg"]})
            series["agents_dispatch_latency_seconds"].append({"t": point["bucket_start"], "v": point["agents"]["dispatch_latency_seconds_avg"]})
            series["decisions_pending"].append({"t": point["bucket_start"], "v": point["decisions"]["pending_count_avg"]})
            series["decisions_overdue"].append({"t": point["bucket_start"], "v": point["decisions"]["overdue_count_avg"]})
            series["contracts_average_lifecycle_seconds"].append({"t": point["bucket_start"], "v": point["contracts"]["average_lifecycle_seconds_avg"]})

            for module_name, per_module in module_buckets.items():
                module_bucket = per_module.get(epoch)
                if module_bucket is None:
                    continue
                module_series[module_name]["agents_queue_length"].append(
                    {"t": point["bucket_start"], "v": round(self._average(module_bucket["queue"]), 3)}
                )
                module_series[module_name]["agents_failure_rate"].append(
                    {"t": point["bucket_start"], "v": round(self._average(module_bucket["failure_rate"]), 6)}
                )
                module_series[module_name]["agents_dispatch_latency_seconds"].append(
                    {"t": point["bucket_start"], "v": round(self._average(module_bucket["dispatch_latency"]), 3)}
                )

        return {
            "generated_at": self._now_iso(),
            "snapshot_type": snapshot_type,
            "modules": {
                "requested": normalized_modules,
                "available": sorted(available_modules),
                "included": sorted(module_buckets.keys()),
            },
            "window": {
                "hours": safe_hours,
                "bucket_minutes": safe_bucket_minutes,
                "start": start_dt.isoformat(),
                "end": now_dt.isoformat(),
            },
            "point_count": len(points),
            "points": points,
            "series": series,
            "module_series": module_series,
        }

    def _maybe_capture_metrics_snapshot(self, force: bool = False) -> dict[str, Any] | None:
        now_iso = self._now_iso()
        due = force or self._last_metrics_snapshot_at is None
        if not due and self._last_metrics_snapshot_at is not None:
            elapsed = self._seconds_between(self._last_metrics_snapshot_at, now_iso)
            due = bool(elapsed is not None and elapsed >= self._metrics_snapshot_interval_seconds)
        if not due:
            return None

        snapshot = self.get_observability_metrics(limit=500)
        self.workflow_store.create_metrics_snapshot(snapshot, snapshot_type="observability")
        self._last_metrics_snapshot_at = now_iso
        self._emit_workflow_event(
            "workflow.metrics.snapshot.captured",
            {
                "generated_at": snapshot["generated_at"],
                "snapshot_type": "observability",
            },
        )
        return snapshot

    async def _dispatch_claimed_work_item(self, work_item_id: str) -> None:
        """Run one ready work item through the matching module agent."""
        item = self.get_work_item(work_item_id)
        if item is None:
            return

        session_key = item.session_key or f"workflow::{item.id}"
        started_at = self._now_iso()
        queue_wait_seconds = self._seconds_between(item.metadata.get("scheduler", {}).get("ready_since"), started_at) or 0.0
        try:
            request_state = await self._submit_project_task(
                project=item.module,
                task=item.goal,
                session_key=session_key,
                channel="workflow",
                chat_id=f"work_item:{item.id}",
                publish_completion_message=False,
            )
            response = await asyncio.wait_for(request_state.future, timeout=1200.0)
            result = f"[Project Scope: {item.module}]\n{str(response.get('result') or '')}"
            latest = self.get_work_item(work_item_id)
            if latest is None:
                return

            artifacts = list(latest.artifacts)
            artifacts.append(
                {
                    "type": "delegation_result",
                    "work_item_id": latest.id,
                    "module": latest.module,
                    "content_preview": result[:2000],
                }
            )
            next_status = "in_progress" if latest.status == "running" else latest.status
            if latest.status == "running" and latest.impl_on_contracts:
                edges = self.list_dependency_edges(filters={"source_work_item_id": latest.id}, limit=200)
                edge_ids = {edge.id for edge in edges}
                manual_blockers = [entry for entry in latest.blocked_by if entry not in edge_ids]
                if self._unresolved_impl_contract_ids(latest) or manual_blockers or any(self._edge_blocks_work_item(edge) for edge in edges):
                    next_status = "blocked"
                else:
                    next_status = "ready"
            updated = self.update_work_item(
                latest.id,
                {
                    "status": next_status,
                    "session_key": session_key,
                    "artifacts": artifacts,
                    "metadata": self._next_work_item_metadata(
                        latest,
                        dispatch_started_at=started_at,
                        dispatch_completed_at=self._now_iso(),
                        last_queue_wait_seconds=queue_wait_seconds,
                        last_dispatch_duration_seconds=self._seconds_between(started_at, self._now_iso()) or 0.0,
                        last_dispatch_status="ok",
                        last_dispatch_error="",
                        last_session_key=session_key,
                        dispatch_attempt_count=int(latest.metadata.get("scheduler", {}).get("dispatch_attempt_count", 0)) + 1,
                        dispatch_success_count=int(latest.metadata.get("scheduler", {}).get("dispatch_success_count", 0)) + 1,
                    ),
                },
            )
            self._emit_workflow_event(
                "workflow.scheduler.dispatched",
                {
                    "work_item_id": updated.id,
                    "module": updated.module,
                    "status": updated.status,
                },
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            latest = self.get_work_item(work_item_id)
            if latest is None:
                return

            failed_at = self._now_iso()
            fallback_status = "blocked" if latest.status == "running" else latest.status
            updated = self.update_work_item(
                latest.id,
                {
                    "status": fallback_status,
                    "metadata": self._next_work_item_metadata(
                        latest,
                        dispatch_started_at=started_at,
                        dispatch_completed_at=failed_at,
                        last_queue_wait_seconds=queue_wait_seconds,
                        last_dispatch_duration_seconds=self._seconds_between(started_at, failed_at) or 0.0,
                        last_dispatch_status="error",
                        last_dispatch_error=str(exc),
                        last_session_key=session_key,
                        dispatch_attempt_count=int(latest.metadata.get("scheduler", {}).get("dispatch_attempt_count", 0)) + 1,
                        dispatch_failure_count=int(latest.metadata.get("scheduler", {}).get("dispatch_failure_count", 0)) + 1,
                    ),
                },
            )
            self._emit_workflow_event(
                "workflow.scheduler.dispatch_failed",
                {
                    "work_item_id": updated.id,
                    "module": updated.module,
                    "status": updated.status,
                    "error": str(exc),
                },
            )
        finally:
            self._running_work_item_dispatches.pop(work_item_id, None)

    def _claim_ready_work_item(self, work_item: WorkItemRecord) -> WorkItemRecord | None:
        """Claim a ready item for dispatch so only one scheduler loop handles it."""
        current = self.get_work_item(work_item.id)
        if current is None or current.status != "ready":
            return None

        session_key = current.session_key or f"workflow::{current.id}"
        owner = current.owner_agent or f"{current.module}-agent"
        return self.update_work_item(
            current.id,
            {
                "status": "running",
                "session_key": session_key,
                "owner_agent": owner,
                "metadata": self._next_work_item_metadata(
                    current,
                    state="dispatching",
                    ready_since=current.metadata.get("scheduler", {}).get("ready_since", self._now_iso()),
                    last_dispatch_status="running",
                    last_dispatch_error="",
                    last_session_key=session_key,
                    dispatch_started_at=self._now_iso(),
                ),
            },
        )

    async def _dispatch_ready_work_items(self, limit: int = 100) -> dict[str, Any]:
        """Claim and dispatch ready work items to module agents."""
        ready_items = self.list_work_items(filters={"status": "ready"}, limit=limit)
        plan = self._build_dispatch_plan(ready_items)
        started: list[str] = []
        skipped: list[dict[str, str]] = []
        selected: list[dict[str, Any]] = []
        available_slots = max(0, self._scheduler_max_concurrent_dispatches - len(self._running_work_item_dispatches))

        if self._decision_sla_global_block:
            skipped.extend(
                [{"work_item_id": item.id, "reason": "decision_sla_global_block"} for item in ready_items]
            )
        elif self._decision_sla_blocked_modules:
            for item in ready_items:
                if item.module in self._decision_sla_blocked_modules:
                    skipped.append({"work_item_id": item.id, "reason": "decision_sla_module_block"})

        for entry in plan:
            if available_slots <= 0:
                break
            item = self.get_work_item(entry["work_item_id"])
            if item is None:
                continue
            if item.id in self._running_work_item_dispatches:
                skipped.append({"work_item_id": item.id, "reason": "already_running"})
                continue
            profile = entry["profile"]
            current_inflight = self._module_inflight_count(item.module)
            cap = int(profile["concurrency_cap"])
            if current_inflight >= cap:
                skipped.append({"work_item_id": item.id, "reason": "module_concurrency_cap_reached"})
                continue

            claimed = self._claim_ready_work_item(item)
            if claimed is None:
                skipped.append({"work_item_id": item.id, "reason": "claim_failed"})
                continue

            selected.append(
                {
                    "work_item_id": claimed.id,
                    "module": claimed.module,
                    "score": entry["score"],
                    "priority": claimed.priority,
                    "priority_bias": entry["priority_bias"],
                    "concurrency_cap": cap,
                }
            )
            task = asyncio.create_task(self._dispatch_claimed_work_item(claimed.id))
            self._running_work_item_dispatches[claimed.id] = task
            started.append(claimed.id)
            available_slots -= 1

        if started or skipped:
            self._emit_workflow_event(
                "workflow.scheduler.dispatch_cycle",
                {
                    "selected": selected,
                    "started": started,
                    "skipped": skipped,
                    "max_concurrent_dispatches": self._scheduler_max_concurrent_dispatches,
                    "running_dispatches": len(self._running_work_item_dispatches),
                },
            )

        if plan:
            self._emit_workflow_event(
                "workflow.scheduler.dispatch_plan",
                {
                    "ready_count": len(ready_items),
                    "plan": [
                        {
                            "work_item_id": entry["work_item_id"],
                            "module": entry["module"],
                            "score": entry["score"],
                            "priority": entry["priority"],
                            "priority_bias": entry["priority_bias"],
                            "concurrency_cap": entry["concurrency_cap"],
                        }
                        for entry in plan
                    ],
                },
            )

        return {"started": started, "skipped": skipped}

    async def _run_reconciler_loop(self) -> None:
        """Continuously reconcile workflow state and dispatch ready work items."""
        try:
            while True:
                self.run_workflow_scheduler_tick(limit=300)
                await self._dispatch_ready_work_items(limit=150)
                self._maybe_capture_metrics_snapshot()
                await asyncio.sleep(self._reconciler_interval_seconds)
        except asyncio.CancelledError:
            raise

    async def _start_reconciler(self) -> None:
        if self._reconciler_task is not None and not self._reconciler_task.done():
            return
        self._reconciler_task = asyncio.create_task(self._run_reconciler_loop())

    async def _stop_reconciler(self) -> None:
        task = self._reconciler_task
        self._reconciler_task = None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        dispatch_tasks = list(self._running_work_item_dispatches.values())
        self._running_work_item_dispatches.clear()
        for dispatch_task in dispatch_tasks:
            dispatch_task.cancel()
        if dispatch_tasks:
            await asyncio.gather(*dispatch_tasks, return_exceptions=True)

    async def _shutdown_project_subprocesses(self) -> None:
        """Terminate project subprocesses and wait for their transports to close cleanly."""
        handles = list(self._project_subprocesses.values())
        self._project_subprocesses.clear()
        self.project_loops.clear()

        for handle in handles:
            process = handle.process
            if process.returncode is not None:
                continue
            try:
                if process.stdin is not None:
                    process.stdin.close()
            except Exception:
                pass
            try:
                process.terminate()
            except ProcessLookupError:
                pass
            except Exception:
                pass

        for handle in handles:
            process = handle.process
            if process.returncode is None:
                try:
                    await asyncio.wait_for(process.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass
                    except Exception:
                        pass
                    await asyncio.gather(process.wait(), return_exceptions=True)

        reader_tasks = [handle.reader_task for handle in handles if handle.reader_task is not None]
        if reader_tasks:
            await asyncio.gather(*reader_tasks, return_exceptions=True)

    def _serialize_workflow_record(self, record: Any) -> dict[str, Any]:
        return asdict(record)

    def create_work_item(self, fields: dict[str, Any]) -> WorkItemRecord:
        metadata = dict(fields.get("metadata", {}))
        scheduler_meta = dict(metadata.get("scheduler", {}))
        if str(fields.get("status", "proposed")) == "ready" and not scheduler_meta.get("ready_since"):
            scheduler_meta["ready_since"] = self._now_iso()
        if scheduler_meta:
            metadata["scheduler"] = scheduler_meta
        record = WorkItemRecord(
            id=str(fields.get("id") or uuid.uuid4())[:36],
            module=str(fields["module"]),
            goal=str(fields["goal"]),
            status=str(fields.get("status", "proposed")),
            priority=int(fields.get("priority", 0)),
            owner_agent=str(fields.get("owner_agent", "")),
            session_key=str(fields.get("session_key", "")),
            decision_required=bool(fields.get("decision_required", False)),
            decision_type=str(fields.get("decision_type", "")),
            impl_on_contracts=list(fields.get("impl_on_contracts", [])),
            blocked_by=list(fields.get("blocked_by", [])),
            artifacts=list(fields.get("artifacts", [])),
            metadata=metadata,
        )
        created = self.workflow_store.create_work_item(record)
        self._emit_workflow_event("workflow.work_item.created", self._serialize_workflow_record(created))
        self._sync_dependency_edges_for_target(created.id)
        return created

    def get_work_item(self, record_id: str) -> WorkItemRecord | None:
        return self.workflow_store.get_work_item(record_id)

    def list_work_items(self, filters: dict[str, Any] | None = None, limit: int = 100) -> list[WorkItemRecord]:
        filters = filters or {}
        return self.workflow_store.list_work_items(
            module=filters.get("module"),
            status=filters.get("status"),
            owner_agent=filters.get("owner_agent"),
            decision_required=filters.get("decision_required"),
            limit=limit,
        )

    def update_work_item(self, record_id: str, changes: dict[str, Any]) -> WorkItemRecord:
        updated = self.workflow_store.update_work_item(record_id, **changes)
        self._emit_workflow_event("workflow.work_item.updated", self._serialize_workflow_record(updated))
        self._sync_dependency_edges_for_target(updated.id)
        return updated

    def delete_work_item(self, record_id: str) -> bool:
        deleted = self.workflow_store.delete_work_item(record_id)
        self._emit_workflow_event("workflow.work_item.deleted", {"id": record_id, "deleted": deleted})
        return deleted

    def create_contract(self, fields: dict[str, Any]) -> ContractRecord:
        existing = self._mergeable_contract_candidate(fields)
        if existing is not None:
            # 同一语义接口被重复请求：比如 provider_module、consumer_module、interface_name、version 一样，且都处于 requested/draft 这类未定稿状态。
            # 并发扇出导致重复：module1 和 module2 几乎同时向 module3 请求同一接口，或者重试机制触发重复创建。
            # 人工决策窗口内多次提交：前端还没审阅完，后端又收到内容相近的新请求，这时更适合并入同一条 contract。
            # 目标是一个“共享接口”而不是多份分支协议：如果本质是同一 API 契约，就该合并；如果是不同版本/不同边界，就不该合并。
            #
            # 决策对象更少，评审更快。
            # 变更传播更稳定，不会因为多条重复 contract 出现不一致。
            # 审计更清晰，可以追踪“谁的请求被合并进来了（由audit实现）。
            return self._merge_contract_request(existing, fields)

        work_item_id = str(fields.get("work_item_id", "")).strip()
        provider_work_item_id = str(fields.get("provider_work_item_id", "")).strip()
        implementer_work_item_id = provider_work_item_id or work_item_id
        consumer_work_item_id = str(fields.get("consumer_work_item_id", "")).strip()
        incoming_metadata = dict(fields.get("metadata", {}))
        incoming_metadata.setdefault("merged_request_count", 1)
        incoming_metadata.setdefault("dedupe_mode", "request_merge")
        incoming_metadata.setdefault("merged_work_item_ids", [item for item in [implementer_work_item_id] if item])
        incoming_metadata.setdefault("merged_consumer_work_item_ids", [item for item in [consumer_work_item_id] if item])
        incoming_metadata.setdefault("merged_provider_work_item_ids", [item for item in [implementer_work_item_id] if item])

        record = ContractRecord(
            id=str(fields.get("id") or uuid.uuid4())[:36],
            provider_module=str(fields["provider_module"]),
            consumer_module=str(fields["consumer_module"]),
            interface_name=str(fields["interface_name"]),
            version=int(fields.get("version", 1)),
            status=str(fields.get("status", "requested")),
            functions=self._incoming_contract_functions(
                fields,
                consumer_module=str(fields["consumer_module"]),
                implementer_work_item_id=implementer_work_item_id,
            ),
            spec=dict(fields.get("spec", {})),
            stub_path=str(fields.get("stub_path", "")),
            implementation_path=str(fields.get("implementation_path", "")),
            work_item_id=implementer_work_item_id,
            metadata=incoming_metadata,
        )
        created = self.workflow_store.create_contract(record)
        self._sync_contract_implementer_membership(created)
        self._emit_workflow_event("workflow.contract.created", self._serialize_workflow_record(created))
        self._apply_contract_side_effects(created, fields)
        return created

    def get_contract(self, record_id: str) -> ContractRecord | None:
        return self.workflow_store.get_contract(record_id)

    def list_contracts(self, filters: dict[str, Any] | None = None, limit: int = 100) -> list[ContractRecord]:
        filters = filters or {}
        return self.workflow_store.list_contracts(
            provider_module=filters.get("provider_module"),
            consumer_module=filters.get("consumer_module"),
            interface_name=filters.get("interface_name"),
            status=filters.get("status"),
            work_item_id=filters.get("work_item_id"),
            limit=limit,
        )

    def describe_provider_interfaces(self, provider_module: str, *, limit: int = 500) -> dict[str, Any]:
        provider = str(provider_module or "").strip()
        if not provider:
            raise ValueError("provider_module cannot be empty")

        contracts = self.list_contracts(filters={"provider_module": provider}, limit=max(1, limit))
        grouped: dict[str, list[ContractRecord]] = {}
        for contract in contracts:
            grouped.setdefault(contract.interface_name, []).append(contract)

        interfaces: list[dict[str, Any]] = []
        for interface_name in sorted(grouped, key=str.lower):
            interface_contracts = sorted(
                grouped[interface_name],
                key=lambda record: (int(record.version), str(record.created_at), str(record.updated_at), record.id),
            )

            consumer_modules = self._unique_list([contract.consumer_module for contract in interface_contracts if contract.consumer_module])
            available_versions = sorted({int(contract.version) for contract in interface_contracts})
            statuses = self._unique_list([str(contract.status).strip() for contract in interface_contracts if str(contract.status).strip()])

            merged_functions: list[dict[str, Any]] = []
            for contract in interface_contracts:
                merged_functions = self._normalize_contract_functions(
                    self._effective_contract_functions(contract),
                    consumer_module=contract.consumer_module,
                    implementer_work_item_id=contract.work_item_id,
                    existing_functions=merged_functions,
                )

            interfaces.append(
                {
                    "interface_name": interface_name,
                    "latest_version": available_versions[-1] if available_versions else 0,
                    "available_versions": available_versions,
                    "statuses": statuses,
                    "consumer_modules": consumer_modules,
                    "contract_ids": [contract.id for contract in interface_contracts],
                    "function_count": len(merged_functions),
                    "functions": merged_functions,
                }
            )

        if interfaces:
            lines = [
                f"Provider module: {provider}",
                "Known reusable interfaces and functions:",
            ]
            for index, interface in enumerate(interfaces, start=1):
                consumers = ", ".join(interface["consumer_modules"]) or "none"
                statuses_text = ", ".join(interface["statuses"]) or "unknown"
                lines.append(
                    f"{index}. {interface['interface_name']}"
                    f" (latest_version={interface['latest_version']}; statuses={statuses_text}; consumers={consumers})"
                )
                for function in interface["functions"]:
                    function_consumers = ", ".join(function.get("consumer_modules", [])) or "none"
                    signature = function.get("sig") or function.get("name") or "<unknown>"
                    impl_status = function.get("impl_status") or "unknown"
                    desc = function.get("desc") or ""
                    suffix = f" - {desc}" if desc else ""
                    lines.append(
                        f"   - {function.get('name') or signature}: {signature} [{impl_status}] consumers={function_consumers}{suffix}"
                    )
            lines.append("Reuse an existing interface/function when semantics already match.")
            lines.append("If none fit, request a contract change or a new contract.")
            llm_prompt = "\n".join(lines)
        else:
            llm_prompt = (
                f"Provider module: {provider}\n"
                "Known reusable interfaces and functions: none.\n"
                "If you need cross-module functionality here, request a new contract."
            )

        return {
            "provider_module": provider,
            "interface_count": len(interfaces),
            "interfaces": interfaces,
            "llm_prompt": llm_prompt,
        }

    def get_contract_merge_audit(self, contract_id: str) -> dict[str, Any] | None:
        """Return one contract's merge audit view for UI/decision auditing."""
        contract = self.get_contract(contract_id)
        if contract is None:
            return None

        metadata = dict(contract.metadata or {})
        merge_audit = dict(metadata.get("merge_audit") or {})
        entries = [dict(item) for item in merge_audit.get("entries", []) if isinstance(item, dict)]

        merged_request_count = int(metadata.get("merged_request_count", 1))
        return {
            "contract_id": contract.id,
            "provider_module": contract.provider_module,
            "consumer_module": contract.consumer_module,
            "interface_name": contract.interface_name,
            "version": contract.version,
            "status": contract.status,
            "functions": [dict(item) for item in contract.functions],
            "merged_request_count": merged_request_count,
            "dedupe_mode": metadata.get("dedupe_mode", "none"),
            "first_seen_at": str(merge_audit.get("first_seen_at") or contract.created_at),
            "last_merged_at": str(merge_audit.get("last_merged_at") or metadata.get("last_merged_at") or ""),
            "merged_work_item_ids": [str(item) for item in metadata.get("merged_work_item_ids", [])],
            "merged_consumer_work_item_ids": [str(item) for item in metadata.get("merged_consumer_work_item_ids", [])],
            "merged_provider_work_item_ids": [str(item) for item in metadata.get("merged_provider_work_item_ids", [])],
            "entries": entries,
        }

    def list_contract_merge_audits(
        self,
        filters: dict[str, Any] | None = None,
        limit: int = 100,
        merged_only: bool = True,
    ) -> list[dict[str, Any]]:
        """List merge audit views over contracts, optionally merged-only."""
        contracts = self.list_contracts(filters=filters or {}, limit=limit)
        views: list[dict[str, Any]] = []
        for contract in contracts:
            view = self.get_contract_merge_audit(contract.id)
            if view is None:
                continue
            if merged_only and int(view.get("merged_request_count", 1)) <= 1:
                continue
            views.append(view)
        return views

    def update_contract(self, record_id: str, changes: dict[str, Any]) -> ContractRecord:
        previous = self.get_contract(record_id)
        if previous is None:
            raise ValueError(f"Unknown contract id: {record_id}")
        normalized_changes = dict(changes)
        if "functions" in normalized_changes or "spec" in normalized_changes:
            implementer_work_item_id = str(
                normalized_changes.get("work_item_id")
                or normalized_changes.get("provider_work_item_id")
                or previous.work_item_id
                or ""
            ).strip()
            consumer_module = str(normalized_changes.get("consumer_module") or previous.consumer_module)
            normalized_changes["functions"] = self._incoming_contract_functions(
                normalized_changes,
                consumer_module=consumer_module,
                implementer_work_item_id=implementer_work_item_id,
                existing_functions=previous.functions,
            )
        updated = self.workflow_store.update_contract(record_id, **normalized_changes)
        self._sync_contract_implementer_membership(updated, previous_work_item_id=previous.work_item_id)
        if self._is_contract_terminal(updated.status):
            lifecycle = dict(updated.metadata.get("lifecycle", {}))
            if not lifecycle.get("resolved_at"):
                lifecycle["resolved_at"] = self._now_iso()
                updated = self.workflow_store.update_contract(record_id, metadata={**updated.metadata, "lifecycle": lifecycle})
        self._emit_workflow_event("workflow.contract.updated", self._serialize_workflow_record(updated))
        self._apply_contract_side_effects(updated, normalized_changes)
        self._apply_contract_version_invalidation(previous, updated, normalized_changes)
        return updated

    def delete_contract(self, record_id: str) -> bool:
        previous = self.get_contract(record_id)
        deleted = self.workflow_store.delete_contract(record_id)
        if deleted and previous is not None:
            self._unlink_contract_from_work_item(previous.work_item_id, previous.id)
        self._emit_workflow_event("workflow.contract.deleted", {"id": record_id, "deleted": deleted})
        return deleted

    def create_dependency_edge(self, fields: dict[str, Any]) -> DependencyEdgeRecord:
        record = DependencyEdgeRecord(
            id=str(fields.get("id") or uuid.uuid4())[:36],
            source_work_item_id=str(fields["source_work_item_id"]),
            target_work_item_id=str(fields["target_work_item_id"]),
            edge_type=str(fields.get("edge_type", "depends_on")),
            status=str(fields.get("status", "active")),
            metadata=dict(fields.get("metadata", {})),
        )
        created = self.workflow_store.create_dependency_edge(record)
        self._sync_dependency_edges_for_target(created.target_work_item_id)
        self._sync_work_item_dependency_blockers(created.source_work_item_id)
        self._emit_workflow_event("workflow.dependency_edge.created", self._serialize_workflow_record(created))
        return created

    def get_dependency_edge(self, record_id: str) -> DependencyEdgeRecord | None:
        return self.workflow_store.get_dependency_edge(record_id)

    def list_dependency_edges(self, filters: dict[str, Any] | None = None, limit: int = 100) -> list[DependencyEdgeRecord]:
        filters = filters or {}
        return self.workflow_store.list_dependency_edges(
            source_work_item_id=filters.get("source_work_item_id"),
            target_work_item_id=filters.get("target_work_item_id"),
            edge_type=filters.get("edge_type"),
            status=filters.get("status"),
            limit=limit,
        )

    def update_dependency_edge(self, record_id: str, changes: dict[str, Any]) -> DependencyEdgeRecord:
        previous = self.get_dependency_edge(record_id)
        updated = self.workflow_store.update_dependency_edge(record_id, **changes)
        for work_item_id in {item for item in [updated.source_work_item_id, previous.source_work_item_id if previous else ""] if item}:
            self._sync_work_item_dependency_blockers(work_item_id)
        self._emit_workflow_event("workflow.dependency_edge.updated", self._serialize_workflow_record(updated))
        return updated

    def delete_dependency_edge(self, record_id: str) -> bool:
        previous = self.get_dependency_edge(record_id)
        deleted = self.workflow_store.delete_dependency_edge(record_id)
        if deleted and previous is not None:
            self._sync_work_item_dependency_blockers(previous.source_work_item_id)
        self._emit_workflow_event("workflow.dependency_edge.deleted", {"id": record_id, "deleted": deleted})
        return deleted

    def create_decision(self, fields: dict[str, Any]) -> DecisionRecord:
        record = DecisionRecord(
            id=str(fields.get("id") or uuid.uuid4())[:36],
            work_item_id=str(fields["work_item_id"]),
            decision_type=str(fields["decision_type"]),
            status=str(fields.get("status", "pending")),
            options=list(fields.get("options", [])),
            chosen_option=str(fields.get("chosen_option", "")),
            decider=str(fields.get("decider", "")),
            rationale=str(fields.get("rationale", "")),
            metadata=dict(fields.get("metadata", {})),
        )
        created = self.workflow_store.create_decision(record)
        self._emit_workflow_event("workflow.decision.created", self._serialize_workflow_record(created))
        self._apply_decision_side_effects(created)
        return created

    def get_decision(self, record_id: str) -> DecisionRecord | None:
        return self.workflow_store.get_decision(record_id)

    def list_decisions(self, filters: dict[str, Any] | None = None, limit: int = 100) -> list[DecisionRecord]:
        filters = filters or {}
        return self.workflow_store.list_decisions(
            work_item_id=filters.get("work_item_id"),
            decision_type=filters.get("decision_type"),
            status=filters.get("status"),
            decider=filters.get("decider"),
            limit=limit,
        )

    def update_decision(self, record_id: str, changes: dict[str, Any]) -> DecisionRecord:
        updated = self.workflow_store.update_decision(record_id, **changes)
        if self._is_decision_terminal(updated.status):
            lifecycle = dict(updated.metadata.get("lifecycle", {}))
            if not lifecycle.get("resolved_at"):
                lifecycle["resolved_at"] = self._now_iso()
                updated = self.workflow_store.update_decision(record_id, metadata={**updated.metadata, "lifecycle": lifecycle})
        self._emit_workflow_event("workflow.decision.updated", self._serialize_workflow_record(updated))
        self._apply_decision_side_effects(updated)
        return updated

    def delete_decision(self, record_id: str) -> bool:
        deleted = self.workflow_store.delete_decision(record_id)
        self._emit_workflow_event("workflow.decision.deleted", {"id": record_id, "deleted": deleted})
        return deleted

    def manage_workflow_state(
        self,
        entity: str,
        action: str,
        record_id: str | None = None,
        fields: dict[str, Any] | None = None,
        filters: dict[str, Any] | None = None,
        limit: int = 100,
    ) -> str:
        fields = fields or {}
        filters = filters or {}
        if entity == "scheduler":
            if action != "tick":
                raise ValueError("scheduler entity only supports 'tick' action")
            return json.dumps(self.run_workflow_scheduler_tick(limit=limit), ensure_ascii=False, indent=2)

        entity_map = {
            "work_item": (self.create_work_item, self.get_work_item, self.list_work_items, self.update_work_item, self.delete_work_item),
            "contract": (self.create_contract, self.get_contract, self.list_contracts, self.update_contract, self.delete_contract),
            "dependency_edge": (self.create_dependency_edge, self.get_dependency_edge, self.list_dependency_edges, self.update_dependency_edge, self.delete_dependency_edge),
            "decision": (self.create_decision, self.get_decision, self.list_decisions, self.update_decision, self.delete_decision),
        }
        create_fn, get_fn, list_fn, update_fn, delete_fn = entity_map[entity]

        if action == "create":
            return json.dumps(self._serialize_workflow_record(create_fn(fields)), ensure_ascii=False, indent=2)
        if action == "get":
            if not record_id:
                raise ValueError("record_id is required for get")
            record = get_fn(record_id)
            return json.dumps(self._serialize_workflow_record(record), ensure_ascii=False, indent=2) if record else "Not found"
        if action == "list":
            records = [self._serialize_workflow_record(record) for record in list_fn(filters, limit)]
            return json.dumps(records, ensure_ascii=False, indent=2)
        if action == "update":
            if not record_id:
                raise ValueError("record_id is required for update")
            return json.dumps(self._serialize_workflow_record(update_fn(record_id, fields)), ensure_ascii=False, indent=2)
        if action == "delete":
            if not record_id:
                raise ValueError("record_id is required for delete")
            return json.dumps({"deleted": delete_fn(record_id), "id": record_id}, ensure_ascii=False, indent=2)
        raise ValueError(f"Unsupported workflow action: {action}")

    def _discover_default_project_scopes(self) -> list[str]:
        """Discover a conservative default set of delegable top-level directories."""
        scopes: list[str] = []
        try:
            entries = sorted(self.workspace.iterdir(), key=lambda item: item.name.lower())
        except OSError:
            return scopes

        for entry in entries:
            if not entry.is_dir():
                continue
            if entry.name.startswith(".") and entry.name not in {".github", ".vscode"}:
                continue
            if entry.name in self._scope_exclude_names:
                continue
            scopes.append(entry.relative_to(self.workspace).as_posix())
        return scopes

    def _coerce_project_registry_entry(self, entry: Any) -> ProjectScopeRegistration | None:
        """Normalize config input into a fixed project-scope registration."""
        if isinstance(entry, str):
            raw_path = entry
            owner = ""
            description = ""
            prompt_hint = ""
            tags: list[str] = []
        elif isinstance(entry, dict):
            raw_path = entry.get("path") or entry.get("project") or entry.get("scope")
            owner = str(entry.get("owner") or "").strip()
            description = str(entry.get("description") or "").strip()
            prompt_hint = str(entry.get("prompt_hint") or "").strip()
            tags = [str(tag).strip() for tag in entry.get("tags", []) if str(tag).strip()]
        else:
            raw_path = getattr(entry, "path", None) or getattr(entry, "project", None) or getattr(entry, "scope", None)
            owner = str(getattr(entry, "owner", "") or "").strip()
            description = str(getattr(entry, "description", "") or "").strip()
            prompt_hint = str(getattr(entry, "prompt_hint", "") or "").strip()
            tags = [str(tag).strip() for tag in getattr(entry, "tags", []) if str(tag).strip()]

        normalized = str(raw_path or "").strip().replace("\\", "/").strip("/")
        if not normalized:
            return None

        scope_path = (self.workspace / normalized).resolve()
        try:
            within_workspace = scope_path.is_relative_to(self.workspace)
        except AttributeError:
            within_workspace = str(scope_path).startswith(str(self.workspace))

        if not within_workspace:
            return None
        if not scope_path.exists() or not scope_path.is_dir():
            return None

        return ProjectScopeRegistration(
            path=normalized,
            owner=owner,
            description=description,
            prompt_hint=prompt_hint,
            tags=tuple(tags),
        )

    def _build_project_registry(
        self,
        configured_entries: list[Any] | None,
        allowed_scopes: list[str],
    ) -> dict[str, ProjectScopeRegistration]:
        """Build the fixed project-agent registry and its whitelist."""
        registry: dict[str, ProjectScopeRegistration] = {}

        if configured_entries:
            source_entries: list[Any] = configured_entries
        elif allowed_scopes:
            source_entries = allowed_scopes
        else:
            source_entries = self._discover_default_project_scopes()

        for entry in source_entries:
            registration = self._coerce_project_registry_entry(entry)
            if registration is None:
                continue
            registry[registration.path] = registration

        return registry

    def get_project_registry(self) -> list[ProjectScopeRegistration]:
        """Return the fixed project-agent registry entries."""
        return [self._project_registry[path] for path in sorted(self._project_registry)]

    def set_project_runtime_attributes(self, project: str, attributes: dict[str, Any]) -> dict[str, Any]:
        """Set runtime control attributes for one project scope."""
        normalized, _ = self._normalize_project(project)
        next_attributes = dict(attributes or {})
        self._project_runtime_attributes[normalized] = next_attributes
        self._emit_workflow_event(
            "workflow.project_agent.attributes.updated",
            {
                "project": normalized,
                "attributes": next_attributes,
            },
        )
        return next_attributes

    def get_project_runtime_attributes(self, project: str) -> dict[str, Any]:
        """Get runtime control attributes for one project scope."""
        normalized, _ = self._normalize_project(project)
        return dict(self._project_runtime_attributes.get(normalized, {}))

    def get_project_workers_status(self) -> dict[str, Any]:
        """Query project worker subprocesses status and decision blocking state.

        Returns:
            dict with:
              - projects: dict of project -> pending_request_count
              - total_pending_requests: int
              - decision_blocked: dict with blocked_modules and global_blocked
        """
        projects: dict[str, int] = {}
        total_pending = 0
        for project, handle in self._project_subprocesses.items():
            count = len(handle.pending_requests)
            projects[project] = count
            total_pending += count

        return {
            "projects": projects,
            "total_pending_requests": total_pending,
            "decision_blocked": {
                "blocked_modules": sorted(self._decision_sla_blocked_modules),
                "global_blocked": self._decision_sla_global_block,
            },
        }

    def _project_scheduler_profile(self, project: str) -> dict[str, Any]:
        """Normalize project runtime attributes into scheduler controls."""
        normalized, _ = self._normalize_project(project)
        raw_attributes = dict(self._project_runtime_attributes.get(normalized, {}))
        scheduler_attributes: dict[str, Any] = {}

        nested_scheduler = raw_attributes.get("scheduler")
        if isinstance(nested_scheduler, dict):
            scheduler_attributes.update(nested_scheduler)

        for key in ("dispatch_enabled", "priority_bias", "concurrency_cap", "min_priority"):
            if key in raw_attributes:
                scheduler_attributes[key] = raw_attributes[key]

        dispatch_enabled = bool(scheduler_attributes.get("dispatch_enabled", True))
        priority_bias = int(scheduler_attributes.get("priority_bias", 0))
        concurrency_cap = max(1, int(scheduler_attributes.get("concurrency_cap", 1)))
        min_priority = int(scheduler_attributes.get("min_priority", -1_000_000))

        return {
            "project": normalized,
            "dispatch_enabled": dispatch_enabled,
            "priority_bias": priority_bias,
            "concurrency_cap": concurrency_cap,
            "min_priority": min_priority,
            "raw_attributes": raw_attributes,
        }

    @staticmethod
    def _dispatch_score(work_item: WorkItemRecord, project_profile: dict[str, Any]) -> int:
        """Compute one dispatch score from work-item priority and project bias."""
        return int(work_item.priority) + int(project_profile.get("priority_bias", 0))

    def _module_inflight_count(self, module: str) -> int:
        """Count running dispatches for one project module."""
        count = 0
        for work_item_id in self._running_work_item_dispatches:
            item = self.get_work_item(work_item_id)
            if item is not None and item.module == module:
                count += 1
        return count

    def _build_dispatch_plan(self, ready_items: list[WorkItemRecord]) -> list[dict[str, Any]]:
        """Rank ready work items by priority and project attributes."""
        plan: list[dict[str, Any]] = []
        for item in ready_items:
            if item.module not in self._allowed_project_scopes:
                continue
            if self._decision_sla_global_block:
                continue
            if item.module in self._decision_sla_blocked_modules:
                continue

            profile = self._project_scheduler_profile(item.module)
            if not profile["dispatch_enabled"]:
                continue
            if int(item.priority) < int(profile["min_priority"]):
                continue

            score = self._dispatch_score(item, profile)
            plan.append(
                {
                    "work_item_id": item.id,
                    "module": item.module,
                    "score": score,
                    "priority": item.priority,
                    "priority_bias": profile["priority_bias"],
                    "concurrency_cap": profile["concurrency_cap"],
                    "profile": profile,
                }
            )

        plan.sort(
            key=lambda entry: (
                -int(entry["score"]),
                -int(entry["priority"]),
                str(entry["module"]),
                str(entry["work_item_id"]),
            )
        )
        return plan

    def list_project_scopes(self, max_depth: int = 2, include_files: bool = False) -> list[str]:
        """List delegable project scopes under the core workspace."""
        if self._project_registry:
            return sorted(self._project_registry)

        max_depth = max(1, min(max_depth, 4))
        scopes: list[str] = []

        def _walk(base: Path, depth: int) -> None:
            if depth > max_depth:
                return

            try:
                entries = sorted(base.iterdir(), key=lambda item: (not item.is_dir(), item.name.lower()))
            except OSError:
                return

            for entry in entries:
                if entry.name.startswith(".") and entry.name not in {".github", ".vscode"}:
                    continue
                if entry.name in self._scope_exclude_names:
                    continue

                if entry.is_dir():
                    rel = entry.relative_to(self.workspace).as_posix()
                    scopes.append(rel)
                    _walk(entry, depth + 1)
                elif include_files and depth == 1:
                    rel = entry.relative_to(self.workspace).as_posix()
                    scopes.append(rel)

        _walk(self.workspace, 1)
        return scopes

    def _normalize_project(self, project: str) -> tuple[str, Path]:
        normalized = project.strip().replace("\\", "/").strip("/")
        if not normalized:
            raise ValueError("project must be a non-empty relative subdirectory path")

        if normalized not in self._allowed_project_scopes:
            allowed = ", ".join(sorted(self._allowed_project_scopes)) if self._allowed_project_scopes else "(none)"
            raise ValueError(f"project '{normalized}' is not in the allowed project scope whitelist: {allowed}")

        project_path = (self.workspace / normalized).resolve()
        try:
            within_workspace = project_path.is_relative_to(self.workspace)
        except AttributeError:
            within_workspace = str(project_path).startswith(str(self.workspace))

        if not within_workspace:
            raise ValueError("project must stay within the core workspace")
        if not project_path.exists() or not project_path.is_dir():
            raise ValueError(f"project directory not found: {normalized}")

        return normalized, project_path

    async def _ensure_project_subprocess(self, project: str) -> ProjectSubprocessHandle:
        """Start (or return existing) persistent subprocess for a project."""
        normalized, _ = self._normalize_project(project)
        existing = self._project_subprocesses.get(normalized)
        if existing is not None and existing.process.returncode is None:
            return existing

        registration = self._project_registry[normalized]
        scope_lines = [f"Project scope: {normalized}"]
        if registration.owner:
            scope_lines.append(f"Owner: {registration.owner}")
        if registration.description:
            scope_lines.append(f"Description: {registration.description}")
        if registration.prompt_hint:
            scope_lines.append(f"Prompt hint: {registration.prompt_hint}")
        scope_hint = "\n".join(scope_lines)

        # Find the worker module path relative to the venv / python path
        worker_module = "nanobot.agent.project_worker"
        python_exe = self._find_python_executable()

        cmd = [
            python_exe,
            "-m", worker_module,
            "--config-path", str(self._config_path or ""),
            "--workspace", str(self.workspace),
            "--project", normalized,
            "--scope-hint", scope_hint,
            "--provider-type", self._worker_provider_type,
        ]

        logger = __import__("loguru").logger
        logger.info("Starting project subprocess for {}: {}", normalized, " ".join(cmd))

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=1024 * 1024,  # 1MB buffer
        )

        from datetime import timezone
        handle = ProjectSubprocessHandle(
            project=normalized,
            process=process,
            pid=process.pid or 0,
            started_at=datetime.now(timezone.utc).isoformat(),
            scope_hint=scope_hint,
        )
        self._project_subprocesses[normalized] = handle

        handle.reader_task = asyncio.create_task(self._read_subprocess_stdout(handle))

        # Start background stderr logger
        asyncio.create_task(self._pipe_subprocess_stderr(normalized, process))

        # Also keep in old project_loops dict for backward compatibility
        self.project_loops[normalized] = None  # placeholder

        return handle

    @staticmethod
    def _find_python_executable() -> str:
        """Find the Python executable to use for subprocess workers."""
        import sys
        return sys.executable

    async def _pipe_subprocess_stderr(self, project: str, process: asyncio.subprocess.Process) -> None:
        """Read and log subprocess stderr in the background."""
        logger = __import__("loguru").logger
        try:
            while True:
                line = await process.stderr.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").rstrip()
                if text:
                    logger.info("[project:{}] {}", project, text)
        except Exception:
            pass

    async def _read_subprocess_stdout(self, handle: ProjectSubprocessHandle) -> None:
        """Continuously read completion messages from one project subprocess."""
        logger = __import__("loguru").logger
        process = handle.process
        try:
            while True:
                line = await process.stdout.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").strip()
                if not text:
                    continue
                try:
                    response = json.loads(text)
                except json.JSONDecodeError as exc:
                    logger.warning("Ignoring invalid project subprocess JSON for {}: {}", handle.project, exc)
                    continue

                response_type = str(response.get("type") or "").strip().lower()
                request_id = str(response.get("id") or "").strip()
                if response_type == "decision_request":
                    request_state = handle.pending_requests.get(request_id)
                    if request_state is None:
                        logger.warning("Received orphan project decision request for {}: {}", handle.project, request_id)
                        continue
                    await self._handle_project_subprocess_decision_request(request_state, response)
                    continue

                request_state = handle.pending_requests.pop(request_id, None)
                if request_state is None:
                    logger.warning("Received orphan project subprocess response for {}: {}", handle.project, request_id)
                    continue
                await self._handle_project_subprocess_response(request_state, response)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("Project subprocess reader failed for {}: {}", handle.project, exc)
            await self._fail_pending_project_requests(handle, str(exc))
            return

        if handle.pending_requests and process.returncode is not None:
            await self._fail_pending_project_requests(
                handle,
                f"Subprocess for {handle.project} exited with code {process.returncode}",
            )

    async def _handle_project_subprocess_response(
        self,
        request_state: PendingProjectRequest,
        response: dict[str, Any],
    ) -> None:
        """Resolve one queued project request and publish its completion notification."""
        self._clear_pending_project_decisions_for_request(request_state.request_id)
        success = bool(response.get("success", False))
        result_text = str(response.get("result") or "")
        error_text = str(response.get("error") or "Unknown subprocess error")

        future = request_state.future
        if future is not None and not future.done():
            if success:
                future.set_result(response)
            else:
                future.set_exception(RuntimeError(error_text))

        if success:
            self._emit_workflow_event(
                "workflow.project_agent.delegation.completed",
                {
                    "project": request_state.project,
                    "session_key": request_state.session_key,
                    "channel": request_state.channel,
                    "chat_id": request_state.chat_id,
                    "result_preview": result_text[:300],
                    "request_id": request_state.request_id,
                },
            )
        else:
            self._emit_workflow_event(
                "workflow.project_agent.delegation.failed",
                {
                    "project": request_state.project,
                    "session_key": request_state.session_key,
                    "channel": request_state.channel,
                    "chat_id": request_state.chat_id,
                    "error": error_text,
                    "request_id": request_state.request_id,
                },
            )

        if not request_state.publish_completion_message:
            return

        message_content = (
            f"[Project Scope: {request_state.project}]\n{result_text}"
            if success
            else f"[Project Scope: {request_state.project}]\nError: {error_text}"
        )
        await self.bus.publish_inbound(
            InboundMessage(
                channel="system",
                sender_id="project_agent_completion",
                chat_id=f"{request_state.channel}:{request_state.chat_id}",
                content=message_content,
                metadata={
                    "passthrough": True,
                    "project": request_state.project,
                    "request_id": request_state.request_id,
                },
            )
        )

    async def _handle_project_subprocess_decision_request(
        self,
        request_state: PendingProjectRequest,
        response: dict[str, Any],
    ) -> None:
        """Forward a project-agent decision request to the frontend and keep the task pending."""
        decision_id = str(response.get("decision_id") or "").strip()
        if not decision_id:
            raise RuntimeError(f"Project subprocess decision request missing decision_id for {request_state.project}")

        prompt = str(response.get("prompt") or "").strip()
        options = [str(item) for item in (response.get("options") or []) if str(item).strip()]
        self._pending_project_decisions[decision_id] = PendingProjectDecision(
            decision_id=decision_id,
            request_id=request_state.request_id,
            project=request_state.project,
            channel=request_state.channel,
            chat_id=request_state.chat_id,
            session_key=request_state.session_key,
            prompt=prompt,
            options=options,
        )
        await self.bus.publish_outbound(
            OutboundMessage(
                channel=request_state.channel,
                chat_id=request_state.chat_id,
                content=f"[Project Scope: {request_state.project}]\nDecision required: {prompt}",
                metadata={
                    "type": "project_agent_decision_request",
                    "project_decision_id": decision_id,
                    "project": request_state.project,
                    "request_id": request_state.request_id,
                    "options": options,
                },
            )
        )

    def _clear_pending_project_decisions_for_request(self, request_id: str) -> None:
        stale_ids = [decision_id for decision_id, state in self._pending_project_decisions.items() if state.request_id == request_id]
        for decision_id in stale_ids:
            self._pending_project_decisions.pop(decision_id, None)

    async def _maybe_handle_project_decision_reply(self, msg: InboundMessage) -> bool:
        """Route a frontend decision reply back to the waiting project subprocess."""
        decision_id = str((msg.metadata or {}).get("project_decision_id") or "").strip()
        if not decision_id:
            return False

        decision_state = self._pending_project_decisions.pop(decision_id, None)
        if decision_state is None:
            return False

        handle = self._project_subprocesses.get(decision_state.project)
        if handle is None:
            raise RuntimeError(f"Project subprocess not found for pending decision {decision_id}")

        await self._send_subprocess_request(
            handle,
            {
                "type": "decision_response",
                "decision_id": decision_id,
                "content": msg.content,
            },
        )
        return True

    async def _fail_pending_project_requests(self, handle: ProjectSubprocessHandle, reason: str) -> None:
        """Fail all queued requests when a subprocess dies or its reader collapses."""
        pending = list(handle.pending_requests.values())
        handle.pending_requests.clear()
        for request_state in pending:
            future = request_state.future
            if future is not None and not future.done():
                future.set_exception(RuntimeError(reason))
            await self._handle_project_subprocess_response(
                request_state,
                {
                    "id": request_state.request_id,
                    "success": False,
                    "error": reason,
                },
            )

    async def _send_subprocess_request(
        self,
        handle: ProjectSubprocessHandle,
        request: dict[str, Any],
    ) -> None:
        """Send a JSON request to a subprocess without waiting for the response."""
        process = handle.process
        if process.returncode is not None:
            raise RuntimeError(f"Subprocess for {handle.project} exited with code {process.returncode}")

        req_line = json.dumps(request, ensure_ascii=False) + "\n"
        assert process.stdin is not None
        process.stdin.write(req_line.encode("utf-8"))
        await process.stdin.drain()

    async def _submit_project_task(
        self,
        *,
        project: str,
        task: str,
        session_key: str,
        channel: str,
        chat_id: str,
        publish_completion_message: bool,
    ) -> PendingProjectRequest:
        """Queue one project task in its subprocess and return a future resolved by the reader task."""
        normalized, _ = self._normalize_project(project)
        handle = await self._ensure_project_subprocess(project)
        project_session = f"{session_key}::project::{normalized.replace('/', ':')}"
        runtime_attributes = dict(self._project_runtime_attributes.get(normalized, {}))
        delegated_task = task
        if runtime_attributes:
            attributes_json = json.dumps(runtime_attributes, ensure_ascii=False, indent=2)
            delegated_task = (
                "[Runtime Control Attributes]\n"
                f"{attributes_json}\n\n"
                "Use the attributes above as hard constraints and operation preferences for this task.\n\n"
                f"[Task]\n{task}"
            )

        request_id = f"deleg-{normalized}-{self._next_subprocess_req_id}"
        self._next_subprocess_req_id += 1
        request_state = PendingProjectRequest(
            request_id=request_id,
            project=normalized,
            channel=channel,
            chat_id=chat_id,
            session_key=project_session,
            submitted_at=self._now_iso(),
            task_preview=task[:200],
            publish_completion_message=publish_completion_message,
            future=asyncio.get_running_loop().create_future(),
        )
        handle.pending_requests[request_id] = request_state

        self._emit_workflow_event(
            "workflow.project_agent.delegation.started",
            {
                "project": normalized,
                "session_key": project_session,
                "channel": channel,
                "chat_id": chat_id,
                "has_runtime_attributes": bool(runtime_attributes),
                "task_preview": task[:200],
                "request_id": request_id,
            },
        )

        try:
            await self._send_subprocess_request(
                handle,
                {
                    "id": request_id,
                    "task": delegated_task,
                    "session_key": project_session,
                    "channel": channel,
                    "chat_id": chat_id,
                },
            )
        except Exception as exc:
            handle.pending_requests.pop(request_id, None)
            future = request_state.future
            if future is not None and not future.done():
                future.set_exception(exc)
            self._emit_workflow_event(
                "workflow.project_agent.delegation.failed",
                {
                    "project": normalized,
                    "session_key": project_session,
                    "channel": channel,
                    "chat_id": chat_id,
                    "error": str(exc),
                    "request_id": request_id,
                },
            )
            raise

        return request_state

    async def delegate_project_task(
        self,
        project: str,
        task: str,
        session_key: str,
        channel: str,
        chat_id: str,
    ) -> str:
        request_state = await self._submit_project_task(
            project=project,
            task=task,
            session_key=session_key,
            channel=channel,
            chat_id=chat_id,
            publish_completion_message=True,
        )
        return (
            f"Queued task for project scope '{request_state.project}' asynchronously "
            f"(request_id={request_state.request_id}). A completion message will be sent when it finishes."
        )

    async def delegate_projects_batch(
        self,
        items: list[dict[str, str]],
        session_key: str,
        channel: str,
        chat_id: str,
    ) -> str:
        """Start a concurrent batch of project tasks and return a handle immediately."""
        if not items:
            raise ValueError("items must contain at least one project task")

        normalized_items: list[dict[str, str]] = []
        for item in items:
            project = (item.get("project") or "").strip()
            task = (item.get("task") or "").strip()
            if not project or not task:
                raise ValueError("Each batch item must include non-empty 'project' and 'task'")
            normalized_items.append({"project": project, "task": task})

        batch_id = str(uuid.uuid4())[:8]
        self._batch_status[batch_id] = {
            "status": "running",
            "items": normalized_items,
            "results": [],
        }

        batch_task = asyncio.create_task(
            self._run_delegate_projects_batch(
                batch_id=batch_id,
                items=normalized_items,
                session_key=session_key,
                channel=channel,
                chat_id=chat_id,
            )
        )
        self._running_batch_tasks[batch_id] = batch_task
        batch_task.add_done_callback(lambda _: self._running_batch_tasks.pop(batch_id, None))

        return (
            f"Batch delegation started (id: {batch_id}) for {len(normalized_items)} project tasks. "
            "Use get_batch_delegation_status with this id to inspect progress. "
            "I'll also report back when the batch completes."
        )

    async def _run_delegate_projects_batch(
        self,
        batch_id: str,
        items: list[dict[str, str]],
        session_key: str,
        channel: str,
        chat_id: str,
    ) -> None:
        """Execute a batch delegation in the background and store its results."""

        async def _delegate(index: int, item: dict[str, str]) -> tuple[int, str, str]:
            project = (item.get("project") or "").strip()
            task = (item.get("task") or "").strip()
            if not project or not task:
                raise ValueError("Each batch item must include non-empty 'project' and 'task'")

            batch_session = f"{session_key}::batch::{index}"
            request_state = await self._submit_project_task(
                project=project,
                task=task,
                session_key=batch_session,
                channel=channel,
                chat_id=chat_id,
                publish_completion_message=False,
            )
            response = await asyncio.wait_for(request_state.future, timeout=1200.0)
            result = f"[Project Scope: {request_state.project}]\n{str(response.get('result') or '')}"
            return index, request_state.project, result

        results = await asyncio.gather(
            *[_delegate(index, item) for index, item in enumerate(items, start=1)],
            return_exceptions=True,
        )

        formatted_results: list[dict[str, Any]] = []
        had_error = False
        for index, result in enumerate(results, start=1):
            requested_project = items[index - 1]["project"]
            if isinstance(result, Exception):
                had_error = True
                formatted_results.append(
                    {
                        "index": index,
                        "project": requested_project,
                        "status": "error",
                        "error": str(result),
                    }
                )
                continue

            _, project, output = result
            formatted_results.append(
                {
                    "index": index,
                    "project": project,
                    "status": "ok",
                    "output": output,
                }
            )

        self._batch_status[batch_id] = {
            "status": "completed_with_errors" if had_error else "completed",
            "items": items,
            "results": formatted_results,
        }
        await self._announce_batch_result(batch_id, formatted_results, channel=channel, chat_id=chat_id)

    async def _announce_batch_result(
        self,
        batch_id: str,
        results: list[dict[str, Any]],
        channel: str,
        chat_id: str,
    ) -> None:
        """Send the completed batch summary back through the core loop."""
        lines = [f"[Project batch {batch_id} completed]", "", "Results:"]
        for result in results:
            index = result["index"]
            project = result["project"]
            if result["status"] == "error":
                lines.append(f"- Item {index} ({project}) failed: {result['error']}")
                continue
            lines.append(f"- Item {index} ({project}) completed")
            lines.append(result["output"])

        lines.append("")
        lines.append("Summarize the batch completion naturally for the user. Highlight failures first. Keep it brief.")
        await self.bus.publish_inbound(
            InboundMessage(
                channel="system",
                sender_id="project_batch",
                chat_id=f"{channel}:{chat_id}",
                content="\n".join(lines),
            )
        )

    def get_batch_delegation_status(self, batch_id: str) -> str:
        """Return a human-readable status report for one batch delegation handle."""
        state = self._batch_status.get(batch_id)
        if state is None:
            raise ValueError(f"Unknown batch delegation id: {batch_id}")

        lines = [f"Batch delegation [{batch_id}] status: {state['status']}"]
        items: list[dict[str, str]] = state.get("items", [])
        if items:
            lines.append("Requested items:")
            for index, item in enumerate(items, start=1):
                lines.append(f"- {index}. {item['project']}: {item['task']}")

        results: list[dict[str, Any]] = state.get("results", [])
        if results:
            lines.append("Results:")
            for result in results:
                if result["status"] == "error":
                    lines.append(f"- {result['index']}. {result['project']}: ERROR - {result['error']}")
                else:
                    lines.append(f"- {result['index']}. {result['project']}: OK")
                    lines.append(result["output"])

        return "\n".join(lines)

    async def run(self) -> None:
        """Run the core agent loop against the shared message bus."""
        logger = __import__("loguru").logger
        # await self._start_reconciler()
        self.core_loop._running = True
        logger.info("Agent loop started")
        try:
            while self.core_loop._running:
                try:
                    msg = await asyncio.wait_for(
                        self.bus.consume_inbound(),
                        timeout=1.0,
                    )
                except asyncio.TimeoutError:
                    continue

                if await self._maybe_handle_project_decision_reply(msg):
                    continue

                try:
                    response = await self.core_loop._process_message(msg)
                    if response:
                        await self.bus.publish_outbound(response)
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    await self.bus.publish_outbound(OutboundMessage(
                        channel=msg.channel,
                        chat_id=msg.chat_id,
                        content=f"Sorry, I encountered an error: {str(e)}"
                    ))
        finally:
            await self._stop_reconciler()
            await self._shutdown_project_subprocesses()

    def stop(self) -> None:
        """Stop the core loop and terminate all project subprocesses."""
        if self._reconciler_task is not None:
            self._reconciler_task.cancel()
            self._reconciler_task = None
        for dispatch_task in list(self._running_work_item_dispatches.values()):
            dispatch_task.cancel()
        self._running_work_item_dispatches.clear()
        self.core_loop.stop()

    async def process_direct(
        self,
        content: str,
        session_key: str = "cli:direct",
        channel: str = "cli",
        chat_id: str = "direct",
    ) -> str:
        """Process a direct message through the core agent."""
        return await self.core_loop.process_direct(
            content,
            session_key=session_key,
            channel=channel,
            chat_id=chat_id,
        )
