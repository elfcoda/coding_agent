"""Thin coordinator over a core agent and project-scoped agent loops."""

from __future__ import annotations

import asyncio
import uuid
import json
import re
from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TYPE_CHECKING

from nanobot.agent.loop import AgentLoop
from nanobot.agent.tools.batch_delegate import DelegateProjectsBatchTool
from nanobot.agent.tools.batch_status import GetBatchDelegationStatusTool
from nanobot.agent.tools.delegate import DelegateProjectTaskTool
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


class CoreAgentManager:
    """Coordinate a core agent and lazily-created project agents."""

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
        scheduler_reconcile_interval_seconds: float = 3.0,
        scheduler_max_concurrent_dispatches: int = 4,
        revalidation_chain_enabled: bool = False,
        revalidation_chain_edge_types: list[str] | None = None,
        revalidation_chain_max_depth: int = 2,
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
        self._running_batch_tasks: dict[str, asyncio.Task[None]] = {}
        self._batch_status: dict[str, dict[str, Any]] = {}
        self._reconciler_interval_seconds = max(0.2, float(scheduler_reconcile_interval_seconds))
        self._scheduler_max_concurrent_dispatches = max(1, int(scheduler_max_concurrent_dispatches))
        self._revalidation_chain_enabled = bool(revalidation_chain_enabled)
        edge_types = [str(item).strip() for item in (revalidation_chain_edge_types or ["requires_contract"]) if str(item).strip()]
        self._revalidation_chain_edge_types = set(edge_types or ["requires_contract"])
        self._revalidation_chain_max_depth = max(1, int(revalidation_chain_max_depth))
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
        self.core_loop.tools.register(RequestContractStubTool(self))
        self.core_loop.tools.register(ManageWorkflowStateTool(self))

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
                "work_item_id": consumer_linked_work_item,
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
    def _is_contract_terminal(status: str) -> bool:
        return status in {"accepted", "implemented", "completed", "rejected", "invalidated", "deprecated", "superseded"}

    @staticmethod
    def _is_decision_terminal(status: str) -> bool:
        return status in {"approved", "rejected", "completed"}

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
        incoming_consumer_work_item_id = str(fields.get("consumer_work_item_id") or "").strip()
        incoming_provider_work_item_id = str(fields.get("provider_work_item_id") or "").strip()

        merged_work_item_ids = self._unique_list(
            [str(item).strip() for item in metadata.get("merged_work_item_ids", []) if str(item).strip()]
            + ([existing.work_item_id] if str(existing.work_item_id).strip() else [])
            + ([incoming_work_item_id] if incoming_work_item_id else [])
        )
        merged_consumer_work_item_ids = self._unique_list(
            [str(item).strip() for item in metadata.get("merged_consumer_work_item_ids", []) if str(item).strip()]
            + ([existing.work_item_id] if str(existing.work_item_id).strip() else [])
            + ([incoming_consumer_work_item_id] if incoming_consumer_work_item_id else [])
        )
        merged_provider_work_item_ids = self._unique_list(
            [str(item).strip() for item in metadata.get("merged_provider_work_item_ids", []) if str(item).strip()]
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
                "work_item_id": incoming_work_item_id,
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
        incoming_stub_path = str(fields.get("stub_path") or "")
        if not existing.stub_path and incoming_stub_path:
            changes["stub_path"] = incoming_stub_path
        incoming_implementation_path = str(fields.get("implementation_path") or "")
        if not existing.implementation_path and incoming_implementation_path:
            changes["implementation_path"] = incoming_implementation_path
        if not str(existing.work_item_id).strip() and incoming_work_item_id:
            changes["work_item_id"] = incoming_work_item_id

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

    def _sync_contract_blocking(self, contract: ContractRecord) -> None:
        """Apply contract status to any linked work items that reference it as blocker."""
        resolved = contract.status in {"accepted", "implemented", "completed"}
        work_items = self.list_work_items(limit=500)
        for work_item in work_items:
            if contract.id not in work_item.blocked_by:
                continue

            next_blocked = [entry for entry in work_item.blocked_by if entry != contract.id] if resolved else list(work_item.blocked_by)
            updates: dict[str, Any] = {"blocked_by": next_blocked}
            if resolved and work_item.status == "blocked" and not next_blocked:
                updates["status"] = "ready"
            if not resolved and work_item.status not in {"waiting_decision", "blocked"}:
                updates["status"] = "blocked"

            updated = self.update_work_item(work_item.id, updates)
            self._emit_workflow_event("workflow.work_item.updated", self._serialize_workflow_record(updated))

    @staticmethod
    def _is_contract_resolved(status: str) -> bool:
        return status in {"accepted", "implemented", "completed"}

    @staticmethod
    def _is_contract_invalidation_status(status: str) -> bool:
        return status in {"requested", "draft", "invalidated", "deprecated", "superseded", "rejected"}

    def _linked_contract_edges(self, contract_id: str, limit: int = 2000) -> list[DependencyEdgeRecord]:
        edges = self.list_dependency_edges(limit=limit)
        return [edge for edge in edges if str(edge.metadata.get("contract_id") or "") == contract_id]

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
        """Mark linked edges/work items as requiring revalidation when a contract version becomes stale."""
        edges = self._linked_contract_edges(contract.id)
        direct_affected_ids: set[str] = set()
        depths: dict[str, int] = {}
        paths: dict[str, list[str]] = {}

        for edge in edges:
            next_metadata = {
                **edge.metadata,
                "contract_id": contract.id,
                "revalidation_required": True,
                "revalidation_reason": reason,
                "required_contract_version": contract.version,
            }
            updated_edge = self.update_dependency_edge(
                edge.id,
                {
                    "metadata": next_metadata,
                    "status": "active",
                },
            )
            source_id = updated_edge.source_work_item_id
            direct_affected_ids.add(source_id)
            depths[source_id] = 1
            paths[source_id] = [source_id]

        work_items = self.list_work_items(limit=500)
        for work_item in work_items:
            if contract.id not in work_item.blocked_by:
                continue
            direct_affected_ids.add(work_item.id)
            depths.setdefault(work_item.id, 1)
            paths.setdefault(work_item.id, [work_item.id])

        # 处理链式多跳的完整的依赖关系传播
        if self._revalidation_chain_enabled and direct_affected_ids:
            active_edges = self.list_dependency_edges(filters={"status": "active"}, limit=5000)
            incoming: dict[str, list[tuple[str, str]]] = {}
            for edge in active_edges:
                if edge.edge_type not in self._revalidation_chain_edge_types:
                    continue
                incoming.setdefault(edge.target_work_item_id, []).append((edge.source_work_item_id, edge.id))

            queue: deque[tuple[str, int, list[str]]] = deque(
                (work_item_id, depths.get(work_item_id, 1), paths.get(work_item_id, [work_item_id]))
                for work_item_id in direct_affected_ids
            )

            while queue:
                current_id, current_depth, current_path = queue.popleft()
                if current_depth >= self._revalidation_chain_max_depth:
                    continue

                for source_id, _ in incoming.get(current_id, []):
                    next_depth = current_depth + 1
                    previous_depth = depths.get(source_id)
                    if previous_depth is not None and previous_depth <= next_depth:
                        continue
                    next_path = [*current_path, source_id]
                    depths[source_id] = next_depth
                    paths[source_id] = next_path
                    queue.append((source_id, next_depth, next_path))

        affected_work_item_ids = set(depths)

        for work_item_id in affected_work_item_ids:
            work_item = self.get_work_item(work_item_id)
            if work_item is None:
                continue

            revalidation_meta = {
                "required": True,
                "contract_id": contract.id,
                "required_version": contract.version,
                "reason": reason,
                "depth": depths.get(work_item_id, 1),
                "path": paths.get(work_item_id, [work_item_id]),
            }
            metadata = {**work_item.metadata, "revalidation": revalidation_meta}
            next_blocked_by = self._unique_list(list(work_item.blocked_by) + [contract.id])
            updates: dict[str, Any] = {
                "metadata": metadata,
                "blocked_by": next_blocked_by,
            }
            if work_item.status not in {"blocked", "waiting_decision"}:
                updates["status"] = "blocked"

            updated = self.update_work_item(work_item.id, updates)
            self._emit_workflow_event("workflow.work_item.revalidation.required", self._serialize_workflow_record(updated))
            self._emit_workflow_event(
                "workflow.work_item.revalidation.propagated",
                {
                    "work_item_id": updated.id,
                    "contract_id": contract.id,
                    "reason": reason,
                    "depth": depths.get(work_item_id, 1),
                    "path": paths.get(work_item_id, [work_item_id]),
                },
            )

        self._emit_workflow_event(
            "workflow.contract.version.invalidated",
            {
                "contract_id": contract.id,
                "version": contract.version,
                "reason": reason,
                "affected_edge_count": len(edges),
                "direct_affected_work_item_count": len(direct_affected_ids),
                "affected_work_item_count": len(affected_work_item_ids),
                "chain_mode": "dag" if self._revalidation_chain_enabled else "one_hop",
                "chain_edge_types": sorted(self._revalidation_chain_edge_types),
                "max_depth": self._revalidation_chain_max_depth,
                "affected": [
                    {
                        "work_item_id": work_item_id,
                        "depth": depths.get(work_item_id, 1),
                        "path": paths.get(work_item_id, [work_item_id]),
                    }
                    for work_item_id in sorted(affected_work_item_ids)
                ],
            },
        )

    def _clear_contract_revalidation(self, contract: ContractRecord) -> None:
        """Clear revalidation markers only after executable checks pass."""
        report = self._run_contract_revalidation_checks(contract)
        if not report["passed"]:
            # 还有两个现实场景需要这个兜底
            # 1. 人工或其他流程把状态改成了 ready/in_progress，失败分支要强制回收。
            # 2. 历史数据漂移（metadata 有 revalidation 标记，但 blocker/status 不一致），失败分支要修复一致性。
            affected_work_item_ids: set[str] = set()
            edges = self._linked_contract_edges(contract.id)
            for edge in edges:
                affected_work_item_ids.add(edge.source_work_item_id)

            work_items = self.list_work_items(limit=500)
            for work_item in work_items:
                marker = dict(work_item.metadata.get("revalidation", {}))
                if str(marker.get("contract_id") or "") == contract.id:
                    affected_work_item_ids.add(work_item.id)

            for work_item_id in affected_work_item_ids:
                work_item = self.get_work_item(work_item_id)
                if work_item is None:
                    continue

                marker = dict(work_item.metadata.get("revalidation", {}))
                marker.update(
                    {
                        "required": True,
                        "contract_id": contract.id,
                        "required_version": marker.get("required_version") or contract.version,
                        "last_failed_checks": report["errors"],
                    }
                )
                next_blocked_by = self._unique_list(list(work_item.blocked_by) + [contract.id])
                updates: dict[str, Any] = {
                    "metadata": {**work_item.metadata, "revalidation": marker},
                    "blocked_by": next_blocked_by,
                }
                if work_item.status not in {"blocked", "waiting_decision"}:
                    updates["status"] = "blocked"

                updated = self.update_work_item(work_item.id, updates)
                self._emit_workflow_event("workflow.work_item.revalidation.failed", self._serialize_workflow_record(updated))

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

        edges = self._linked_contract_edges(contract.id)
        for edge in edges:
            metadata = dict(edge.metadata)
            if not metadata.get("revalidation_required"):
                continue
            metadata["revalidation_required"] = False
            metadata["validated_contract_version"] = contract.version
            self.update_dependency_edge(edge.id, {"metadata": metadata})

        work_items = self.list_work_items(limit=500)
        for work_item in work_items:
            marker = dict(work_item.metadata.get("revalidation", {}))
            if str(marker.get("contract_id") or "") != contract.id:
                continue
            required_version_raw = marker.get("required_version")
            required_version = int(required_version_raw) if str(required_version_raw or "").strip() else 0
            if required_version > 0 and contract.version < required_version:
                self._emit_workflow_event(
                    "workflow.work_item.revalidation.failed",
                    {
                        "work_item_id": work_item.id,
                        "contract_id": contract.id,
                        "required_version": required_version,
                        "contract_version": contract.version,
                        "reason": "contract_version_too_low",
                    },
                )
                continue
            marker["required"] = False
            marker["validated_version"] = contract.version
            next_blocked_by = [entry for entry in work_item.blocked_by if entry != contract.id]
            updates: dict[str, Any] = {
                "metadata": {**work_item.metadata, "revalidation": marker},
                "blocked_by": next_blocked_by,
            }
            if work_item.status == "blocked" and not next_blocked_by:
                updates["status"] = "ready"
                scheduler_meta = dict(work_item.metadata.get("scheduler", {}))
                scheduler_meta.setdefault("ready_since", self._now_iso())
                updates["metadata"] = {**work_item.metadata, "revalidation": marker, "scheduler": scheduler_meta}

            updated = self.update_work_item(work_item.id, updates)
            self._emit_workflow_event("workflow.work_item.revalidation.cleared", self._serialize_workflow_record(updated))

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
        """Auto-link contract lifecycle with dependency edges and work item blockers."""
        auto_edge = bool(fields.get("auto_create_dependency_edge", True))
        consumer_work_item_id = str(fields.get("consumer_work_item_id") or contract.work_item_id or "")
        if not consumer_work_item_id:
            candidate = self._find_latest_work_item_for_module(contract.consumer_module)
            consumer_work_item_id = candidate.id if candidate else ""

        # Always attach contract blocker to consumer work item for scheduler visibility.
        if consumer_work_item_id:
            consumer_item = self.get_work_item(consumer_work_item_id)
            if consumer_item:
                next_blocked_by = self._unique_list(list(consumer_item.blocked_by) + [contract.id])
                updates: dict[str, Any] = {
                    "blocked_by": next_blocked_by,
                }
                if contract.status not in {"accepted", "implemented", "completed"} and consumer_item.status not in {"blocked", "waiting_decision"}:
                    updates["status"] = "blocked"
                updated = self.update_work_item(consumer_work_item_id, updates)
                self._emit_workflow_event("workflow.work_item.updated", self._serialize_workflow_record(updated))

        if auto_edge:
            provider_work_item_id = str(fields.get("provider_work_item_id") or "")

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
                # 已经移除业务路径里的 depends_on 双写，单一来源于 dependency_edges
                self._emit_workflow_event("workflow.dependency_edge.upserted", self._serialize_workflow_record(edge))

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
        work_items = self.list_work_items(limit=limit)
        transitions: list[dict[str, Any]] = []

        for work_item in work_items:
            blockers: list[str] = []
            decisions = self.list_decisions(filters={"work_item_id": work_item.id}, limit=100)
            if any(decision.status in {"pending", "in_review"} for decision in decisions):
                blockers.append("pending_decision")

            edges = self.list_dependency_edges(filters={"source_work_item_id": work_item.id, "status": "active"}, limit=100)
            for edge in edges:
                target = self.get_work_item(edge.target_work_item_id)
                if target and target.status not in {"completed", "done", "cancelled"}:
                    blockers.append(f"dependency:{edge.id}")

            # Single source of truth: dependency_edges. WorkItem.depends_on is a scheduler-maintained projection.
            # 不一致才回写，避免无意义抖动写库
            # 依赖只写 edge，depends_on 由 scheduler 自动投影修复。
            desired_depends_on = sorted({edge.target_work_item_id for edge in edges})
            current_depends_on = sorted({str(item) for item in work_item.depends_on})
            if desired_depends_on != current_depends_on:
                projected = self.update_work_item(
                    work_item.id,
                    {
                        "depends_on": desired_depends_on,
                    },
                )
                work_item = projected
                self._emit_workflow_event("workflow.work_item.dependencies_projected", self._serialize_workflow_record(projected))

            unresolved_contracts = []
            for contract_id in work_item.blocked_by:
                contract = self.get_contract(contract_id)
                if contract and contract.status not in {"accepted", "implemented", "completed"}:
                    unresolved_contracts.append(contract_id)
            blockers.extend([f"contract:{item}" for item in unresolved_contracts])

            desired_status = work_item.status
            desired_decision_required = any(decision.status in {"pending", "in_review"} for decision in decisions)
            if desired_decision_required:
                desired_status = "waiting_decision"
            elif blockers:
                desired_status = "blocked"
            elif work_item.status in {"proposed", "blocked", "waiting_decision"}:
                desired_status = "ready"

            if desired_status != work_item.status or desired_decision_required != work_item.decision_required:
                scheduler_updates: dict[str, Any] = {"state": "reconciled"}
                if desired_status == "ready":
                    scheduler_updates["ready_since"] = work_item.metadata.get("scheduler", {}).get("ready_since", self._now_iso())
                updated = self.update_work_item(
                    work_item.id,
                    {
                        "status": desired_status,
                        "decision_required": desired_decision_required,
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
                "average_turnaround_seconds": round(self._average(decision_durations), 3),
                "max_turnaround_seconds": round(max(decision_durations), 3) if decision_durations else 0.0,
                "items": decision_items,
            },
        }

    async def _dispatch_claimed_work_item(self, work_item_id: str) -> None:
        """Run one ready work item through the matching module agent."""
        item = self.get_work_item(work_item_id)
        if item is None:
            return

        session_key = item.session_key or f"workflow::{item.id}"
        started_at = self._now_iso()
        queue_wait_seconds = self._seconds_between(item.metadata.get("scheduler", {}).get("ready_since"), started_at) or 0.0
        try:
            result = await self.delegate_project_task(
                project=item.module,
                task=item.goal,
                session_key=session_key,
                channel="workflow",
                chat_id=f"work_item:{item.id}",
            )
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
            depends_on=list(fields.get("depends_on", [])),
            blocked_by=list(fields.get("blocked_by", [])),
            artifacts=list(fields.get("artifacts", [])),
            metadata=metadata,
        )
        created = self.workflow_store.create_work_item(record)
        self._emit_workflow_event("workflow.work_item.created", self._serialize_workflow_record(created))
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
        consumer_work_item_id = str(fields.get("consumer_work_item_id", "")).strip() or work_item_id
        provider_work_item_id = str(fields.get("provider_work_item_id", "")).strip()
        incoming_metadata = dict(fields.get("metadata", {}))
        incoming_metadata.setdefault("merged_request_count", 1)
        incoming_metadata.setdefault("dedupe_mode", "request_merge")
        incoming_metadata.setdefault("merged_work_item_ids", [item for item in [work_item_id] if item])
        incoming_metadata.setdefault("merged_consumer_work_item_ids", [item for item in [consumer_work_item_id] if item])
        incoming_metadata.setdefault("merged_provider_work_item_ids", [item for item in [provider_work_item_id] if item])

        record = ContractRecord(
            id=str(fields.get("id") or uuid.uuid4())[:36],
            provider_module=str(fields["provider_module"]),
            consumer_module=str(fields["consumer_module"]),
            interface_name=str(fields["interface_name"]),
            version=int(fields.get("version", 1)),
            status=str(fields.get("status", "requested")),
            spec=dict(fields.get("spec", {})),
            stub_path=str(fields.get("stub_path", "")),
            implementation_path=str(fields.get("implementation_path", "")),
            work_item_id=work_item_id,
            metadata=incoming_metadata,
        )
        created = self.workflow_store.create_contract(record)
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
        updated = self.workflow_store.update_contract(record_id, **changes)
        if self._is_contract_terminal(updated.status):
            lifecycle = dict(updated.metadata.get("lifecycle", {}))
            if not lifecycle.get("resolved_at"):
                lifecycle["resolved_at"] = self._now_iso()
                updated = self.workflow_store.update_contract(record_id, metadata={**updated.metadata, "lifecycle": lifecycle})
        self._emit_workflow_event("workflow.contract.updated", self._serialize_workflow_record(updated))
        self._apply_contract_side_effects(updated, changes)
        self._apply_contract_version_invalidation(previous, updated, changes)
        return updated

    def delete_contract(self, record_id: str) -> bool:
        deleted = self.workflow_store.delete_contract(record_id)
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
        updated = self.workflow_store.update_dependency_edge(record_id, **changes)
        self._emit_workflow_event("workflow.dependency_edge.updated", self._serialize_workflow_record(updated))
        return updated

    def delete_dependency_edge(self, record_id: str) -> bool:
        deleted = self.workflow_store.delete_dependency_edge(record_id)
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

    def get_project_loop(self, project: str) -> AgentLoop:
        normalized, project_path = self._normalize_project(project)
        registration = self._project_registry[normalized]
        loop = self.project_loops.get(normalized)
        if loop is None:
            scope_lines = [f"Project scope: {normalized}"]
            if registration.owner:
                scope_lines.append(f"Owner: {registration.owner}")
            if registration.description:
                scope_lines.append(f"Description: {registration.description}")
            if registration.prompt_hint:
                scope_lines.append(f"Prompt hint: {registration.prompt_hint}")
            loop = AgentLoop(
                bus=self.bus,
                provider=self.provider,
                workspace=project_path,
                model=self.model,
                max_iterations=self.max_iterations,
                brave_api_key=self.brave_api_key,
                exec_config=self.exec_config,
                restrict_to_workspace=self.restrict_to_workspace,
                session_manager=self.sessions,
                agent_role="project",
                scope_hint="\n".join(scope_lines),
                enable_message_tool=False,
            )
            self.project_loops[normalized] = loop
        return loop

    async def delegate_project_task(
        self,
        project: str,
        task: str,
        session_key: str,
        channel: str,
        chat_id: str,
    ) -> str:
        loop = self.get_project_loop(project)
        normalized, _ = self._normalize_project(project)
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

        self._emit_workflow_event(
            "workflow.project_agent.delegation.started",
            {
                "project": normalized,
                "session_key": project_session,
                "channel": channel,
                "chat_id": chat_id,
                "has_runtime_attributes": bool(runtime_attributes),
                "task_preview": task[:200],
            },
        )

        try:
            result = await loop.process_direct(
                delegated_task,
                session_key=project_session,
                channel=channel,
                chat_id=chat_id,
            )
            self._emit_workflow_event(
                "workflow.project_agent.delegation.completed",
                {
                    "project": normalized,
                    "session_key": project_session,
                    "channel": channel,
                    "chat_id": chat_id,
                    "has_runtime_attributes": bool(runtime_attributes),
                    "result_preview": result[:300],
                },
            )
            return f"[Project Scope: {normalized}]\n{result}"
        except Exception as exc:
            self._emit_workflow_event(
                "workflow.project_agent.delegation.failed",
                {
                    "project": normalized,
                    "session_key": project_session,
                    "channel": channel,
                    "chat_id": chat_id,
                    "has_runtime_attributes": bool(runtime_attributes),
                    "error": str(exc),
                },
            )
            raise

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
            result = await self.delegate_project_task(
                project=project,
                task=task,
                session_key=batch_session,
                channel=channel,
                chat_id=chat_id,
            )
            return index, project, result

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
        await self._start_reconciler()
        try:
            await self.core_loop.run()
        finally:
            await self._stop_reconciler()

    def stop(self) -> None:
        """Stop the core loop."""
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
