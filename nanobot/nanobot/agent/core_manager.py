"""Thin coordinator over a core agent and project-scoped agent loops."""

from __future__ import annotations

import asyncio
import uuid
import json
import re
from dataclasses import asdict, dataclass
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
                self._emit_workflow_event("workflow.dependency_edge.upserted", self._serialize_workflow_record(edge))

                consumer_item = self.get_work_item(consumer_work_item_id)
                if consumer_item:
                    next_depends_on = self._unique_list(list(consumer_item.depends_on) + [provider_work_item_id])
                    updates: dict[str, Any] = {
                        "depends_on": next_depends_on,
                    }
                    if contract.status not in {"accepted", "implemented", "completed"} and consumer_item.status not in {"blocked", "waiting_decision"}:
                        updates["status"] = "blocked"
                    updated = self.update_work_item(consumer_work_item_id, updates)
                    self._emit_workflow_event("workflow.work_item.updated", self._serialize_workflow_record(updated))

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
                updated = self.update_work_item(
                    work_item.id,
                    {
                        "status": desired_status,
                        "decision_required": desired_decision_required,
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

    def _serialize_workflow_record(self, record: Any) -> dict[str, Any]:
        return asdict(record)

    def create_work_item(self, fields: dict[str, Any]) -> WorkItemRecord:
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
            metadata=dict(fields.get("metadata", {})),
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
            work_item_id=str(fields.get("work_item_id", "")),
            metadata=dict(fields.get("metadata", {})),
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

    def update_contract(self, record_id: str, changes: dict[str, Any]) -> ContractRecord:
        updated = self.workflow_store.update_contract(record_id, **changes)
        self._emit_workflow_event("workflow.contract.updated", self._serialize_workflow_record(updated))
        self._apply_contract_side_effects(updated, changes)
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
        result = await loop.process_direct(
            task,
            session_key=project_session,
            channel=channel,
            chat_id=chat_id,
        )
        return f"[Project Scope: {normalized}]\n{result}"

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
        await self.core_loop.run()

    def stop(self) -> None:
        """Stop the core loop."""
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
