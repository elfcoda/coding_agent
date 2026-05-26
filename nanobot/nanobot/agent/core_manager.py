"""Thin coordinator over a core agent and project-scoped agent loops."""

from __future__ import annotations

import asyncio
import uuid
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, TYPE_CHECKING

from nanobot.agent.loop import AgentLoop
from nanobot.agent.tools.batch_delegate import DelegateProjectsBatchTool
from nanobot.agent.tools.batch_status import GetBatchDelegationStatusTool
from nanobot.agent.tools.delegate import DelegateProjectTaskTool
from nanobot.agent.tools.list_project_scopes import ListProjectScopesTool
from nanobot.agent.tools.workflow_state import ManageWorkflowStateTool
from nanobot.bus.events import InboundMessage
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
        self.core_loop.tools.register(ManageWorkflowStateTool(self))

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
        return self.workflow_store.create_work_item(record)

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
        return self.workflow_store.update_work_item(record_id, **changes)

    def delete_work_item(self, record_id: str) -> bool:
        return self.workflow_store.delete_work_item(record_id)

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
        return self.workflow_store.create_contract(record)

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
        return self.workflow_store.update_contract(record_id, **changes)

    def delete_contract(self, record_id: str) -> bool:
        return self.workflow_store.delete_contract(record_id)

    def create_dependency_edge(self, fields: dict[str, Any]) -> DependencyEdgeRecord:
        record = DependencyEdgeRecord(
            id=str(fields.get("id") or uuid.uuid4())[:36],
            source_work_item_id=str(fields["source_work_item_id"]),
            target_work_item_id=str(fields["target_work_item_id"]),
            edge_type=str(fields.get("edge_type", "depends_on")),
            status=str(fields.get("status", "active")),
            metadata=dict(fields.get("metadata", {})),
        )
        return self.workflow_store.create_dependency_edge(record)

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
        return self.workflow_store.update_dependency_edge(record_id, **changes)

    def delete_dependency_edge(self, record_id: str) -> bool:
        return self.workflow_store.delete_dependency_edge(record_id)

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
        return self.workflow_store.create_decision(record)

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
        return self.workflow_store.update_decision(record_id, **changes)

    def delete_decision(self, record_id: str) -> bool:
        return self.workflow_store.delete_decision(record_id)

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
