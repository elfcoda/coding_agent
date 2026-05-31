"""Tool for managing orchestration workflow state."""

from typing import Any, TYPE_CHECKING

from nanobot.agent.tools.base import Tool

if TYPE_CHECKING:
    from nanobot.agent.core_manager import CoreAgentManager


class ManageWorkflowStateTool(Tool):
    """Create, inspect, list, update, and delete workflow state records."""

    def __init__(
        self,
        manager: "CoreAgentManager",
        *,
        allowed_entities: list[str] | None = None,
        allowed_actions: list[str] | None = None,
        actor_role: str = "core",
    ):
        self._manager = manager
        self._allowed_entities = allowed_entities or ["work_item", "contract", "dependency_edge", "decision", "scheduler"]
        self._allowed_actions = allowed_actions or ["create", "get", "list", "update", "delete", "tick"]
        self._actor_role = actor_role

    @property
    def name(self) -> str:
        return "manage_workflow"

    @property
    def description(self) -> str:
        return (
            "Manage orchestration workflow records for work items, contracts, dependency edges, and decisions. "
            "Use this to persist and inspect structured workflow state instead of relying on free-form text. "
            "All requests are executed through the core manager."
        )

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "entity": {
                    "type": "string",
                    "enum": list(self._allowed_entities),
                    "description": "Workflow entity type to manage",
                },
                "action": {
                    "type": "string",
                    "enum": list(self._allowed_actions),
                    "description": "Operation to perform on the workflow entity",
                },
                "record_id": {
                    "type": "string",
                    "description": "Record id for get, update, or delete",
                },
                "fields": {
                    "type": "object",
                    "description": "Fields for create or update actions",
                },
                "filters": {
                    "type": "object",
                    "description": "Optional filters for list actions",
                },
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 500,
                    "description": "Maximum number of records to return for list actions",
                },
            },
            "required": ["entity", "action"],
        }

    async def execute(
        self,
        entity: str,
        action: str,
        record_id: str | None = None,
        fields: dict[str, Any] | None = None,
        filters: dict[str, Any] | None = None,
        limit: int = 100,
        **kwargs: Any,
    ) -> str:
        if entity not in self._allowed_entities:
            raise ValueError(f"Role '{self._actor_role}' cannot access workflow entity: {entity}")
        if action not in self._allowed_actions:
            raise ValueError(f"Role '{self._actor_role}' cannot perform workflow action: {action}")

        return self._manager.manage_workflow_state(
            entity=entity,
            action=action,
            record_id=record_id,
            fields=fields,
            filters=filters,
            limit=limit,
        )
