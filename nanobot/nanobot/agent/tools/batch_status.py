"""Tool for querying asynchronous batch delegation handles."""

from typing import Any, TYPE_CHECKING

from nanobot.agent.tools.base import Tool

if TYPE_CHECKING:
    from nanobot.agent.core_manager import CoreAgentManager


class GetBatchDelegationStatusTool(Tool):
    """Inspect the status of a background project-batch delegation."""

    def __init__(self, manager: "CoreAgentManager"):
        self._manager = manager

    @property
    def name(self) -> str:
        return "get_batch_delegation_status"

    @property
    def description(self) -> str:
        return "Inspect the current status or final result of a delegate_projects_batch handle."

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "batch_id": {
                    "type": "string",
                    "description": "Batch delegation handle returned by delegate_projects_batch",
                },
            },
            "required": ["batch_id"],
        }

    async def execute(self, batch_id: str, **kwargs: Any) -> str:
        return self._manager.get_batch_delegation_status(batch_id)
