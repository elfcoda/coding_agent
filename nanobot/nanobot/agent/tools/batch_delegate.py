"""Tool for delegating multiple project-scoped tasks concurrently."""

from typing import Any, TYPE_CHECKING

from nanobot.agent.tools.base import Tool

if TYPE_CHECKING:
    from nanobot.agent.core_manager import CoreAgentManager


class DelegateProjectsBatchTool(Tool):
    """Start multiple project-agent tasks concurrently and return a handle."""

    def __init__(self, manager: "CoreAgentManager"):
        self._manager = manager
        self._channel = "cli"
        self._chat_id = "direct"
        self._session_key = "cli:direct"

    def set_context(self, channel: str, chat_id: str, session_key: str | None = None) -> None:
        """Store the current routing/session context for delegated batch work."""
        self._channel = channel
        self._chat_id = chat_id
        if session_key:
            self._session_key = session_key

    @property
    def name(self) -> str:
        return "delegate_projects_batch"

    @property
    def description(self) -> str:
        return (
            "Start multiple focused tasks on project agents concurrently and return a batch handle immediately. "
            "Use get_batch_delegation_status to inspect the handle later."
        )

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "description": "List of project-scoped tasks to delegate in parallel",
                    "items": {
                        "type": "object",
                        "properties": {
                            "project": {
                                "type": "string",
                                "description": "Allowed project scope to delegate to",
                            },
                            "task": {
                                "type": "string",
                                "description": "Focused task for that project agent",
                            },
                        },
                        "required": ["project", "task"],
                    },
                },
            },
            "required": ["items"],
        }

    async def execute(self, items: list[dict[str, str]], **kwargs: Any) -> str:
        """Start the batch of tasks and return the asynchronous handle."""
        return await self._manager.delegate_projects_batch(
            items=items,
            session_key=self._session_key,
            channel=self._channel,
            chat_id=self._chat_id,
        )
