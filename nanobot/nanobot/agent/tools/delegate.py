"""Tool for delegating focused tasks to project-scoped agents."""

from typing import Any, TYPE_CHECKING

from nanobot.agent.tools.base import Tool

if TYPE_CHECKING:
    from nanobot.agent.core_manager import CoreAgentManager


class DelegateProjectTaskTool(Tool):
    """Delegate a task to a project-scoped agent rooted at a subdirectory."""

    def __init__(self, manager: "CoreAgentManager"):
        self._manager = manager
        self._channel = "cli"
        self._chat_id = "direct"
        self._session_key = "cli:direct"

    def set_context(self, channel: str, chat_id: str, session_key: str | None = None) -> None:
        """Store the current routing/session context for delegated work."""
        self._channel = channel
        self._chat_id = chat_id
        if session_key:
            self._session_key = session_key

    @property
    def name(self) -> str:
        return "delegate_project_task"

    @property
    def description(self) -> str:
        return (
            "Delegate a focused task to a project agent rooted at a specific subdirectory. "
            "Use this when the work is mostly contained within one project or folder."
        )

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "project": {
                    "type": "string",
                    "description": "Relative subdirectory path that defines the project scope, for example 'fastcode' or 'nanobot/nanobot'",
                },
                "task": {
                    "type": "string",
                    "description": "Focused task for the project agent to complete",
                },
            },
            "required": ["project", "task"],
        }

    async def execute(self, project: str, task: str, **kwargs: Any) -> str:
        """Delegate the task and return the project agent's result."""
        return await self._manager.delegate_project_task(
            project=project,
            task=task,
            session_key=self._session_key,
            channel=self._channel,
            chat_id=self._chat_id,
        )
