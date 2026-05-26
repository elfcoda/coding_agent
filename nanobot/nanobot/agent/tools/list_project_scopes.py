"""Tool for discovering delegable project scopes for the core agent."""

from typing import Any, TYPE_CHECKING

from nanobot.agent.tools.base import Tool

if TYPE_CHECKING:
    from nanobot.agent.core_manager import CoreAgentManager


class ListProjectScopesTool(Tool):
    """List project scopes that the core agent can delegate work to."""

    def __init__(self, manager: "CoreAgentManager"):
        self._manager = manager

    @property
    def name(self) -> str:
        return "list_project_scopes"

    @property
    def description(self) -> str:
        return (
            "List candidate project scopes under the workspace that can be used with delegate_project_task. "
            "Use this before delegation when you need to discover which subdirectories look like independent work areas."
        )

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "max_depth": {
                    "type": "integer",
                    "description": "Maximum directory depth to explore from the workspace root",
                    "minimum": 1,
                    "maximum": 4,
                },
                "include_files": {
                    "type": "boolean",
                    "description": "Whether to include top-level files alongside directories",
                },
            },
        }

    async def execute(self, max_depth: int = 2, include_files: bool = False, **kwargs: Any) -> str:
        """Return a newline-formatted list of delegable scopes."""
        registry_entries = self._manager.get_project_registry()
        if registry_entries:
            lines = ["Delegable project scopes:"]
            for entry in registry_entries:
                details: list[str] = []
                if entry.owner:
                    details.append(f"owner={entry.owner}")
                if entry.description:
                    details.append(entry.description)
                if entry.tags:
                    details.append(f"tags={', '.join(entry.tags)}")

                suffix = f" ({'; '.join(details)})" if details else ""
                lines.append(f"- {entry.path}{suffix}")
            return "\n".join(lines)

        scopes = self._manager.list_project_scopes(max_depth=max_depth, include_files=include_files)
        if not scopes:
            return "No delegable project scopes found under the current workspace."

        lines = ["Delegable project scopes:"]
        for scope in scopes:
            lines.append(f"- {scope}")
        return "\n".join(lines)
