"""Request-user-decision tool shared by project workers and subagents."""

from __future__ import annotations

from typing import Any, Callable, Awaitable

from nanobot.agent.tools.base import Tool


class RequestUserDecisionTool(Tool):
    """
    Ask the user for a decision when the agent is uncertain and wait for the reply.

    Works for both the project agent loop and spawned subagents that share the
    same decision bridge.
    """

    def __init__(
        self,
        decision_callback: Callable[[str, list[str] | None], Awaitable[str]],
    ):
        self._decision_callback = decision_callback

    @property
    def name(self) -> str:
        return "request_user_decision"

    @property
    def description(self) -> str:
        return (
            "Ask the user for a decision when the agent is uncertain "
            "and wait for the reply before continuing."
        )

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "prompt": {
                    "type": "string",
                    "description": "Question to send to the user",
                },
                "options": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional list of candidate answers",
                },
            },
            "required": ["prompt"],
        }

    async def execute(self, prompt: str, options: list[str] | None = None, **kwargs: Any) -> str:
        return await self._decision_callback(prompt=prompt, options=options)
