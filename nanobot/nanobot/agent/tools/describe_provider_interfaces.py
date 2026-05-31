"""Tool for summarizing one provider module's contract interfaces for LLM planning."""

from __future__ import annotations

import json
from typing import Any, TYPE_CHECKING

from nanobot.agent.tools.base import Tool

if TYPE_CHECKING:
    from nanobot.agent.core_manager import CoreAgentManager


class DescribeProviderInterfacesTool(Tool):
    """Return a provider module's known interfaces and function signatures."""

    def __init__(self, manager: "CoreAgentManager"):
        self._manager = manager

    @property
    def name(self) -> str:
        return "describe_provider_interfaces"

    @property
    def description(self) -> str:
        return (
            "Summarize a provider module's known contracts, interfaces, and function signatures. "
            "Use this before requesting a new contract so you can decide whether to reuse an existing interface."
        )

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "provider_module": {
                    "type": "string",
                    "description": "Provider module id whose interface catalog should be summarized",
                },
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 1000,
                    "description": "Maximum number of contract records to scan for this provider",
                },
            },
            "required": ["provider_module"],
        }

    async def execute(self, provider_module: str, limit: int = 500, **kwargs: Any) -> str:
        return json.dumps(
            self._manager.describe_provider_interfaces(provider_module, limit=limit),
            ensure_ascii=False,
            indent=2,
        )
