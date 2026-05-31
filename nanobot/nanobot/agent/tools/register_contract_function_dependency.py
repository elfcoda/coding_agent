"""Tool for registering one work item's dependency on an unfinished contract function."""

from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any, TYPE_CHECKING

from nanobot.agent.tools.base import Tool

if TYPE_CHECKING:
    from nanobot.agent.core_manager import CoreAgentManager


class RegisterContractFunctionDependencyTool(Tool):
    """Register function-level contract consumption and dependency edges."""

    def __init__(self, manager: "CoreAgentManager"):
        self._manager = manager

    @property
    def name(self) -> str:
        return "register_contract_function_dependency"

    @property
    def description(self) -> str:
        return (
            "Record that a work item depends on a specific contract function. "
            "This updates the function's consumer_modules and creates a dependency to impl_latest_work_item_id when the function is unfinished."
        )

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "contract_id": {
                    "type": "string",
                    "description": "Contract id that owns the target function",
                },
                "function_name": {
                    "type": "string",
                    "description": "Function name (or normalized function key) inside the contract",
                },
                "dependent_work_item_id": {
                    "type": "string",
                    "description": "Work item id that depends on the target function",
                },
            },
            "required": ["contract_id", "function_name", "dependent_work_item_id"],
        }

    async def execute(
        self,
        contract_id: str,
        function_name: str,
        dependent_work_item_id: str,
        **kwargs: Any,
    ) -> str:
        return json.dumps(
            asdict(
                self._manager.register_contract_function_dependency(
                    contract_id=contract_id,
                    function_name=function_name,
                    dependent_work_item_id=dependent_work_item_id,
                )
            ),
            ensure_ascii=False,
            indent=2,
        )
