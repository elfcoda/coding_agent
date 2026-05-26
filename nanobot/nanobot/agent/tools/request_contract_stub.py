"""Tool for cross-module contract-request + stub-only orchestration."""

from typing import Any, TYPE_CHECKING

from nanobot.agent.tools.base import Tool

if TYPE_CHECKING:
    from nanobot.agent.core_manager import CoreAgentManager


class RequestContractStubTool(Tool):
    """Create a contract request and caller-side stub without requiring provider implementation."""

    def __init__(self, manager: "CoreAgentManager"):
        self._manager = manager

    @property
    def name(self) -> str:
        return "request_contract_stub"

    @property
    def description(self) -> str:
        return (
            "For cross-module calls, create a contract request and generate a consumer-side stub only. "
            "Do not ask provider module to fully implement the business logic in this step."
        )

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "consumer_project": {
                    "type": "string",
                    "description": "Allowed consumer project scope where the stub should be generated",
                },
                "provider_project": {
                    "type": "string",
                    "description": "Allowed provider project scope that receives the contract request",
                },
                "interface_name": {
                    "type": "string",
                    "description": "Interface/contract name used between modules",
                },
                "contract_spec": {
                    "type": "object",
                    "description": "Structured contract request details",
                },
                "stub_relative_path": {
                    "type": "string",
                    "description": "Optional relative path inside consumer project for generated stub file",
                },
                "stub_content": {
                    "type": "string",
                    "description": "Optional full stub content. If omitted, a default stub template is generated",
                },
                "consumer_work_item_id": {
                    "type": "string",
                    "description": "Optional consumer work item id to link to the contract",
                },
                "provider_work_item_id": {
                    "type": "string",
                    "description": "Optional provider work item id for dependency edge linking",
                },
            },
            "required": ["consumer_project", "provider_project", "interface_name"],
        }

    async def execute(
        self,
        consumer_project: str,
        provider_project: str,
        interface_name: str,
        contract_spec: dict[str, Any] | None = None,
        stub_relative_path: str | None = None,
        stub_content: str | None = None,
        consumer_work_item_id: str | None = None,
        provider_work_item_id: str | None = None,
        **kwargs: Any,
    ) -> str:
        return self._manager.request_contract_stub(
            consumer_project=consumer_project,
            provider_project=provider_project,
            interface_name=interface_name,
            contract_spec=contract_spec,
            stub_relative_path=stub_relative_path,
            stub_content=stub_content,
            consumer_work_item_id=consumer_work_item_id,
            provider_work_item_id=provider_work_item_id,
        )
