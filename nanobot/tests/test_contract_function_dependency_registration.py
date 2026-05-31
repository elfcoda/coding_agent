from __future__ import annotations

import json
import uuid
from pathlib import Path

from nanobot.agent.core_manager import CoreAgentManager
from nanobot.bus.queue import MessageBus
from nanobot.providers.base import LLMProvider, LLMResponse
from nanobot.workflow import WorkflowStore


class DummyProvider(LLMProvider):
    async def chat(self, messages, tools=None, model=None, max_tokens=4096, temperature=1.0):
        return LLMResponse(content="ok")

    def get_default_model(self):
        return "dummy/model"


def _make_manager(tmp_path: Path) -> CoreAgentManager:
    db = tmp_path / f"workflow_test_{uuid.uuid4().hex}.db"
    store = WorkflowStore(db)
    return CoreAgentManager(
        bus=MessageBus(),
        provider=DummyProvider(),
        workspace=tmp_path,
        allowed_project_scopes=["fastcode", "nanobot", "reporting"],
        workflow_store=store,
        decision_sla_seconds=3600,
        decision_sla_block_scope="module",
        decision_queue_impact_weight=10,
        decision_queue_age_weight=1,
        decision_default_degradation="wait",
    )


def test_register_contract_function_dependency_updates_consumer_modules_and_edge(tmp_path: Path) -> None:
    (tmp_path / "fastcode").mkdir(parents=True, exist_ok=True)
    (tmp_path / "nanobot").mkdir(parents=True, exist_ok=True)
    (tmp_path / "reporting").mkdir(parents=True, exist_ok=True)
    manager = _make_manager(tmp_path)

    provider = manager.create_work_item({"module": "fastcode", "goal": "implement shared interface", "status": "in_progress"})
    consumer = manager.create_work_item({"module": "reporting", "goal": "consume shared summarize", "status": "proposed"})
    contract = manager.create_contract(
        {
            "provider_module": "fastcode",
            "consumer_module": "nanobot",
            "interface_name": "SharedSearch",
            "status": "requested",
            "provider_work_item_id": provider.id,
            "functions": [
                {
                    "name": "summarize",
                    "sig": "summarize(results: list[str]) -> str",
                    "desc": "Summarize results",
                    "impl_status": "draft",
                    "consumer_modules": ["nanobot"],
                }
            ],
        }
    )

    updated = manager.register_contract_function_dependency(
        contract_id=contract.id,
        function_name="summarize",
        dependent_work_item_id=consumer.id,
    )

    assert updated.functions == [
        {
            "name": "summarize",
            "sig": "summarize(results: list[str]) -> str",
            "desc": "Summarize results",
            "impl_status": "draft",
            "impl_latest_work_item_id": provider.id,
            "consumer_modules": ["nanobot", "reporting"],
        }
    ]

    edges = manager.list_dependency_edges(
        filters={
            "source_work_item_id": consumer.id,
            "target_work_item_id": provider.id,
            "edge_type": "requires_contract_function",
            "status": "active",
        },
        limit=10,
    )

    assert len(edges) == 1
    assert edges[0].metadata["contract_id"] == contract.id
    assert edges[0].metadata["contract_function_names"] == ["summarize"]
    assert edges[0].metadata["interface_name"] == "SharedSearch"


async def test_project_loop_tool_can_register_contract_function_dependency(tmp_path: Path) -> None:
    (tmp_path / "fastcode").mkdir(parents=True, exist_ok=True)
    (tmp_path / "nanobot").mkdir(parents=True, exist_ok=True)
    manager = _make_manager(tmp_path)

    provider = manager.create_work_item({"module": "fastcode", "goal": "implement shared interface", "status": "in_progress"})
    consumer = manager.create_work_item({"module": "nanobot", "goal": "consume shared search", "status": "proposed"})
    contract = manager.create_contract(
        {
            "provider_module": "fastcode",
            "consumer_module": "nanobot",
            "interface_name": "SharedSearch",
            "status": "requested",
            "provider_work_item_id": provider.id,
            "functions": [
                {
                    "name": "search",
                    "sig": "search(query: str) -> list[str]",
                    "desc": "Search the shared index",
                    "impl_status": "draft",
                }
            ],
        }
    )

    assert manager.core_loop.tools.has("register_contract_function_dependency")

    project_loop = manager.get_project_loop("nanobot")
    assert project_loop.tools.has("register_contract_function_dependency")

    result = await project_loop.tools.execute(
        "register_contract_function_dependency",
        {
            "contract_id": contract.id,
            "function_name": "search",
            "dependent_work_item_id": consumer.id,
        },
    )
    payload = json.loads(result)

    assert payload["functions"][0]["consumer_modules"] == ["nanobot"]
    assert payload["functions"][0]["impl_latest_work_item_id"] == provider.id

    tools = manager.core_loop.subagents._build_tool_registry()
    assert not tools.has("register_contract_function_dependency")
