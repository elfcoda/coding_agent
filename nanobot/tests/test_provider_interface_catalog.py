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


def test_provider_interface_catalog_aggregates_interfaces_for_llm(tmp_path: Path) -> None:
    (tmp_path / "fastcode").mkdir(parents=True, exist_ok=True)
    (tmp_path / "nanobot").mkdir(parents=True, exist_ok=True)
    (tmp_path / "reporting").mkdir(parents=True, exist_ok=True)
    manager = _make_manager(tmp_path)

    provider = manager.create_work_item({"module": "fastcode", "goal": "implement shared interfaces", "status": "in_progress"})
    consumer_a = manager.create_work_item({"module": "nanobot", "goal": "consume shared search", "status": "proposed"})
    consumer_b = manager.create_work_item({"module": "reporting", "goal": "consume shared search", "status": "proposed"})

    manager.create_contract(
        {
            "provider_module": "fastcode",
            "consumer_module": "nanobot",
            "interface_name": "SharedSearch",
            "status": "implemented",
            "provider_work_item_id": provider.id,
            "consumer_work_item_id": consumer_a.id,
            "functions": [
                {
                    "name": "search",
                    "sig": "search(query: str) -> list[str]",
                    "desc": "Search the shared index",
                    "impl_status": "implemented",
                }
            ],
        }
    )
    manager.create_contract(
        {
            "provider_module": "fastcode",
            "consumer_module": "reporting",
            "interface_name": "SharedSearch",
            "status": "requested",
            "provider_work_item_id": provider.id,
            "consumer_work_item_id": consumer_b.id,
            "functions": [
                {
                    "name": "search",
                    "sig": "search(query: str) -> list[str]",
                    "desc": "Search the shared index",
                    "impl_status": "implemented",
                },
                {
                    "name": "summarize",
                    "sig": "summarize(results: list[str]) -> str",
                    "desc": "Summarize search results",
                    "impl_status": "draft",
                },
            ],
        }
    )

    catalog = manager.describe_provider_interfaces("fastcode")

    assert catalog["provider_module"] == "fastcode"
    assert catalog["interface_count"] == 1
    assert catalog["interfaces"][0]["interface_name"] == "SharedSearch"
    assert catalog["interfaces"][0]["consumer_modules"] == ["nanobot", "reporting"]
    assert catalog["interfaces"][0]["function_count"] == 2
    assert catalog["interfaces"][0]["functions"] == [
        {
            "name": "search",
            "sig": "search(query: str) -> list[str]",
            "desc": "Search the shared index",
            "impl_status": "implemented",
            "impl_latest_work_item_id": provider.id,
            "consumer_modules": ["nanobot", "reporting"],
        },
        {
            "name": "summarize",
            "sig": "summarize(results: list[str]) -> str",
            "desc": "Summarize search results",
            "impl_status": "draft",
            "impl_latest_work_item_id": provider.id,
            "consumer_modules": ["reporting"],
        },
    ]
    assert "SharedSearch" in catalog["llm_prompt"]
    assert "search(query: str) -> list[str]" in catalog["llm_prompt"]
    assert "If none fit, request a contract change or a new contract" in catalog["llm_prompt"]


async def test_provider_interface_catalog_tool_is_available_to_project_loops(tmp_path: Path) -> None:
    (tmp_path / "fastcode").mkdir(parents=True, exist_ok=True)
    (tmp_path / "nanobot").mkdir(parents=True, exist_ok=True)
    manager = _make_manager(tmp_path)

    provider = manager.create_work_item({"module": "fastcode", "goal": "implement shared interfaces", "status": "in_progress"})
    consumer = manager.create_work_item({"module": "nanobot", "goal": "consume shared search", "status": "proposed"})
    manager.create_contract(
        {
            "provider_module": "fastcode",
            "consumer_module": "nanobot",
            "interface_name": "SharedSearch",
            "status": "implemented",
            "provider_work_item_id": provider.id,
            "consumer_work_item_id": consumer.id,
            "functions": [
                {
                    "name": "search",
                    "sig": "search(query: str) -> list[str]",
                    "desc": "Search the shared index",
                    "impl_status": "implemented",
                }
            ],
        }
    )

    assert manager.core_loop.tools.has("describe_provider_interfaces")

    project_loop = manager.get_project_loop("nanobot")
    assert project_loop.tools.has("describe_provider_interfaces")

    result = await project_loop.tools.execute(
        "describe_provider_interfaces",
        {
            "provider_module": "fastcode",
        },
    )
    payload = json.loads(result)

    assert payload["provider_module"] == "fastcode"
    assert payload["interfaces"][0]["interface_name"] == "SharedSearch"
    assert "search(query: str) -> list[str]" in payload["llm_prompt"]

    tools = manager.core_loop.subagents._build_tool_registry()
    assert not tools.has("describe_provider_interfaces")
