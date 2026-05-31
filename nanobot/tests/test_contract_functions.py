from __future__ import annotations

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
        allowed_project_scopes=["fastcode", "nanobot"],
        workflow_store=store,
        decision_sla_seconds=3600,
        decision_sla_block_scope="module",
        decision_queue_impact_weight=10,
        decision_queue_age_weight=1,
        decision_default_degradation="wait",
    )


def test_create_contract_persists_normalized_functions(tmp_path: Path) -> None:
    (tmp_path / "fastcode").mkdir(parents=True, exist_ok=True)
    (tmp_path / "nanobot").mkdir(parents=True, exist_ok=True)
    manager = _make_manager(tmp_path)

    provider = manager.create_work_item({"module": "fastcode", "goal": "implement contract", "status": "in_progress"})
    consumer = manager.create_work_item({"module": "nanobot", "goal": "consume contract", "status": "proposed"})

    created = manager.create_contract(
        {
            "provider_module": "fastcode",
            "consumer_module": "nanobot",
            "interface_name": "SharedSearch",
            "status": "requested",
            "provider_work_item_id": provider.id,
            "consumer_work_item_id": consumer.id,
            "functions": [
                {
                    "name": "search",
                    "sig": "search(query: str) -> list[str]",
                    "desc": "Search shared index",
                    "impl_status": "draft",
                }
            ],
        }
    )

    fetched = manager.get_contract(created.id)

    assert fetched is not None
    assert fetched.functions == [
        {
            "name": "search",
            "sig": "search(query: str) -> list[str]",
            "desc": "Search shared index",
            "impl_status": "draft",
            "impl_latest_work_item_id": provider.id,
            "consumer_modules": ["nanobot"],
        }
    ]


def test_update_contract_persists_function_impl_progress(tmp_path: Path) -> None:
    (tmp_path / "fastcode").mkdir(parents=True, exist_ok=True)
    (tmp_path / "nanobot").mkdir(parents=True, exist_ok=True)
    manager = _make_manager(tmp_path)

    provider = manager.create_work_item({"module": "fastcode", "goal": "implement contract", "status": "in_progress"})
    consumer = manager.create_work_item({"module": "nanobot", "goal": "consume contract", "status": "proposed"})
    contract = manager.create_contract(
        {
            "provider_module": "fastcode",
            "consumer_module": "nanobot",
            "interface_name": "SharedSearch",
            "status": "requested",
            "provider_work_item_id": provider.id,
            "consumer_work_item_id": consumer.id,
            "functions": [{"name": "search", "sig": "search(query: str)", "desc": "Search", "impl_status": "draft"}],
        }
    )
    latest_provider = manager.create_work_item({"module": "fastcode", "goal": "implement search v2", "status": "in_progress"})

    updated = manager.update_contract(
        contract.id,
        {
            "functions": [
                {
                    "name": "search",
                    "sig": "search(query: str) -> list[str]",
                    "desc": "Search shared index",
                    "impl_status": "implemented",
                    "impl_latest_work_item_id": latest_provider.id,
                    "consumer_modules": ["nanobot", "reporting"],
                }
            ]
        },
    )

    assert updated.functions[0]["impl_status"] == "implemented"
    assert updated.functions[0]["impl_latest_work_item_id"] == latest_provider.id
    assert updated.functions[0]["consumer_modules"] == ["nanobot", "reporting"]


def test_duplicate_contract_request_merges_function_consumers_and_latest_impl(tmp_path: Path) -> None:
    (tmp_path / "fastcode").mkdir(parents=True, exist_ok=True)
    (tmp_path / "nanobot").mkdir(parents=True, exist_ok=True)
    manager = _make_manager(tmp_path)

    provider_v1 = manager.create_work_item({"module": "fastcode", "goal": "implement contract v1", "status": "in_progress"})
    consumer = manager.create_work_item({"module": "nanobot", "goal": "consume contract", "status": "proposed"})
    created = manager.create_contract(
        {
            "provider_module": "fastcode",
            "consumer_module": "nanobot",
            "interface_name": "SharedSearch",
            "status": "requested",
            "provider_work_item_id": provider_v1.id,
            "consumer_work_item_id": consumer.id,
            "functions": [
                {
                    "name": "search",
                    "sig": "search(query: str) -> list[str]",
                    "desc": "Search shared index",
                    "impl_status": "draft",
                    "consumer_modules": ["nanobot"],
                }
            ],
        }
    )
    provider_v2 = manager.create_work_item({"module": "fastcode", "goal": "implement contract v2", "status": "in_progress"})

    merged = manager.create_contract(
        {
            "provider_module": "fastcode",
            "consumer_module": "nanobot",
            "interface_name": "SharedSearch",
            "status": "requested",
            "provider_work_item_id": provider_v2.id,
            "consumer_work_item_id": consumer.id,
            "functions": [
                {
                    "name": "search",
                    "sig": "search(query: str) -> list[str]",
                    "desc": "Search shared index v2",
                    "impl_status": "in_progress",
                    "consumer_modules": ["nanobot", "reporting"],
                }
            ],
        }
    )

    assert merged.id == created.id
    assert merged.functions == [
        {
            "name": "search",
            "sig": "search(query: str) -> list[str]",
            "desc": "Search shared index v2",
            "impl_status": "in_progress",
            "impl_latest_work_item_id": provider_v2.id,
            "consumer_modules": ["nanobot", "reporting"],
        }
    ]
