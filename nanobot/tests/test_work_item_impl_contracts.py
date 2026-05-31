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


def test_create_contract_links_impl_on_contracts_to_provider_work_item(tmp_path: Path) -> None:
    (tmp_path / "fastcode").mkdir(parents=True, exist_ok=True)
    (tmp_path / "nanobot").mkdir(parents=True, exist_ok=True)
    manager = _make_manager(tmp_path)

    provider = manager.create_work_item({"module": "fastcode", "goal": "implement shared contract", "status": "blocked"})
    consumer = manager.create_work_item({"module": "nanobot", "goal": "consume shared contract", "status": "proposed"})

    contract = manager.create_contract(
        {
            "provider_module": "fastcode",
            "consumer_module": "nanobot",
            "interface_name": "SharedSearch",
            "status": "requested",
            "consumer_work_item_id": consumer.id,
            "provider_work_item_id": provider.id,
        }
    )

    provider_after = manager.get_work_item(provider.id)

    assert provider_after is not None
    assert provider_after.impl_on_contracts == [contract.id]


async def test_dispatch_completion_reblocks_work_item_until_impl_contracts_finish(tmp_path: Path) -> None:
    (tmp_path / "fastcode").mkdir(parents=True, exist_ok=True)
    (tmp_path / "nanobot").mkdir(parents=True, exist_ok=True)
    manager = _make_manager(tmp_path)

    provider = manager.create_work_item({"module": "fastcode", "goal": "implement shared contract", "status": "blocked"})
    consumer = manager.create_work_item({"module": "nanobot", "goal": "consume shared contract", "status": "proposed"})
    contract = manager.create_contract(
        {
            "provider_module": "fastcode",
            "consumer_module": "nanobot",
            "interface_name": "SharedSearch",
            "status": "requested",
            "consumer_work_item_id": consumer.id,
            "provider_work_item_id": provider.id,
        }
    )

    async def _delegate_ok(**kwargs):
        return "done"

    manager.delegate_project_task = _delegate_ok  # type: ignore[method-assign]

    manager.update_work_item(provider.id, {"status": "running"})

    await manager._dispatch_claimed_work_item(provider.id)

    provider_after = manager.get_work_item(provider.id)

    assert provider_after is not None
    assert provider_after.impl_on_contracts == [contract.id]
    assert provider_after.status == "blocked"


def test_contract_resolution_unblocks_provider_and_inactivates_downstream_edge(tmp_path: Path) -> None:
    (tmp_path / "fastcode").mkdir(parents=True, exist_ok=True)
    (tmp_path / "nanobot").mkdir(parents=True, exist_ok=True)
    manager = _make_manager(tmp_path)

    provider = manager.create_work_item({"module": "fastcode", "goal": "implement shared contract", "status": "blocked"})
    consumer = manager.create_work_item({"module": "nanobot", "goal": "consume shared contract", "status": "proposed"})

    contract = manager.create_contract(
        {
            "provider_module": "fastcode",
            "consumer_module": "nanobot",
            "interface_name": "SharedSearch",
            "status": "requested",
            "consumer_work_item_id": consumer.id,
            "provider_work_item_id": provider.id,
        }
    )
    manager.run_workflow_scheduler_tick(limit=50)

    edge = manager.list_dependency_edges(
        filters={
            "source_work_item_id": consumer.id,
            "target_work_item_id": provider.id,
            "edge_type": "requires_contract",
        },
        limit=10,
    )[0]
    consumer_blocked = manager.get_work_item(consumer.id)

    assert consumer_blocked is not None and consumer_blocked.status == "blocked"
    assert edge.status == "active"

    manager.update_contract(contract.id, {"status": "implemented"})
    manager.run_workflow_scheduler_tick(limit=50)

    provider_after = manager.get_work_item(provider.id)
    consumer_after = manager.get_work_item(consumer.id)
    edge_after = manager.get_dependency_edge(edge.id)

    assert provider_after is not None and provider_after.status == "completed"
    assert edge_after is not None and edge_after.status == "inactive"
    assert consumer_after is not None and consumer_after.status == "ready"
    assert consumer_after.blocked_by == []
