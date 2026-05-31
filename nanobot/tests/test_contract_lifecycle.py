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


def _create_contract_dependency(manager: CoreAgentManager):
    provider = manager.create_work_item({"module": "fastcode", "goal": "implement shared contract", "status": "in_progress"})
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
            "status": "active",
        },
        limit=10,
    )[0]
    return provider, consumer, contract, edge


def test_blocked_by_tracks_only_dependency_edge_ids_for_contract_blockers(tmp_path: Path) -> None:
    (tmp_path / "fastcode").mkdir(parents=True, exist_ok=True)
    (tmp_path / "nanobot").mkdir(parents=True, exist_ok=True)
    manager = _make_manager(tmp_path)

    _, consumer, contract, edge = _create_contract_dependency(manager)

    consumer_after = manager.get_work_item(consumer.id)

    assert consumer_after is not None
    assert consumer_after.blocked_by == [edge.id]
    assert contract.id not in consumer_after.blocked_by


def test_blocked_by_preserves_manual_blockers_when_contract_resolves(tmp_path: Path) -> None:
    (tmp_path / "fastcode").mkdir(parents=True, exist_ok=True)
    (tmp_path / "nanobot").mkdir(parents=True, exist_ok=True)
    manager = _make_manager(tmp_path)

    provider = manager.create_work_item({"module": "fastcode", "goal": "implement shared contract", "status": "in_progress"})
    consumer = manager.create_work_item(
        {
            "module": "nanobot",
            "goal": "consume shared contract",
            "status": "proposed",
            "blocked_by": ["manual:review"],
        }
    )

    manager.create_contract(
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
            "status": "active",
        },
        limit=10,
    )[0]
    blocked = manager.get_work_item(consumer.id)

    assert blocked is not None
    assert set(blocked.blocked_by) == {"manual:review", edge.id}

    contract = manager.list_contracts(filters={"interface_name": "SharedSearch"}, limit=10)[0]
    manager.update_contract(contract.id, {"status": "implemented"})
    manager.run_workflow_scheduler_tick(limit=50)

    consumer_after = manager.get_work_item(consumer.id)

    assert consumer_after is not None
    assert consumer_after.blocked_by == ["manual:review"]
    assert consumer_after.status == "blocked"


def test_contract_resolve_at_version_one_does_not_create_module_followup(tmp_path: Path) -> None:
    (tmp_path / "fastcode").mkdir(parents=True, exist_ok=True)
    (tmp_path / "nanobot").mkdir(parents=True, exist_ok=True)
    manager = _make_manager(tmp_path)

    provider, consumer, contract, _ = _create_contract_dependency(manager)
    existing_nanobot_ids = {item.id for item in manager.list_work_items(filters={"module": "nanobot"}, limit=50)}

    manager.update_contract(contract.id, {"status": "implemented", "version": 1})
    manager.run_workflow_scheduler_tick(limit=50)

    provider_after = manager.get_work_item(provider.id)
    consumer_after = manager.get_work_item(consumer.id)
    nanobot_items = manager.list_work_items(filters={"module": "nanobot"}, limit=50)
    followups = [item for item in nanobot_items if item.id not in existing_nanobot_ids]

    assert provider_after is not None and provider_after.status == "completed"
    assert consumer_after is not None and consumer_after.blocked_by == []
    assert followups == []


def test_contract_invalidation_creates_followup_without_contract_id_blockers(tmp_path: Path) -> None:
    (tmp_path / "fastcode").mkdir(parents=True, exist_ok=True)
    (tmp_path / "nanobot").mkdir(parents=True, exist_ok=True)
    manager = _make_manager(tmp_path)

    provider, consumer, contract, edge = _create_contract_dependency(manager)
    manager.update_contract(contract.id, {"status": "implemented", "version": 1})
    manager.run_workflow_scheduler_tick(limit=50)

    existing_nanobot_ids = {item.id for item in manager.list_work_items(filters={"module": "nanobot"}, limit=50)}

    manager.update_contract(
        contract.id,
        {
            "version": 2,
            "status": "invalidated",
            "invalidate_dependents": True,
        },
    )
    manager.run_workflow_scheduler_tick(limit=50)

    provider_after = manager.get_work_item(provider.id)
    consumer_after = manager.get_work_item(consumer.id)
    nanobot_items = manager.list_work_items(filters={"module": "nanobot"}, limit=50)
    followups = [item for item in nanobot_items if item.id not in existing_nanobot_ids]

    assert provider_after is not None and provider_after.status == "in_progress"
    assert consumer_after is not None
    assert consumer_after.blocked_by == [edge.id]
    assert contract.id not in consumer_after.blocked_by
    assert len(followups) == 1
    assert followups[0].metadata.get("contract_id") == contract.id
    assert followups[0].metadata.get("contract_version") == 2
