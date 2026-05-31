from __future__ import annotations

import sqlite3
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


def _make_manager(tmp_path: Path, *, sla_seconds: int = 60) -> tuple[CoreAgentManager, Path]:
    db = tmp_path / f"workflow_test_{uuid.uuid4().hex}.db"
    store = WorkflowStore(db)
    manager = CoreAgentManager(
        bus=MessageBus(),
        provider=DummyProvider(),
        workspace=tmp_path,
        allowed_project_scopes=["fastcode", "nanobot"],
        workflow_store=store,
        decision_sla_seconds=sla_seconds,
        decision_sla_block_scope="module",
        decision_queue_impact_weight=10,
        decision_queue_age_weight=1,
        decision_default_degradation="wait",
    )
    return manager, db


def _backdate_decision(db_path: Path, decision_id: str, ts: str = "2020-01-01T00:00:00+00:00") -> None:
    con = sqlite3.connect(db_path)
    try:
        con.execute(
            "UPDATE decisions SET created_at = ?, updated_at = ? WHERE id = ?",
            (ts, ts, decision_id),
        )
        con.commit()
    finally:
        con.close()


def test_decision_sla_overdue_blocks_module_dispatch(tmp_path: Path) -> None:
    (tmp_path / "fastcode").mkdir(parents=True, exist_ok=True)
    (tmp_path / "nanobot").mkdir(parents=True, exist_ok=True)
    manager, db = _make_manager(tmp_path, sla_seconds=5)

    wi_decision = manager.create_work_item({"module": "fastcode", "goal": "need decision", "status": "proposed"})
    manager.create_work_item({"module": "fastcode", "goal": "fastcode ready", "status": "ready", "priority": 10})
    manager.create_work_item({"module": "nanobot", "goal": "nanobot ready", "status": "ready", "priority": 1})
    decision = manager.create_decision({"work_item_id": wi_decision.id, "decision_type": "arch", "status": "pending"})

    _backdate_decision(db, decision.id)

    summary = manager.run_workflow_scheduler_tick(limit=50)
    plan = manager._build_dispatch_plan(manager.list_work_items(filters={"status": "ready"}, limit=50))

    assert summary["decision_control"]["overdue_count"] == 1
    assert "fastcode" in summary["decision_control"]["blocked_modules"]
    assert [entry["module"] for entry in plan] == ["nanobot"]


def test_decision_queue_prioritizes_higher_impact(tmp_path: Path) -> None:
    (tmp_path / "fastcode").mkdir(parents=True, exist_ok=True)
    (tmp_path / "nanobot").mkdir(parents=True, exist_ok=True)
    manager, _ = _make_manager(tmp_path, sla_seconds=3600)

    wi_heavy = manager.create_work_item({"module": "fastcode", "goal": "heavy impact", "status": "proposed", "priority": 0})
    wi_dep_a = manager.create_work_item({"module": "fastcode", "goal": "depends a", "status": "ready"})
    wi_dep_b = manager.create_work_item({"module": "nanobot", "goal": "depends b", "status": "ready"})
    wi_light = manager.create_work_item({"module": "nanobot", "goal": "light impact", "status": "proposed", "priority": 0})

    manager.create_dependency_edge(
        {
            "source_work_item_id": wi_dep_a.id,
            "target_work_item_id": wi_heavy.id,
            "edge_type": "requires_contract",
            "status": "active",
        }
    )
    manager.create_dependency_edge(
        {
            "source_work_item_id": wi_dep_b.id,
            "target_work_item_id": wi_heavy.id,
            "edge_type": "requires_contract",
            "status": "active",
        }
    )

    heavy_decision = manager.create_decision({"work_item_id": wi_heavy.id, "decision_type": "design", "status": "pending"})
    light_decision = manager.create_decision({"work_item_id": wi_light.id, "decision_type": "design", "status": "pending"})

    queue = manager.get_decision_queue(limit=10)

    assert queue[0]["decision_id"] == heavy_decision.id
    assert queue[0]["impact_size"] > queue[1]["impact_size"]
    assert {item["decision_id"] for item in queue[:2]} == {heavy_decision.id, light_decision.id}


def test_degradation_modes_affect_waiting_decision_status(tmp_path: Path) -> None:
    (tmp_path / "fastcode").mkdir(parents=True, exist_ok=True)
    (tmp_path / "nanobot").mkdir(parents=True, exist_ok=True)
    manager, _ = _make_manager(tmp_path, sla_seconds=3600)

    wi_wait = manager.create_work_item({"module": "fastcode", "goal": "wait mode", "status": "proposed"})
    wi_stub = manager.create_work_item({"module": "fastcode", "goal": "stub mode", "status": "proposed"})
    wi_partial = manager.create_work_item({"module": "nanobot", "goal": "partial mode", "status": "proposed"})

    manager.create_decision(
        {
            "work_item_id": wi_wait.id,
            "decision_type": "implementation",
            "status": "pending",
            "metadata": {"degradation_mode": "wait"},
        }
    )
    manager.create_decision(
        {
            "work_item_id": wi_stub.id,
            "decision_type": "implementation",
            "status": "pending",
            "metadata": {"degradation_mode": "stub"},
        }
    )
    manager.create_decision(
        {
            "work_item_id": wi_partial.id,
            "decision_type": "implementation",
            "status": "pending",
            "metadata": {"degradation_mode": "continue_partial"},
        }
    )

    manager.run_workflow_scheduler_tick(limit=50)

    wait_after = manager.get_work_item(wi_wait.id)
    stub_after = manager.get_work_item(wi_stub.id)
    partial_after = manager.get_work_item(wi_partial.id)

    assert wait_after is not None and wait_after.status == "waiting_decision"
    assert stub_after is not None and stub_after.status == "blocked"
    assert partial_after is not None and partial_after.status == "ready"


def test_contract_request_tracks_provider_work_item_and_uses_edge_blockers(tmp_path: Path) -> None:
    (tmp_path / "fastcode").mkdir(parents=True, exist_ok=True)
    (tmp_path / "nanobot").mkdir(parents=True, exist_ok=True)
    manager, _ = _make_manager(tmp_path, sla_seconds=3600)

    provider = manager.create_work_item({"module": "fastcode", "goal": "implement shared contract", "status": "in_progress"})
    consumer = manager.create_work_item({"module": "nanobot", "goal": "wait for shared contract", "status": "proposed"})

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

    contract_after = manager.get_contract(contract.id)
    consumer_after = manager.get_work_item(consumer.id)
    edges = manager.list_dependency_edges(
        filters={
            "source_work_item_id": consumer.id,
            "target_work_item_id": provider.id,
            "edge_type": "requires_contract",
            "status": "active",
        },
        limit=10,
    )

    assert contract_after is not None and contract_after.work_item_id == provider.id
    assert len(edges) == 1
    assert consumer_after is not None and consumer_after.status == "blocked"
    assert consumer_after.blocked_by == [edges[0].id]


def test_scheduler_clears_edge_blocker_when_dependency_edge_is_inactive(tmp_path: Path) -> None:
    (tmp_path / "fastcode").mkdir(parents=True, exist_ok=True)
    (tmp_path / "nanobot").mkdir(parents=True, exist_ok=True)
    manager, _ = _make_manager(tmp_path, sla_seconds=3600)

    provider = manager.create_work_item({"module": "fastcode", "goal": "implement shared contract", "status": "in_progress"})
    consumer = manager.create_work_item({"module": "nanobot", "goal": "wait for shared contract", "status": "proposed"})

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

    manager.update_dependency_edge(edge.id, {"status": "inactive"})
    manager.run_workflow_scheduler_tick(limit=50)

    consumer_after = manager.get_work_item(consumer.id)

    assert consumer_after is not None and consumer_after.status == "ready"
    assert consumer_after.blocked_by == []


def test_contract_version_change_creates_module_followup_work_item(tmp_path: Path) -> None:
    (tmp_path / "fastcode").mkdir(parents=True, exist_ok=True)
    (tmp_path / "nanobot").mkdir(parents=True, exist_ok=True)
    manager, _ = _make_manager(tmp_path, sla_seconds=3600)

    provider = manager.create_work_item({"module": "fastcode", "goal": "implement shared contract", "status": "in_progress"})
    consumer = manager.create_work_item({"module": "nanobot", "goal": "integrate shared contract", "status": "ready"})

    contract = manager.create_contract(
        {
            "provider_module": "fastcode",
            "consumer_module": "nanobot",
            "interface_name": "SharedSearch",
            "status": "implemented",
            "version": 1,
            "consumer_work_item_id": consumer.id,
            "provider_work_item_id": provider.id,
        }
    )

    existing_nanobot_ids = {item.id for item in manager.list_work_items(filters={"module": "nanobot"}, limit=50)}

    manager.update_contract(
        contract.id,
        {
            "version": 2,
            "status": "invalidated",
            "invalidate_dependents": True,
        },
    )

    nanobot_items = manager.list_work_items(filters={"module": "nanobot"}, limit=50)
    followups = [item for item in nanobot_items if item.id not in existing_nanobot_ids]

    assert len(followups) == 1
    assert followups[0].status == "proposed"
    assert followups[0].metadata.get("contract_id") == contract.id
    assert followups[0].metadata.get("contract_version") == 2
