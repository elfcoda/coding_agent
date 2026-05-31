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


async def test_core_loop_has_manage_workflow_tool(tmp_path: Path) -> None:
    (tmp_path / "fastcode").mkdir(parents=True, exist_ok=True)
    (tmp_path / "nanobot").mkdir(parents=True, exist_ok=True)
    manager = _make_manager(tmp_path)

    assert manager.core_loop.tools.has("manage_workflow")


async def test_project_loop_has_manage_workflow_tool_via_core_manager(tmp_path: Path) -> None:
    (tmp_path / "fastcode").mkdir(parents=True, exist_ok=True)
    (tmp_path / "nanobot").mkdir(parents=True, exist_ok=True)
    manager = _make_manager(tmp_path)

    project_loop = manager.get_project_loop("fastcode")

    assert project_loop.tools.has("manage_workflow")

    result = await project_loop.tools.execute(
        "manage_workflow",
        {
            "entity": "work_item",
            "action": "create",
            "fields": {
                "module": "fastcode",
                "goal": "created through project loop",
                "status": "proposed",
            },
        },
    )

    assert "created through project loop" in result
    items = manager.list_work_items(filters={"module": "fastcode"}, limit=20)
    assert any(item.goal == "created through project loop" for item in items)


async def test_subagent_toolset_does_not_include_manage_workflow(tmp_path: Path) -> None:
    (tmp_path / "fastcode").mkdir(parents=True, exist_ok=True)
    (tmp_path / "nanobot").mkdir(parents=True, exist_ok=True)
    manager = _make_manager(tmp_path)

    tools = manager.core_loop.subagents._build_tool_registry()

    assert not tools.has("manage_workflow")


async def test_project_loop_manage_workflow_cannot_run_scheduler_actions(tmp_path: Path) -> None:
    (tmp_path / "fastcode").mkdir(parents=True, exist_ok=True)
    (tmp_path / "nanobot").mkdir(parents=True, exist_ok=True)
    manager = _make_manager(tmp_path)

    project_loop = manager.get_project_loop("fastcode")
    result = await project_loop.tools.execute(
        "manage_workflow",
        {
            "entity": "scheduler",
            "action": "tick",
        },
    )

    assert "Invalid parameters" in result
