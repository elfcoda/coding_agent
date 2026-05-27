from __future__ import annotations

import uuid
from pathlib import Path

from fastapi.testclient import TestClient

from nanobot.agent.core_manager import CoreAgentManager
from nanobot.bus.queue import MessageBus
from nanobot.control_plane.server import create_control_plane_app
from nanobot.providers.base import LLMProvider, LLMResponse
from nanobot.workflow import WorkflowStore


class DummyProvider(LLMProvider):
    async def chat(self, messages, tools=None, model=None, max_tokens=4096, temperature=1.0):
        return LLMResponse(content="ok")

    def get_default_model(self):
        return "dummy/model"


def _make_app(tmp_path: Path) -> tuple[TestClient, CoreAgentManager]:
    (tmp_path / "fastcode").mkdir(parents=True, exist_ok=True)
    (tmp_path / "nanobot").mkdir(parents=True, exist_ok=True)

    db = tmp_path / f"workflow_test_{uuid.uuid4().hex}.db"
    manager = CoreAgentManager(
        bus=MessageBus(),
        provider=DummyProvider(),
        workspace=tmp_path,
        allowed_project_scopes=["fastcode", "nanobot"],
        workflow_store=WorkflowStore(db),
    )
    app = create_control_plane_app(manager)
    return TestClient(app), manager


def test_command_update_decision_degradation(tmp_path: Path) -> None:
    client, manager = _make_app(tmp_path)
    work_item = manager.create_work_item({"module": "fastcode", "goal": "manual control", "status": "proposed"})

    response = client.post(
        f"/api/control/commands/work-items/{work_item.id}/decision-degradation",
        json={"decision_degradation": "continue_partial"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["decision_degradation"] == "continue_partial"

    updated = manager.get_work_item(work_item.id)
    assert updated is not None
    assert updated.metadata["scheduler"]["decision_degradation"] == "continue_partial"


def test_command_update_decision_degradation_rejects_invalid_mode(tmp_path: Path) -> None:
    client, manager = _make_app(tmp_path)
    work_item = manager.create_work_item({"module": "fastcode", "goal": "invalid control", "status": "proposed"})

    response = client.post(
        f"/api/control/commands/work-items/{work_item.id}/decision-degradation",
        json={"decision_degradation": "invalid_mode"},
    )

    assert response.status_code == 400
    assert "decision_degradation must be one of" in response.json()["detail"]
