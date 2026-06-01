from __future__ import annotations

import asyncio
import re
import uuid
from pathlib import Path

from nanobot.agent.core_manager import CoreAgentManager
from nanobot.bus.events import InboundMessage, OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.providers.base import LLMProvider, LLMResponse, ToolCallRequest
from nanobot.workflow import WorkflowStore


class ScriptedDelegationProvider(LLMProvider):
    def __init__(self, repo_root: Path):
        super().__init__()
        self._repo_root = repo_root.resolve()

    def get_default_model(self) -> str:
        return "scripted/e2e"

    async def chat(self, messages, tools=None, model=None, max_tokens=4096, temperature=1.0):
        tool_names = {
            item.get("function", {}).get("name", "")
            for item in (tools or [])
            if isinstance(item, dict)
        }
        tool_messages = [msg for msg in messages if msg.get("role") == "tool"]

        if "delegate_project_task" in tool_names:
            if any(msg.get("name") == "delegate_project_task" for msg in tool_messages):
                return LLMResponse(
                    content="Delegated simple interface updates to module1, module2, and module3.",
                )
            return LLMResponse(
                content="Dispatching fixed test_code project agents.",
                tool_calls=[
                    ToolCallRequest(
                        id="core-module1",
                        name="delegate_project_task",
                        arguments={
                            "project": "test_code/module1",
                            "task": "Add a simple public interface function to the existing api.py file.",
                        },
                    ),
                    ToolCallRequest(
                        id="core-module2",
                        name="delegate_project_task",
                        arguments={
                            "project": "test_code/module2",
                            "task": "Add a simple public interface function to the existing api.py file.",
                        },
                    ),
                    ToolCallRequest(
                        id="core-module3",
                        name="delegate_project_task",
                        arguments={
                            "project": "test_code/module3",
                            "task": "Add a simple public interface function to the existing api.py file.",
                        },
                    ),
                ],
            )

        if "edit_file" in tool_names:
            workspace = self._extract_workspace(messages)
            module_name = workspace.name
            if any(msg.get("name") == "edit_file" for msg in tool_messages):
                return LLMResponse(content=f"Updated {module_name} api.py with a simple interface.")
            return LLMResponse(
                content=f"Editing {module_name} api.py.",
                tool_calls=[
                    ToolCallRequest(
                        id=f"project-{module_name}",
                        name="edit_file",
                        arguments={
                            "path": str(workspace / "api.py"),
                            "old_text": "# ADD_INTERFACE_HERE",
                            "new_text": (
                                f"def get_{module_name}_interface() -> str:\n"
                                f"    return \"{module_name}-interface\"\n\n"
                                "# ADD_INTERFACE_HERE"
                            ),
                        },
                    )
                ],
            )

        return LLMResponse(content="No action required.")

    @staticmethod
    def _extract_workspace(messages: list[dict]) -> Path:
        system_content = str(messages[0].get("content") or "")
        match = re.search(r"Your workspace is at: (.+)", system_content)
        if not match:
            raise AssertionError("Workspace path not found in system prompt")
        return Path(match.group(1).strip())


async def _wait_for(predicate, *, timeout: float = 5.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("Condition was not met before timeout")


async def _consume_response(bus: MessageBus, *, channel: str, chat_id: str, timeout: float = 10.0) -> OutboundMessage:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        message = await asyncio.wait_for(bus.consume_outbound(), timeout=max(0.1, deadline - loop.time()))
        if message.channel == channel and message.chat_id == chat_id:
            return message
    raise AssertionError("Timed out waiting for the requested outbound message")


async def test_core_manager_run_e2e_delegates_fixed_test_code_projects(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    workflow_db = tmp_path / f"workflow_test_{uuid.uuid4().hex}.db"
    workflow_store = WorkflowStore(workflow_db)
    bus = MessageBus()
    provider = ScriptedDelegationProvider(repo_root)
    manager = CoreAgentManager(
        bus=bus,
        provider=provider,
        workspace=repo_root,
        allowed_project_scopes=["test_code/module1", "test_code/module2", "test_code/module3"],
        workflow_store=workflow_store,
        decision_sla_seconds=3600,
        decision_sla_block_scope="module",
        decision_queue_impact_weight=10,
        decision_queue_age_weight=1,
        decision_default_degradation="wait",
    )

    test_files = {
        module_name: repo_root / "test_code" / module_name / "api.py"
        for module_name in ("module1", "module2", "module3")
    }
    original_contents = {module_name: path.read_text(encoding="utf-8") for module_name, path in test_files.items()}

    manager_task = asyncio.create_task(manager.run())
    try:
        await _wait_for(lambda: manager._reconciler_task is not None and not manager._reconciler_task.done())

        await bus.publish_inbound(
            InboundMessage(
                channel="e2e",
                sender_id="tester",
                chat_id="core-run-flow",
                content="Use the three fixed project agents under test_code to add one simple interface to each module.",
            )
        )

        response = await _consume_response(bus, channel="e2e", chat_id="core-run-flow")

        assert "module1" in response.content
        assert "module2" in response.content
        assert "module3" in response.content
        assert set(manager.project_loops) == {"test_code/module1", "test_code/module2", "test_code/module3"}

        for module_name, path in test_files.items():
            content = path.read_text(encoding="utf-8")
            assert f"def get_{module_name}_interface() -> str:" in content
            assert f'return "{module_name}-interface"' in content
    finally:
        for module_name, path in test_files.items():
            if module_name in original_contents:
                path.write_text(original_contents[module_name], encoding="utf-8")
        manager.stop()
        await asyncio.wait_for(manager_task, timeout=5.0)
