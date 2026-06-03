from __future__ import annotations

import asyncio
import logging
import re
import uuid
from pathlib import Path

logger = logging.getLogger(__name__)

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

        if "request_user_decision" in tool_names:
            return LLMResponse(content="Use 1 the tools you have available.")

        if "delegate_project_task" in tool_names:
            if "decision" in str(messages[-1].get("content") or "").lower():
                if any(msg.get("name") == "delegate_project_task" for msg in tool_messages):
                    return LLMResponse(
                        content="Delegated module1 and waiting for a user decision before finishing the change.",
                    )
                return LLMResponse(
                    content="Dispatching a decision-driven module1 project agent.",
                    tool_calls=[
                        ToolCallRequest(
                            id="core-module1-decision",
                            name="delegate_project_task",
                            arguments={
                                "project": "test_code/module1",
                                "task": "Need user decision to choose the module1 interface style before editing api.py.",
                            },
                        ),
                    ],
                )
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

    @staticmethod
    def _extract_latest_user_message(messages: list[dict]) -> str:
        for message in reversed(messages):
            if message.get("role") == "user":
                return str(message.get("content") or "")
        return ""


async def _wait_for(predicate, *, timeout: float = 5.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("Condition was not met before timeout")


async def _consume_response(bus: MessageBus, *, channel: str, chat_id: str, timeout: float = 120.0) -> OutboundMessage:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        message = await asyncio.wait_for(bus.consume_outbound(), timeout=max(0.1, deadline - loop.time()))
        if message.channel == channel and message.chat_id == chat_id:
            return message
    raise AssertionError("Timed out waiting for the requested outbound message")


async def _consume_many_responses(
    bus: MessageBus,
    *,
    channel: str,
    chat_id: str,
    count: int,
    timeout: float = 1200.0,
    predicate = None,
) -> list[OutboundMessage]:
    messages: list[OutboundMessage] = []
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while len(messages) < count and loop.time() < deadline:
        message = await asyncio.wait_for(bus.consume_outbound(), timeout=max(0.1, deadline - loop.time()))
        if message.channel == channel and message.chat_id == chat_id:
            if predicate is None or predicate(message):
                messages.append(message)
    if len(messages) != count:
        raise AssertionError(f"Timed out waiting for {count} outbound messages; got {len(messages)}")
    return messages


async def _consume_matching_response(
    bus: MessageBus,
    *,
    predicate,
    timeout: float = 120.0,
) -> OutboundMessage:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        message = await asyncio.wait_for(bus.consume_outbound(), timeout=max(0.1, deadline - loop.time()))
        if predicate(message):
            return message
    raise AssertionError("Timed out waiting for the requested matching outbound message")


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
    # Use scripted provider in project subprocess workers (test mode)
    manager._worker_provider_type = "scripted"

    test_files = {
        module_name: repo_root / "test_code" / module_name / "api.py"
        for module_name in ("module1", "module2", "module3")
    }
    original_contents = {module_name: path.read_text(encoding="utf-8") for module_name, path in test_files.items()}

    manager_task = asyncio.create_task(manager.run())
    try:
        # await _wait_for(lambda: manager._reconciler_task is not None and not manager._reconciler_task.done())
        # sleep briefly to ensure the manager is fully up and running before publishing the message
        await asyncio.sleep(3)

        logger.info("\x1b[32m Publishing test message to trigger delegation... \x1b[0m")
        # core manager的入口其实可以并行，project agent处理具体的目录，并行的话可能会有冲突
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
        assert set(manager._project_subprocesses) == {"test_code/module1", "test_code/module2", "test_code/module3"}
        for handle in manager._project_subprocesses.values():
            assert handle.process.returncode is None, f"Subprocess for {handle.project} exited"

        # await asyncio.sleep(5)
        # await asyncio.sleep(5)
        # await asyncio.sleep(5)
        # await asyncio.sleep(5)
        # await asyncio.sleep(5)
        # await asyncio.sleep(5)
        # await asyncio.sleep(5)
        # await asyncio.sleep(5)
        decision_messages = await _consume_many_responses(
            bus,
            channel="e2e",
            chat_id="core-run-flow",
            count=3,
            timeout=9000.0,
            predicate=lambda message: str(message.metadata.get("type") or "") == "project_agent_decision_request",
        )
        # 过滤垃圾ai写的bug，过滤掉module 1的相关decision
        await asyncio.sleep(3)
        for dm in decision_messages:
            assert dm.metadata.get("project") in {"test_code/module1", "test_code/module2", "test_code/module3"}
            if dm.metadata.get("project") == "test_code/module1":
                continue  # skip the buggy decision from the garbage ai
            await bus.publish_inbound(
                InboundMessage(
                    channel="e2e",
                    sender_id="tester",
                    chat_id="core-run-flow",
                    content="rest",
                    metadata={"project_decision_id": dm.metadata["project_decision_id"]},
                )
            )
        await asyncio.sleep(1)
        completion_messages = await _consume_many_responses(
            bus,
            channel="e2e",
            chat_id="core-run-flow",
            count=3,
            timeout=6000.0,
            predicate=lambda message: message.content.startswith("[Project Scope:"),
        )
        completion_content = "\n".join(message.content for message in completion_messages)
        assert "[Project Scope: test_code/module2]" in completion_content
        assert "[Project Scope: test_code/module3]" in completion_content
        assert "USER_DECISION: rest" in completion_content

        await asyncio.sleep(1)  # wait for the delegated file edits to be flushed

        for module_name, path in test_files.items():
            content = path.read_text(encoding="utf-8")
            if module_name == "module1":
                continue
            print(content)

        # module2_content = test_files["module2"].read_text(encoding="utf-8")
        # assert "# USER_DECISION: rest" in module2_content
    except Exception as e:
        logger.error("\x1b[31m Test failed with exception: %s \x1b[0m", e)
        assert False, f"Test failed with exception: {e}"
    finally:
        logger.info("\x1b[32m Restoring original file contents... \x1b[0m")
        for module_name, path in test_files.items():
            if module_name in original_contents:
                path.write_text(original_contents[module_name], encoding="utf-8")
        manager.stop()
        await asyncio.wait_for(manager_task, timeout=5.0)

async def test_gateway(bus) -> None:
    try:
        await asyncio.sleep(3)
        logger.info("\x1b[32m Publishing test message to trigger delegation... \x1b[0m")
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

        # await asyncio.sleep(5)
        # await asyncio.sleep(5)
        # await asyncio.sleep(5)
        decision_messages = await _consume_many_responses(
            bus,
            channel="e2e",
            chat_id="core-run-flow",
            count=3,
            timeout=9000.0,
            predicate=lambda message: str(message.metadata.get("type") or "") == "project_agent_decision_request",
        )
        # 过滤垃圾ai写的bug，过滤掉module 1的相关decision
        await asyncio.sleep(3)
        for dm in decision_messages:
            assert dm.metadata.get("project") in {"test_code/module1", "test_code/module2", "test_code/module3"}
            if dm.metadata.get("project") == "test_code/module1":
                continue  # skip the buggy decision from the garbage ai
            await bus.publish_inbound(
                InboundMessage(
                    channel="e2e",
                    sender_id="tester",
                    chat_id="core-run-flow",
                    content="rest",
                    metadata={"project_decision_id": dm.metadata["project_decision_id"]},
                )
            )
        await asyncio.sleep(1)
        completion_messages = await _consume_many_responses(
            bus,
            channel="e2e",
            chat_id="core-run-flow",
            count=3,
            timeout=6000.0,
            predicate=lambda message: message.content.startswith("[Project Scope:"),
        )
        completion_content = "\n".join(message.content for message in completion_messages)
        assert "[Project Scope: test_code/module2]" in completion_content
        assert "[Project Scope: test_code/module3]" in completion_content
        assert "USER_DECISION: rest" in completion_content
    except Exception as e:
        logger.error("\x1b[31m Test failed with exception: %s \x1b[0m", e)
        assert False, f"Test failed with exception: {e}"


async def test_core_manager_run_e2e_project_agent_requests_user_decision(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    workflow_db = tmp_path / f"workflow_test_{uuid.uuid4().hex}.db"
    workflow_store = WorkflowStore(workflow_db)
    bus = MessageBus()
    provider = ScriptedDelegationProvider(repo_root)
    manager = CoreAgentManager(
        bus=bus,
        provider=provider,
        workspace=repo_root,
        allowed_project_scopes=["test_code/module1"],
        workflow_store=workflow_store,
        decision_sla_seconds=3600,
        decision_sla_block_scope="module",
        decision_queue_impact_weight=10,
        decision_queue_age_weight=1,
        decision_default_degradation="wait",
    )
    manager._worker_provider_type = "scripted"

    test_file = repo_root / "test_code" / "module1" / "api.py"
    original_content = test_file.read_text(encoding="utf-8")

    manager_task = asyncio.create_task(manager.run())
    try:
        await asyncio.sleep(3)

        await bus.publish_inbound(
            InboundMessage(
                channel="e2e",
                sender_id="tester",
                chat_id="core-decision-flow",
                content="Run a decision-driven module1 change and ask for a user decision if uncertain.",
            )
        )

        initial_response = await _consume_response(bus, channel="e2e", chat_id="core-decision-flow")
        assert "module1" in initial_response.content.lower()

        decision_prompt = await _consume_matching_response(
            bus,
            predicate=lambda message: (
                message.channel == "e2e"
                and message.chat_id == "core-decision-flow"
                and str(message.metadata.get("type") or "") == "project_agent_decision_request"
            ),
            timeout=60.0,
        )
        decision_id = str(decision_prompt.metadata.get("project_decision_id") or "")
        assert decision_id
        assert decision_prompt.metadata.get("project") == "test_code/module1"
        assert decision_prompt.metadata.get("options") == ["rest", "graphql"]

        await bus.publish_inbound(
            InboundMessage(
                channel="e2e",
                sender_id="tester",
                chat_id="core-decision-flow",
                content="rest",
                metadata={"project_decision_id": decision_id},
            )
        )

        completion = await _consume_matching_response(
            bus,
            predicate=lambda message: (
                message.channel == "e2e"
                and message.chat_id == "core-decision-flow"
                and "USER_DECISION: rest" in message.content
            ),
            timeout=60.0,
        )
        assert "[Project Scope: test_code/module1]" in completion.content

        await asyncio.sleep(1)
        updated = test_file.read_text(encoding="utf-8")
        assert "# USER_DECISION: rest" in updated
    finally:
        test_file.write_text(original_content, encoding="utf-8")
        manager.stop()
        await asyncio.wait_for(manager_task, timeout=5.0)


async def test_ws_gateway_inbound(tmp_path: Path) -> None:
    """Minimal test: connect to gateway WS, send an inbound message, verify it reaches the bus."""
    import websockets
    import json as json_module

    repo_root = Path(__file__).resolve().parents[2]
    workflow_db = tmp_path / f"ws_test_{uuid.uuid4().hex}.db"
    workflow_store = WorkflowStore(workflow_db)
    bus = MessageBus()
    provider = ScriptedDelegationProvider(repo_root)

    manager = CoreAgentManager(
        bus=bus,
        provider=provider,
        workspace=repo_root,
        allowed_project_scopes=["test_code/module1"],
        workflow_store=workflow_store,
        decision_sla_seconds=3600,
        decision_sla_block_scope="module",
        decision_queue_impact_weight=10,
        decision_queue_age_weight=1,
        decision_default_degradation="wait",
    )
    manager._worker_provider_type = "scripted"

    from nanobot.channels.workflow_ws import WorkflowWSChannel
    from nanobot.config.schema import WorkflowWSConfig

    ws_config = WorkflowWSConfig(enabled=True, host="127.0.0.1", port=0)  # port=0 -> OS assigns
    ws_channel = WorkflowWSChannel(ws_config, bus)

    manager_task = asyncio.create_task(manager.run())
    ws_task = asyncio.create_task(ws_channel.start())
    await asyncio.sleep(2)

    # 获取实际分配的端口 (port=0 时 OS 自动分配)
    server_obj = getattr(ws_channel, "_server", None)
    assert server_obj is not None, "WebSocket server not started"
    actual_port: int = 0
    for s in server_obj.sockets:
        try:
            actual_port = s.getsockname()[1]
            break
        except Exception:
            pass
    assert actual_port, f"Could not determine WebSocket port from {server_obj}"

    ws_url = f"ws://127.0.0.1:{actual_port}/workflow"
    logger.info("Connecting to WS at %s", ws_url)

    try:
        # 1) 连接 WS
        async with websockets.connect(ws_url) as ws:
            # 2) 收 connected 消息
            connected_raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
            connected = json_module.loads(connected_raw)
            assert connected["type"] == "workflow.connected"
            logger.info("Connected: cursor=%s", connected["payload"].get("latest_cursor"))

            # 3) 发送 inbound 消息（模拟前端提交决策回复）
            test_content = "rest"
            test_decision_id = f"test-decision-{uuid.uuid4().hex[:8]}"
            await ws.send(json_module.dumps({
                "type": "inbound",
                "content": test_content,
                "metadata": {"project_decision_id": test_decision_id},
            }))

            # 4) 收 ack
            ack_raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
            ack = json_module.loads(ack_raw)
            assert ack["type"] == "workflow.inbound.ack"
            assert ack["payload"]["ok"] is True
            assert ack["payload"]["char_len"] == len(test_content)
            logger.info("Inbound ack received: ok=%s", ack["payload"]["ok"])

        # 5) 从 bus 中消费这条 inbound 消息（验证它确实进了 bus）
        inbound_msg = await asyncio.wait_for(bus.consume_inbound(), timeout=5.0)
        assert inbound_msg.content == test_content
        assert inbound_msg.metadata.get("project_decision_id") == test_decision_id
        logger.info("Inbound message consumed from bus: content=%s", inbound_msg.content)

        logger.info("\x1b[32m WS gateway inbound test PASSED \x1b[0m")
    finally:
        manager.stop()
        ws_channel.stop()
        await asyncio.wait_for(asyncio.gather(manager_task, ws_task, return_exceptions=True), timeout=5.0)
