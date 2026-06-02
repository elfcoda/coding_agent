"""
Project agent subprocess worker.

Runs as a persistent subprocess managed by CoreAgentManager.
Reads JSON tasks from stdin, processes them via AgentLoop,
and writes JSON results to stdout.

Protocol (JSON line-delimited):
  Request:  {"id": "...", "task": "...", "session_key": "...", "channel": "...", "chat_id": "..."}
  Response: {"id": "...", "success": true,  "result": "..."}
            {"id": "...", "success": false, "error": "..."}
"""

from __future__ import annotations

import asyncio
import json
import re
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loguru import logger
from numpy import random

from nanobot.agent.tools.base import Tool
from nanobot.bus.events import InboundMessage


# ---------------------------------------------------------------------------
# Scripted provider for testing (--provider-type scripted)
# ---------------------------------------------------------------------------

class _ScriptedProjectProvider:
    """A scripted LLM provider used in test mode that responds with edit_file tool calls."""

    MOCK_NETWORK_DATA = "MOCK_NETWORK_DATA: module1-service-status=healthy"

    def __init__(self, repo_root: Path):
        self._repo_root = repo_root.resolve()

    def get_default_model(self) -> str:
        return "scripted/project-worker"

    async def chat(self, messages, tools=None, model=None, max_tokens=4096, temperature=1.0):
        from nanobot.providers.base import LLMResponse, ToolCallRequest

        tool_names = {
            item.get("function", {}).get("name", "")
            for item in (tools or [])
            if isinstance(item, dict)
        }
        tool_messages = [msg for msg in messages if msg.get("role") == "tool"]

        workspace = self._extract_workspace(messages)
        module_name = workspace.name
        latest_user_message = self._extract_latest_user_message(messages)
        is_subagent_prompt = self._is_subagent_prompt(messages)
        has_mock_network_data = self.MOCK_NETWORK_DATA in latest_user_message
        decision_choice = self._extract_tool_result(tool_messages, "request_user_decision")

        if (
            module_name == "module1"
            and "need user decision" in latest_user_message.lower()
            and "request_user_decision" in tool_names
        ):
            if decision_choice is None and not any(msg.get("name") == "request_user_decision" for msg in tool_messages):
                return LLMResponse(
                    content="Need a user decision before editing module1 api.py.",
                    tool_calls=[
                        ToolCallRequest(
                            id="module1-user-decision",
                            name="request_user_decision",
                            arguments={
                                "prompt": "Choose the module1 interface style.",
                                "options": ["rest", "graphql"],
                            },
                        )
                    ],
                )
            if decision_choice and "edit_file" in tool_names:
                if any(msg.get("name") == "edit_file" for msg in tool_messages):
                    return LLMResponse(
                        content=f"Updated {module_name} api.py with USER_DECISION: {decision_choice}."
                    )
                return LLMResponse(
                    content=f"Editing {module_name} api.py with the user decision.",
                    tool_calls=[
                        ToolCallRequest(
                            id=f"project-{module_name}-decision",
                            name="edit_file",
                            arguments={
                                "path": str(workspace / "api.py"),
                                "old_text": "# ADD_INTERFACE_HERE",
                                "new_text": (
                                    f"def get_{module_name}_interface() -> str:\n"
                                    f"    return \"{module_name}-{decision_choice}-interface\"\n\n"
                                    f"# USER_DECISION: {decision_choice}\n"
                                    "# ADD_INTERFACE_HERE"
                                ),
                            },
                        )
                    ],
                )

        if is_subagent_prompt:
            if module_name == "module1" and "mock_network_fetch" in tool_names:
                if not any(msg.get("name") == "mock_network_fetch" for msg in tool_messages):
                    return LLMResponse(
                        content="Fetching mocked module1 network data.",
                        tool_calls=[
                            ToolCallRequest(
                                id="module1-mock-network-fetch",
                                name="mock_network_fetch",
                                arguments={"resource": "module1-service-status"},
                            )
                        ],
                    )
                return LLMResponse(content=self.MOCK_NETWORK_DATA)
            return LLMResponse(content=f"No mocked network data required for {module_name}.")

        if module_name == "module1" and "spawn" in tool_names and not has_mock_network_data:
            if not any(msg.get("name") == "spawn" for msg in tool_messages):
                return LLMResponse(
                    content="Spawning a helper to fetch mocked module1 network data.",
                    tool_calls=[
                        ToolCallRequest(
                            id="module1-network-probe",
                            name="spawn",
                            arguments={
                                "task": "Fetch mocked network data for module1 and return only the network status line.",
                                "label": "module1 network probe",
                            },
                        )
                    ],
                )
            return LLMResponse(content="Waiting for mocked network data before editing module1 api.py.")

        if "edit_file" in tool_names:
            if any(msg.get("name") == "edit_file" for msg in tool_messages):
                if module_name == "module1" and has_mock_network_data:
                    return LLMResponse(
                        content=(
                            f"Updated {module_name} api.py with a simple interface and {self.MOCK_NETWORK_DATA}."
                        )
                    )
                return LLMResponse(content=f"Updated {module_name} api.py with a simple interface.")

            if module_name == "module1" and has_mock_network_data:
                return LLMResponse(
                    content=f"Editing {module_name} api.py with mocked network data.",
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
                                    f"# {self.MOCK_NETWORK_DATA}\n"
                                    "# ADD_INTERFACE_HERE"
                                ),
                            },
                        )
                    ],
                )

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

    @staticmethod
    def _is_subagent_prompt(messages: list[dict]) -> bool:
        system_content = str(messages[0].get("content") or "")
        return system_content.lstrip().startswith("# Subagent")

    @staticmethod
    def _extract_tool_result(tool_messages: list[dict], tool_name: str) -> str | None:
        for message in reversed(tool_messages):
            if message.get("name") == tool_name:
                return str(message.get("content") or "")
        return None


@dataclass
class _ActiveRequestContext:
    request_id: str
    origin_channel: str
    origin_chat_id: str
    origin_session_key: str


class _ProjectDecisionBridge:
    """Bridge project-agent decision requests to the parent core manager."""

    def __init__(self, project: str, writer: Any):
        self._project = project
        self._writer = writer
        self._write_lock = asyncio.Lock()
        self._pending: dict[str, asyncio.Future[str]] = {}
        self._active_request: _ActiveRequestContext | None = None

    def activate_request(self, context: _ActiveRequestContext | None) -> None:
        self._active_request = context

    async def request_decision(self, prompt: str, options: list[str] | None = None) -> str:
        context = self._active_request
        if context is None:
            raise RuntimeError("No active project request is available for user decision")

        decision_id = f"{context.request_id}:decision:{uuid.uuid4().hex[:8]}"
        future = asyncio.get_running_loop().create_future()
        self._pending[decision_id] = future
        await self._write_json(
            {
                "type": "decision_request",
                "id": context.request_id,
                "decision_id": decision_id,
                "project": self._project,
                "channel": context.origin_channel,
                "chat_id": context.origin_chat_id,
                "session_key": context.origin_session_key,
                "prompt": prompt,
                "options": list(options or []),
            }
        )
        try:
            return await future
        finally:
            self._pending.pop(decision_id, None)

    def resolve_decision(self, payload: dict[str, Any]) -> bool:
        decision_id = str(payload.get("decision_id") or "").strip()
        if not decision_id:
            return False
        future = self._pending.get(decision_id)
        if future is None or future.done():
            return False
        answer = str(payload.get("content") or payload.get("decision") or "").strip()
        future.set_result(answer)
        return True

    def fail_all(self, reason: str) -> None:
        for future in list(self._pending.values()):
            if not future.done():
                future.set_exception(RuntimeError(reason))
        self._pending.clear()

    async def _write_json(self, payload: dict[str, Any]) -> None:
        line = json.dumps(payload, ensure_ascii=False) + "\n"
        async with self._write_lock:
            self._writer.write(line)
            self._writer.flush()


class _RequestUserDecisionTool(Tool):
    """Request a user decision through the parent core manager and wait for the reply."""

    def __init__(self, bridge: _ProjectDecisionBridge):
        self._bridge = bridge

    @property
    def name(self) -> str:
        return "request_user_decision"

    @property
    def description(self) -> str:
        return "Ask the user for a decision when the project agent is uncertain and wait for the reply."

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "prompt": {
                    "type": "string",
                    "description": "Question to send to the user",
                },
                "options": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional list of candidate answers",
                },
            },
            "required": ["prompt"],
        }

    async def execute(self, prompt: str, options: list[str] | None = None, **kwargs: Any) -> str:
        return await self._bridge.request_decision(prompt=prompt, options=options)


# ---------------------------------------------------------------------------
# Real provider: load from nanobot config
# ---------------------------------------------------------------------------

def _create_real_provider(config_path: str | None = None):
    """Create LLM provider from nanobot config."""
    from nanobot.config.loader import load_config
    from nanobot.providers.litellm_provider import LiteLLMProvider

    config = load_config(Path(config_path) if config_path else None)
    provider_config = config.providers
    model = config.agents.defaults.model

    api_key = ""
    api_base = None
    for prov_name in ("openai", "anthropic", "openrouter", "deepseek", "gemini", "groq", "zhipu", "dashscope", "aihubmix", "vllm", "moonshot"):
        prov = getattr(provider_config, prov_name, None)
        if prov is None:
            continue
        if prov.api_key:
            api_key = prov.api_key
            api_base = prov.api_base
            break

    return LiteLLMProvider(api_key=api_key, api_base=api_base), model


def _create_agent_loop(
    workspace: Path,
    config_path: str | None,
    scope_hint: str | None,
    provider_type: str = "litellm",
) -> Any:
    """Create an AgentLoop for the project worker."""
    from nanobot.bus.queue import MessageBus
    from nanobot.agent.loop import AgentLoop
    from nanobot.config.schema import ExecToolConfig

    bus = MessageBus()

    if provider_type == "scripted":
        provider = _ScriptedProjectProvider(workspace)
        model = provider.get_default_model()
    else:
        provider, model = _create_real_provider(config_path)

    loop = AgentLoop(
        bus=bus,
        provider=provider,
        workspace=workspace,
        model=model,
        agent_role="project",
        scope_hint=scope_hint or f"Project scope: {workspace.name}",
        enable_message_tool=False,
    )
    return loop


async def _process_single_request(
    loop: Any,
    decision_bridge: _ProjectDecisionBridge,
    request: dict[str, Any],
) -> dict[str, Any]:
    """Process one delegation request through the agent loop."""
    req_id = request.get("id", "unknown")
    task = request.get("task", "")
    session_key = request.get("session_key", f"project_worker:{req_id}")
    local_channel = "project_worker"
    local_chat_id = str(req_id)
    decision_bridge.activate_request(
        _ActiveRequestContext(
            request_id=req_id,
            origin_channel=str(request.get("channel") or "cli"),
            origin_chat_id=str(request.get("chat_id") or "direct"),
            origin_session_key=session_key,
        )
    )

    try:
        await loop.bus.publish_inbound(
            InboundMessage(
                channel=local_channel,
                sender_id="user",
                chat_id=local_chat_id,
                content=task,
                session_key_override=session_key,
            )
        )

        # 取最新的一条，也就是混合了所有tool和sub agent结果的那条消息，即最终task的返回
        result = await _collect_request_result(
            loop,
            channel=local_channel,
            chat_id=local_chat_id,
        )
        return {"id": req_id, "success": True, "result": result}
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.error("Project worker task failed: {}", exc)
        return {"id": req_id, "success": False, "error": str(exc)}
    finally:
        decision_bridge.activate_request(None)


async def _collect_request_result(
    loop: Any,
    *,
    channel: str,
    chat_id: str,
    overall_timeout: float = 600.0,
    quiet_period: float = 2.0,
) -> str:
    """Collect the latest outbound message for a request, allowing follow-up subagent results to arrive."""
    event_loop = asyncio.get_running_loop()
    deadline = event_loop.time() + overall_timeout
    latest_content: str | None = None

    while event_loop.time() < deadline:
        message = await asyncio.wait_for(
            loop.bus.consume_outbound(),
            timeout=max(0.1, deadline - event_loop.time()),
        )
        if message.channel != channel or message.chat_id != chat_id:
            continue

        latest_content = message.content
        quiet_deadline = min(deadline, event_loop.time() + quiet_period)
        while event_loop.time() < quiet_deadline:
            try:
                next_message = await asyncio.wait_for(
                    loop.bus.consume_outbound(),
                    timeout=max(0.1, quiet_deadline - event_loop.time()),
                )
            except asyncio.TimeoutError:
                return latest_content

            if next_message.channel == channel and next_message.chat_id == chat_id:
                latest_content = next_message.content
                quiet_deadline = min(deadline, event_loop.time() + quiet_period)

        return latest_content

    raise TimeoutError(f"Timed out waiting for project worker response on {channel}:{chat_id}")


async def _run_worker_loop(
    project_path: Path,
    config_path: str | None,
    scope_hint: str | None,
    provider_type: str = "litellm",
) -> None:
    """Main worker loop: read requests from stdin, process, write responses to stdout."""
    loop = _create_agent_loop(project_path, config_path, scope_hint, provider_type=provider_type)
    writer = sys.stdout
    request_queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
    decision_bridge = _ProjectDecisionBridge(project=project_path.relative_to(project_path.parents[1]).as_posix(), writer=writer)
    loop.tools.register(_RequestUserDecisionTool(decision_bridge))
    loop_task = asyncio.create_task(loop.run())
    logger.info(
        "Project worker ready for {} (scope: {}, provider: {})",
        project_path, scope_hint or "none", provider_type,
    )

    async def _read_stdin_forever() -> None:
        while True:
            line = await asyncio.get_event_loop().run_in_executor(None, sys.stdin.readline)
            if not line:
                await request_queue.put(None)
                logger.info("Project worker received EOF, shutting down")
                return

            line = line.strip()
            if not line:
                continue

            try:
                payload = json.loads(line)
            except json.JSONDecodeError as e:
                logger.error("Invalid JSON from stdin: {}", e)
                continue

            if str(payload.get("type") or "") == "decision_response":
                if not decision_bridge.resolve_decision(payload):
                    logger.warning("Ignoring orphan decision response for {}", payload.get("decision_id"))
                continue

            await request_queue.put(payload)

    reader_task = asyncio.create_task(_read_stdin_forever())

    try:
        while True:
            request = await request_queue.get()
            if request is None:
                break

            # wait random seconds to simulate variable processing time and increase chance of concurrent messages in tests (from 3s to 5s)
            await asyncio.sleep(random.uniform(3, 5))

            response = await _process_single_request(loop, decision_bridge, request)
            writer.write(json.dumps(response, ensure_ascii=False) + "\n")
            writer.flush()
    finally:
        reader_task.cancel()
        decision_bridge.fail_all("Project worker shutting down")
        loop.stop()
        await asyncio.gather(loop_task, reader_task, return_exceptions=True)


def main() -> None:
    """Entry point for the project worker subprocess."""
    import argparse

    parser = argparse.ArgumentParser(description="Nanobot project agent worker")
    parser.add_argument("--config-path", default=None, help="Path to nanobot config file")
    parser.add_argument("--workspace", required=True, help="Workspace root directory")
    parser.add_argument("--project", required=True, help="Project subdirectory (relative to workspace)")
    parser.add_argument("--scope-hint", default=None, help="Scope hint for agent context")
    parser.add_argument(
        "--provider-type", default="litellm", choices=["litellm", "scripted"],
        help="Provider type (litellm=real LLM, scripted=test mock)",
    )
    args = parser.parse_args()

    workspace = Path(args.workspace).resolve()
    project_rel = args.project.strip().replace("\\", "/").strip("/")
    project_path = (workspace / project_rel).resolve()

    if not project_path.is_dir():
        print(json.dumps({
            "id": "init",
            "success": False,
            "error": f"Project directory not found: {project_path}",
        }))
        sys.exit(1)

    scope_hint = args.scope_hint or f"Project scope: {project_rel}"

    try:
        asyncio.run(_run_worker_loop(
            project_path, args.config_path, scope_hint,
            provider_type=args.provider_type,
        ))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
