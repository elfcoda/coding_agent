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
from pathlib import Path
from typing import Any

from loguru import logger
from numpy import random

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
    request: dict[str, Any],
) -> dict[str, Any]:
    """Process one delegation request through the agent loop."""
    req_id = request.get("id", "unknown")
    task = request.get("task", "")
    session_key = request.get("session_key", f"project_worker:{req_id}")
    local_channel = "project_worker"
    local_chat_id = str(req_id)

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


async def _collect_request_result(
    loop: Any,
    *,
    channel: str,
    chat_id: str,
    overall_timeout: float = 60.0,
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
    loop_task = asyncio.create_task(loop.run())
    logger.info(
        "Project worker ready for {} (scope: {}, provider: {})",
        project_path, scope_hint or "none", provider_type,
    )

    writer = sys.stdout

    try:
        while True:
            # wait random seconds to simulate variable processing time and increase chance of concurrent messages in tests (from 10s to 20s)
            await asyncio.sleep(random.uniform(10, 20))

            line = await asyncio.get_event_loop().run_in_executor(None, sys.stdin.readline)
            if not line:
                logger.info("Project worker received EOF, shutting down")
                break

            line = line.strip()
            if not line:
                continue

            try:
                request = json.loads(line)
            except json.JSONDecodeError as e:
                logger.error("Invalid JSON from stdin: {}", e)
                continue

            response = await _process_single_request(loop, request)
            writer.write(json.dumps(response, ensure_ascii=False) + "\n")
            writer.flush()
    finally:
        loop.stop()
        await asyncio.gather(loop_task, return_exceptions=True)


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
