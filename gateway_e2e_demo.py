"""
Nanobot E2E Demo Gateway — 用 Scripted Provider 模拟 LLM，通过 Control Plane API + Workflow WS 给前端连接。

启动:
    python gateway_e2e_demo.py

前端连接:
    - Control Plane REST API:  http://localhost:18790/api/control/health
    - Workflow WebSocket:      ws://localhost:18791/workflow
    - Demo Web UI:             http://localhost:18790/

流程:
    1. 前端调用 POST /api/control/delegation/batch 批量委派三个模块的任务
    2. Project worker (scripted) 自动编辑 test_code/module*/api.py
    3. 需要决策时会通过 Workflow WS 推送 decision_request 事件
    4. 前端通过 POST /api/control/commands/decisions/submit 提交决策
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import uuid
from pathlib import Path
from typing import Any

# Windows GBK 编码兼容
os.environ.setdefault("PYTHONIOENCODING", "utf-8")

# ── 把项目根目录和 nanobot 加入路径 ──────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "nanobot"))

# ── 导入 nanobot 组件 ────────────────────────────────────────────────
from nanobot.bus.events import InboundMessage, OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.providers.base import LLMProvider, LLMResponse, ToolCallRequest
from nanobot.workflow import WorkflowStore


# ═══════════════════════════════════════════════════════════════════════
# ScriptedDelegationProvider — 模拟 Core Agent 的 LLM
# ═══════════════════════════════════════════════════════════════════════
class ScriptedDelegationProvider(LLMProvider):
    """模拟 LLM: 收到用户消息后调用 delegate_project_task 委派三个模块。"""

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
            return LLMResponse(content="Use the tools you have available.")

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


# ═══════════════════════════════════════════════════════════════════════
# 前端 Demo HTML
# ═══════════════════════════════════════════════════════════════════════
DEMO_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Nanobot E2E Demo</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         background: #0f172a; color: #e2e8f0; padding: 2rem; }
  h1 { color: #38bdf8; margin-bottom: 1rem; }
  h2 { color: #94a3b8; font-size: 1rem; margin: 1.5rem 0 0.5rem; }
  .panel { background: #1e293b; border-radius: 8px; padding: 1.5rem; margin-bottom: 1rem; }
  button { background: #2563eb; color: white; border: none; border-radius: 6px;
           padding: 0.6rem 1.2rem; cursor: pointer; font-size: 0.9rem; margin-right: 0.5rem; }
  button:hover { background: #1d4ed8; }
  button:disabled { opacity: 0.5; cursor: not-allowed; }
  .btn-green { background: #16a34a; }
  .btn-green:hover { background: #15803d; }
  .btn-red { background: #dc2626; }
  .btn-red:hover { background: #b91c1c; }
  .log { background: #0f172a; border: 1px solid #334155; border-radius: 6px;
         padding: 1rem; max-height: 400px; overflow-y: auto; font-family: monospace; font-size: 0.85rem; }
  .log-entry { padding: 0.25rem 0; border-bottom: 1px solid #1e293b; }
  .log-entry:last-child { border-bottom: none; }
  .event-msg { color: #e2e8f0; }
  .event-warn { color: #fbbf24; }
  .event-good { color: #4ade80; }
  .event-decision { color: #f472b6; }
  .status-badge { display: inline-block; padding: 0.2rem 0.6rem; border-radius: 999px;
                  font-size: 0.75rem; font-weight: 600; }
  .status-ok { background: #166534; color: #86efac; }
  .status-pending { background: #713f12; color: #fde047; }
  pre { white-space: pre-wrap; word-break: break-all; }
  .flex { display: flex; gap: 1rem; align-items: center; }
  input[type=text] { flex: 1; background: #0f172a; border: 1px solid #334155; border-radius: 6px;
                     padding: 0.6rem; color: #e2e8f0; font-family: monospace; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }
  @media (max-width: 768px) { .grid { grid-template-columns: 1fr; } }
</style>
</head>
<body>
<h1>🐈 Nanobot E2E Demo</h1>
<p style="color:#94a3b8;margin-bottom:1.5rem;">
  Scripted Provider 模拟 LLM · 通过 Control Plane API 与 Workflow WS 通信
</p>

<div class="grid">
  <div>
    <div class="panel">
      <h2>🚀 控制面板</h2>
      <button id="btnHealth">Health Check</button>
      <button id="btnSnapshot">取快照</button>
      <button id="btnBatch" class="btn-green">批量委派 (module1/2/3)</button>
      <button id="btnDecisionQueue" class="btn-green">查看决策队列</button>
      <button id="btnPendingDecisions" class="btn-green">查看等待中的决策</button>
      <br><br>
      <div class="flex">
        <input type="text" id="decisionIdInput" placeholder="decision_id" style="font-size:0.8rem">
        <button id="btnSubmitDecision" class="btn-red">提交决策 "rest"</button>
      </div>
      <div id="pendingDecisionsList" style="margin-top:0.5rem;font-size:0.8rem;color:#94a3b8;"></div>
    </div>

    <div class="panel">
      <h2>📋 返回数据</h2>
      <pre id="responseData" style="font-size:0.8rem;max-height:300px;overflow:auto;">等待操作...</pre>
    </div>
  </div>

  <div>
    <div class="panel">
      <h2>🔌 Workflow WebSocket 事件流</h2>
      <div style="margin-bottom:0.5rem;">
        <span class="status-badge status-pending" id="wsStatus">未连接</span>
      </div>
      <div class="log" id="eventLog"></div>
    </div>
  </div>
</div>

<script>
// ── WebSocket 连接 ───────────────────────────────────────────────
const wsUrl = `ws://${location.hostname}:18791/workflow`;
const wsStatus = document.getElementById('wsStatus');
const eventLog = document.getElementById('eventLog');

let ws;

function connectWS() {
  ws = new WebSocket(wsUrl);
  ws.onopen = () => {
    wsStatus.className = 'status-badge status-ok';
    wsStatus.textContent = '已连接';
    addLog('event-good', `[WS] 已连接到 ${wsUrl}`);
  };
  ws.onclose = () => {
    wsStatus.className = 'status-badge status-pending';
    wsStatus.textContent = '已断开 (2s 后重连)';
    addLog('event-warn', '[WS] 连接断开，2s 后重连...');
    setTimeout(connectWS, 2000);
  };
  ws.onerror = () => {
    wsStatus.className = 'status-badge status-pending';
    wsStatus.textContent = '连接错误';
    addLog('event-warn', '[WS] 连接错误');
  };
  ws.onmessage = (e) => {
    try {
      const data = JSON.parse(e.data);
      const ts = data.ts || new Date().toISOString();
      const type = data.type || 'unknown';
      const cls = type.includes('decision') ? 'event-decision'
               : type.includes('completed') ? 'event-good'
               : type.includes('started') ? 'event-msg'
               : 'event-msg';
      const payload = JSON.stringify(data.payload || data, null, 2);
      addLog(cls, `[${ts.slice(11,19)}] ${type}\n${payload}`);
    } catch {
      addLog('event-msg', `[WS] raw: ${e.data}`);
    }
  };
}

function addLog(cls, text) {
  const div = document.createElement('div');
  div.className = `log-entry ${cls}`;
  div.innerHTML = `<pre>${text}</pre>`;
  eventLog.appendChild(div);
  eventLog.scrollTop = eventLog.scrollHeight;
  // 限制日志条数
  while (eventLog.children.length > 200) {
    eventLog.removeChild(eventLog.firstChild);
  }
}

connectWS();

// ── API 调用 ────────────────────────────────────────────────────
const API_BASE = `http://${location.hostname}:18790`;

async function apiCall(method, path, body) {
  const opts = { method, headers: { 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  try {
    const resp = await fetch(`${API_BASE}${path}`, opts);
    const data = await resp.json();
    document.getElementById('responseData').textContent =
      JSON.stringify(data, null, 2);
    return data;
  } catch (err) {
    document.getElementById('responseData').textContent =
      `Error: ${err.message}`;
    return null;
  }
}

// ── 按钮绑定 ────────────────────────────────────────────────────
document.getElementById('btnHealth').onclick = () =>
  apiCall('GET', '/api/control/health');

document.getElementById('btnSnapshot').onclick = () =>
  apiCall('GET', '/api/control/snapshot?limit=50');

document.getElementById('btnBatch').onclick = () =>
  apiCall('POST', '/api/control/delegation/batch', {
    items: [
      { project: 'test_code/module1', task: 'Add a simple public interface function to the existing api.py file.' },
      { project: 'test_code/module2', task: 'Add a simple public interface function to the existing api.py file.' },
      { project: 'test_code/module3', task: 'Add a simple public interface function to the existing api.py file.' },
    ]
  }).then(r => {
    if (r && r.ok) addLog('event-good', `[API] 批量委派已提交 ✓`);
  });

document.getElementById('btnDecisionQueue').onclick = () =>
  apiCall('GET', '/api/control/decisions/queue');

document.getElementById('btnPendingDecisions').onclick = async () => {
  const data = await apiCall('GET', '/api/control/decisions/pending-project-decisions');
  const list = document.getElementById('pendingDecisionsList');
  if (!data || !data.items || data.items.length === 0) {
    list.innerHTML = '<span style="color:#94a3b8;">暂无等待中的决策</span>';
    return;
  }
  let html = '';
  for (const item of data.items) {
    html += `<div style="padding:0.3rem 0;border-bottom:1px solid #334155;">
      <b>${item.project}</b>: ${item.prompt}<br>
      <span style="color:#94a3b8;font-size:0.75rem;">
        选项: ${item.options.join(', ')} |
        decision_id: <code>${item.decision_id}</code>
      </span>
    </div>`;
  }
  list.innerHTML = html;
  // 自动填入第一个 decision_id
  if (data.items.length > 0) {
    document.getElementById('decisionIdInput').value = data.items[0].decision_id;
    addLog('event-msg', `[API] 自动填入 decision_id: ${data.items[0].decision_id}`);
  }
};

document.getElementById('btnSubmitDecision').onclick = async () => {
  const decisionId = document.getElementById('decisionIdInput').value.trim();
  if (!decisionId) {
    addLog('event-warn', '[API] 请先点击"查看等待中的决策"获取 decision_id');
    return;
  }
  const result = await apiCall('POST', '/api/control/commands/decisions/resolve-project-decision', {
    decision_id: decisionId,
    content: 'rest',
  });
  if (result && result.ok) {
    addLog('event-good', `[API] 决策 "${decisionId}" -> "rest" 已发送 ✓ (${result.project})`);
  } else if (result && result.detail) {
    addLog('event-warn', `[API] 提交失败: ${result.detail}`);
  }
};
</script>
</body>
</html>"""


# ═══════════════════════════════════════════════════════════════════════
# 主入口
# ═══════════════════════════════════════════════════════════════════════
async def main():
    import uvicorn
    from nanobot.agent.core_manager import CoreAgentManager
    from nanobot.control_plane import create_control_plane_app
    from nanobot.channels.workflow_ws import WorkflowWSChannel
    from nanobot.config.schema import WorkflowWSConfig, ControlPlaneConfig
    from nanobot.bus.queue import MessageBus
    from contextlib import asynccontextmanager
    from fastapi import FastAPI, HTTPException
    from fastapi.responses import HTMLResponse

    repo_root = PROJECT_ROOT
    workflow_db = repo_root / "data" / "cache" / f"gateway_demo_{uuid.uuid4().hex[:8]}.db"
    workflow_db.parent.mkdir(parents=True, exist_ok=True)

    bus = MessageBus()
    provider = ScriptedDelegationProvider(repo_root)
    workflow_store = WorkflowStore(workflow_db)

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
    manager._worker_provider_type = "scripted"

    # ── 保存原始文件，退出时恢复 ──────────────────────────────────
    test_files = {
        name: repo_root / "test_code" / name / "api.py"
        for name in ("module1", "module2", "module3")
    }
    original_contents = {}
    for name, path in test_files.items():
        if path.exists():
            original_contents[name] = path.read_text(encoding="utf-8")

    # ── 创建 Control Plane FastAPI App ────────────────────────────
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        # shutdown: restore files
        for name, path in test_files.items():
            if name in original_contents:
                path.write_text(original_contents[name], encoding="utf-8")
        manager.stop()

    control_app = create_control_plane_app(manager, ControlPlaneConfig())
    control_app.router.lifespan_context = lifespan

    # 挂载 Demo 前端页面
    @control_app.get("/")
    async def demo_page():
        return HTMLResponse(DEMO_HTML)

    # ── 新增: 前端提交 project agent 决策的入口 ─────────────────
    # 项目 worker 请求用户决策时, decision 存在 manager._pending_project_decisions 里。
    # 前端需要把 decision_id + 用户选择 POST 到这里, 然后 gateway 通过 bus 把消息
    # 送回 core manager 的 run loop, 后者调用 _maybe_handle_project_decision_reply
    # 把选择送回等待中的 project worker 子进程。

    @control_app.get("/api/control/decisions/pending-project-decisions")
    async def pending_project_decisions():
        """返回所有当前等待前端决策的 project agent 决策列表。"""
        items = []
        for decision_id, state in manager._pending_project_decisions.items():
            items.append({
                "decision_id": decision_id,
                "project": state.project,
                "prompt": state.prompt,
                "options": state.options,
                "session_key": state.session_key,
            })
        return {"ok": True, "count": len(items), "items": items}

    @control_app.post("/api/control/commands/decisions/resolve-project-decision")
    async def resolve_project_decision(body: dict):
        decision_id = str(body.get("decision_id") or "").strip()
        content = str(body.get("content") or "")
        if not decision_id:
            raise HTTPException(status_code=400, detail="decision_id is required")
        # 先检查这个 decision_id 是否还在等待中
        pending = manager._pending_project_decisions.get(decision_id)
        if pending is None:
            raise HTTPException(status_code=404, detail=f"decision_id '{decision_id}' not found in pending decisions")
        await bus.publish_inbound(InboundMessage(
            channel="control_api",
            sender_id="gateway",
            chat_id="project_decision",
            content=content,
            metadata={"project_decision_id": decision_id},
        ))
        return {
            "ok": True,
            "decision_id": decision_id,
            "forwarded": True,
            "project": pending.project,
            "prompt": pending.prompt,
            "options": pending.options,
        }

    # ── 启动 Workflow WebSocket ───────────────────────────────────
    ws_config = WorkflowWSConfig(enabled=True, host="127.0.0.1", port=18791)
    ws_channel = WorkflowWSChannel(ws_config, bus)

    # ── 启动所有组件 ─────────────────────────────────────────────
    print("=" * 60)
    print("  Nanobot E2E Demo Gateway")
    print("=" * 60)
    print(f"  Control Plane API:  http://localhost:18790/api/control/health")
    print(f"  Demo Web UI:         http://localhost:18790/")
    print(f"  Workflow WS:         ws://localhost:18791/workflow")
    print(f"  Workspace:           {repo_root}")
    print(f"  Test files:          test_code/module*/api.py")
    print(f"  Provider:            Scripted (no API key needed)")
    print("-" * 60)

    async def restore_on_error():
        for name, path in test_files.items():
            if name in original_contents:
                path.write_text(original_contents[name], encoding="utf-8")

    try:
        await asyncio.gather(
            manager.run(),
            ws_channel.start(),
            uvicorn.Server(uvicorn.Config(control_app, host="0.0.0.0", port=18790, log_level="info")).serve(),
        )
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    except OSError as exc:
        print(f"\n[Error] Port bind failed: {exc}")
        await restore_on_error()
        raise
    except Exception:
        await restore_on_error()
        raise


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 再见!")
