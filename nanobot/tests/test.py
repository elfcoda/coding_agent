import json
import asyncio
from loguru import logger
import websockets


async def fin():
    """订阅 gateway Workflow WS, 持续接收并打印所有事件。

    前置条件:
      nanobot gateway --port 18790  已在后台运行
      workflow_ws channel 已启用 (ws://127.0.0.1:18791/workflow)
    """

    uri = "ws://127.0.0.1:18791/workflow"

    async with websockets.connect(uri, ping_interval=None) as ws:
        # 1) 确认连接成功, 收到 connected 事件
        connected_raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
        connected = json.loads(connected_raw)
        assert connected["type"] == "workflow.connected", f"Expected connected, got {connected['type']}"
        logger.info("WS connected OK: cursor=%s", connected["payload"]["latest_cursor"])

        # 2) 发送一条 inbound 消息 (模拟用户触发任务委派)
        await ws.send(json.dumps({
            "type": "inbound",
            "channel": "e2e",
            "sender_id": "tester",
            "chat_id": "core-run-flow",
            "content": "Use the three fixed project agents under test_code to add one simple interface to each module.",
        }))

        # 3) 持续接收所有事件, 不断开
        logger.info("Listening for events (Ctrl+C to stop)...")
        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=3600.0)
                msg = json.loads(raw)
                logger.info("msg: %s", json.dumps(msg, indent=2, ensure_ascii=False))
            except asyncio.TimeoutError:
                logger.warning("No events for 1h, keeping connection alive...")
            except websockets.ConnectionClosed:
                logger.warning("Connection closed, exiting.")
                break


if __name__ == "__main__":
    logger.info("Starting WS channel test...")
    asyncio.run(fin())
