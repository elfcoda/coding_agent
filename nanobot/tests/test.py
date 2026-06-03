import json
import websockets


async def fin():
    """最小测试: 连上已启动的 gateway Workflow WS, 发一条 inbound 消息并确认收到 ack。

    前置条件:
      nanobot gateway --port 18790  已在后台运行
      workflow_ws channel 已启用 (ws://127.0.0.1:18791/workflow)
    """

    uri = "ws://127.0.0.1:18791/workflow"

    async with websockets.connect(uri) as ws:
        # 1) 确认连接成功, 收到 connected 事件
        connected_raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
        connected = json.loads(connected_raw)
        assert connected["type"] == "workflow.connected", f"Expected connected, got {connected['type']}"
        logger.info("WS connected OK: cursor=%s", connected["payload"]["latest_cursor"])

        # 2) 发送一条 inbound 消息 (模拟前端提交决策回复)
        test_decision_id = "test-ws-inbound-dummy-id"
        await ws.send(json.dumps({
            "type": "inbound",
            "content": "rest",
            "metadata": {"project_decision_id": test_decision_id},
        }))

        # # 3) 确认收到 ack
        # ack_raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
        # ack = json.loads(ack_raw)
        # assert ack["type"] == "workflow.inbound.ack", f"Expected ack, got {ack['type']}"
        # assert ack["payload"]["ok"] is True
        # logger.info("Inbound ack OK: channel=%s, char_len=%s",
        #             ack["payload"]["channel"], ack["payload"]["char_len"])

        # # 4) 再发一条 ping 确认 WS 仍正常
        # await ws.send(json.dumps({"type": "ping"}))
        # pong_raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
        # pong = json.loads(pong_raw)
        # assert pong["type"] == "pong"
        # logger.info("Ping/Pong OK")

    logger.info("test_ws_gateway_inbound PASSED")

if __name__ == "__main__":
    import asyncio
    from loguru import logger

    logger.info("Starting WS channel test...")
    asyncio.run(fin())
