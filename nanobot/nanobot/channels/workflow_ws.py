"""Workflow event WebSocket output channel for orchestration UIs."""

from __future__ import annotations

import asyncio
import json
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from loguru import logger

from nanobot.bus.events import InboundMessage, OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.schema import WorkflowWSConfig


@dataclass(slots=True)
class _ClientSubscription:
    event_types: set[str]


class WorkflowWSChannel(BaseChannel):
    """Expose workflow outbound events over a dedicated WebSocket endpoint."""

    name = "workflow"

    def __init__(self, config: WorkflowWSConfig, bus: MessageBus):
        super().__init__(config, bus)
        self.config: WorkflowWSConfig = config
        self._server: Any = None
        self._stop_event = asyncio.Event()
        self._clients: dict[Any, _ClientSubscription] = {}
        self._clients_lock = asyncio.Lock()
        self._events_lock = asyncio.Lock()
        self._event_cursor = 0
        max_events = max(100, int(self.config.replay_buffer_size))
        self._event_buffer: deque[dict[str, Any]] = deque(maxlen=max_events)

    async def start(self) -> None:
        """Start workflow WebSocket server and keep serving until stopped."""
        import websockets

        self._running = True
        self._stop_event.clear()

        async def _handler(websocket: Any) -> None:
            await self._handle_client(websocket)

        logger.info(
            f"Workflow WS channel listening on ws://{self.config.host}:{self.config.port}{self.config.path}"
        )
        self._server = await websockets.serve(
            _handler,
            self.config.host,
            self.config.port,
            ping_interval=20,
            ping_timeout=20,
        )

        await self._stop_event.wait()

    async def stop(self) -> None:
        """Stop workflow WebSocket server and disconnect clients."""
        self._running = False
        self._stop_event.set()

        async with self._clients_lock:
            clients = list(self._clients.keys())
            self._clients.clear()

        for websocket in clients:
            try:
                await websocket.close(code=1001, reason="workflow ws stopping")
            except Exception:
                pass

        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def send(self, msg: OutboundMessage) -> None:
        """Broadcast one workflow outbound message to connected WebSocket clients."""
        if not self._running:
            return

        try:
            payload = json.loads(msg.content)
        except Exception:
            payload = {"type": "workflow.raw", "payload": {"content": msg.content}}

        event_type = str(msg.metadata.get("event_type") or payload.get("type") or "workflow.unknown")
        envelope = {
            "event_id": f"wf-{datetime.utcnow().timestamp():.6f}",
            "ts": datetime.utcnow().isoformat() + "Z",
            "type": event_type,
            "payload": payload.get("payload", payload),
        }

        async with self._events_lock:
            self._event_cursor += 1
            envelope["cursor"] = self._event_cursor
            self._event_buffer.append(dict(envelope))

        body = json.dumps(envelope, ensure_ascii=False)
        to_remove: list[Any] = []

        async with self._clients_lock:
            for websocket, subscription in self._clients.items():
                if subscription.event_types and event_type not in subscription.event_types:
                    continue
                try:
                    await websocket.send(body)
                except Exception:
                    to_remove.append(websocket)

            for websocket in to_remove:
                self._clients.pop(websocket, None)

    async def _handle_client(self, websocket: Any) -> None:
        """Register one client and process lightweight subscribe/ping messages."""
        try:
            path = getattr(websocket, "path", "")
        except Exception:
            path = ""
        if path and path != self.config.path:
            await websocket.close(code=1008, reason=f"invalid path: expected {self.config.path}")
            return

        async with self._clients_lock:
            self._clients[websocket] = _ClientSubscription(event_types=set())

        try:
            await websocket.send(
                json.dumps(
                    {
                        "type": "workflow.connected",
                        "payload": {
                            "path": self.config.path,
                            "latest_cursor": await self._latest_cursor(),
                            "hint": "Send {'type':'subscribe','event_types':['workflow.scheduler.tick'],'since_cursor':123} to resume from a cursor",
                        },
                    },
                    ensure_ascii=False,
                )
            )

            async for raw in websocket:
                await self._handle_client_command(websocket, raw)
        except Exception:
            pass
        finally:
            async with self._clients_lock:
                self._clients.pop(websocket, None)

    async def _handle_client_command(self, websocket: Any, raw: str) -> None:
        """Process client-side websocket command packets."""
        try:
            command = json.loads(raw)
        except Exception:
            await websocket.send(json.dumps({"type": "workflow.error", "payload": {"error": "invalid_json"}}))
            return

        kind = str(command.get("type") or "").strip().lower()

        if kind == "ping":
            await websocket.send(json.dumps({"type": "pong", "payload": {"ts": datetime.utcnow().isoformat() + "Z"}}))
            return

        if kind in ("inbound", "publish"):
            """前端通过 WS 发送消息回 bus，由 core manager 处理。"""
            payload = command.get("payload", {})
            content = str(payload.get("content") or command.get("content") or "")
            metadata = dict(payload.get("metadata") or command.get("metadata") or {})
            channel = str(payload.get("channel") or command.get("channel") or "workflow")
            sender_id = str(payload.get("sender_id") or command.get("sender_id") or "ws_client")
            chat_id = str(payload.get("chat_id") or command.get("chat_id") or f"ws:{id(websocket)}")
            await self.bus.publish_inbound(InboundMessage(
                channel=channel,
                sender_id=sender_id,
                chat_id=chat_id,
                content=content,
                metadata=metadata,
            ))
            await websocket.send(json.dumps({
                "type": "workflow.inbound.ack",
                "payload": {"ok": True, "channel": channel, "char_len": len(content)},
            }))
            return

        if kind == "subscribe":
            values = command.get("event_types", [])
            if not isinstance(values, list):
                await websocket.send(
                    json.dumps({"type": "workflow.error", "payload": {"error": "event_types_must_be_list"}})
                )
                return

            since_cursor_raw = command.get("since_cursor", 0)
            try:
                since_cursor = max(0, int(since_cursor_raw))
            except (TypeError, ValueError):
                await websocket.send(
                    json.dumps({"type": "workflow.error", "payload": {"error": "since_cursor_must_be_int"}})
                )
                return

            event_types = {str(item).strip() for item in values if str(item).strip()}
            subscription = _ClientSubscription(event_types=event_types)
            async with self._clients_lock:
                if websocket in self._clients:
                    self._clients[websocket] = subscription

            replayed = await self._replay_events(websocket, subscription, since_cursor)
            latest_cursor = await self._latest_cursor()

            await websocket.send(
                json.dumps(
                    {
                        "type": "workflow.subscribed",
                        "payload": {
                            "event_types": sorted(event_types),
                            "since_cursor": since_cursor,
                            "replayed": replayed,
                            "latest_cursor": latest_cursor,
                        },
                    },
                    ensure_ascii=False,
                )
            )
            return

        if kind == "resume":
            await self._handle_client_command(
                websocket,
                json.dumps(
                    {
                        "type": "subscribe",
                        "event_types": command.get("event_types", []),
                        "since_cursor": command.get("since_cursor", 0),
                    },
                    ensure_ascii=False,
                ),
            )
            return

        await websocket.send(
            json.dumps(
                {
                    "type": "workflow.error",
                    "payload": {"error": "unsupported_command", "command": kind},
                },
                ensure_ascii=False,
            )
        )

    async def _latest_cursor(self) -> int:
        async with self._events_lock:
            return self._event_cursor

    async def _replay_events(self, websocket: Any, subscription: _ClientSubscription, since_cursor: int) -> int:
        """Replay buffered events newer than cursor for reconnect/resume clients."""
        async with self._events_lock:
            replay = [dict(item) for item in self._event_buffer if int(item.get("cursor", 0)) > since_cursor]

        sent = 0
        for envelope in replay:
            event_type = str(envelope.get("type") or "")
            if subscription.event_types and event_type not in subscription.event_types:
                continue
            await websocket.send(json.dumps(envelope, ensure_ascii=False))
            sent += 1
        return sent
