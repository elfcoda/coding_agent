"""Workflow event WebSocket output channel for orchestration UIs."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from loguru import logger

from nanobot.bus.events import OutboundMessage
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
                            "hint": "Send {'type':'subscribe','event_types':['workflow.scheduler.tick']} to filter events",
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

        if kind == "subscribe":
            values = command.get("event_types", [])
            if not isinstance(values, list):
                await websocket.send(
                    json.dumps({"type": "workflow.error", "payload": {"error": "event_types_must_be_list"}})
                )
                return

            event_types = {str(item).strip() for item in values if str(item).strip()}
            async with self._clients_lock:
                if websocket in self._clients:
                    self._clients[websocket] = _ClientSubscription(event_types=event_types)

            await websocket.send(
                json.dumps(
                    {
                        "type": "workflow.subscribed",
                        "payload": {"event_types": sorted(event_types)},
                    },
                    ensure_ascii=False,
                )
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
