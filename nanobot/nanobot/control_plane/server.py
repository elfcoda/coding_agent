"""Minimal control plane HTTP API for multi-agent orchestration demos."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from nanobot.agent.core_manager import CoreAgentManager


class WorkflowManageRequest(BaseModel):
    """Generic workflow operation request."""

    entity: str = Field(description="work_item | contract | dependency_edge | decision | scheduler")
    action: str = Field(description="create | get | list | update | delete | tick")
    record_id: str | None = Field(default=None)
    fields: dict[str, Any] = Field(default_factory=dict)
    filters: dict[str, Any] = Field(default_factory=dict)
    limit: int = Field(default=100, ge=1, le=500)


class DelegateProjectRequest(BaseModel):
    """Single project delegation request."""

    project: str
    task: str
    session_key: str = "control:direct"
    channel: str = "workflow"
    chat_id: str = "control-plane"


class DelegateBatchItem(BaseModel):
    """One item for batch project delegation."""

    project: str
    task: str


class DelegateBatchRequest(BaseModel):
    """Batch project delegation request."""

    items: list[DelegateBatchItem]
    session_key: str = "control:batch"
    channel: str = "workflow"
    chat_id: str = "control-plane"


class ProjectAttributesRequest(BaseModel):
    """Runtime attributes attached to one project agent."""

    attributes: dict[str, Any] = Field(default_factory=dict)


def _json_or_text(value: str) -> Any:
    try:
        return json.loads(value)
    except Exception:
        return {"raw": value}


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_control_plane_app(manager: CoreAgentManager) -> FastAPI:
    """Create a small HTTP control plane over CoreAgentManager."""

    app = FastAPI(
        title="nanobot-control-plane",
        version="0.1.0",
        description="Demo control plane for multi-agent orchestration",
    )

    @app.get("/api/control/health")
    async def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "ts": _now_utc_iso(),
        }

    @app.get("/api/control/snapshot")
    async def snapshot(limit: int = 200) -> dict[str, Any]:
        safe_limit = max(1, min(limit, 1000))
        return {
            "ts": _now_utc_iso(),
            "project_scopes": manager.list_project_scopes(),
            "project_registry": [asdict(item) for item in manager.get_project_registry()],
            "work_items": [asdict(item) for item in manager.list_work_items(limit=safe_limit)],
            "contracts": [asdict(item) for item in manager.list_contracts(limit=safe_limit)],
            "dependency_edges": [asdict(item) for item in manager.list_dependency_edges(limit=safe_limit)],
            "decisions": [asdict(item) for item in manager.list_decisions(limit=safe_limit)],
        }

    @app.post("/api/control/workflow/manage")
    async def manage_workflow(request: WorkflowManageRequest) -> dict[str, Any]:
        try:
            result = manager.manage_workflow_state(
                entity=request.entity,
                action=request.action,
                record_id=request.record_id,
                fields=request.fields,
                filters=request.filters,
                limit=request.limit,
            )
            return {
                "ok": True,
                "result": _json_or_text(result),
            }
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/api/control/delegation/project")
    async def delegate_project(request: DelegateProjectRequest) -> dict[str, Any]:
        try:
            output = await manager.delegate_project_task(
                project=request.project,
                task=request.task,
                session_key=request.session_key,
                channel=request.channel,
                chat_id=request.chat_id,
            )
            return {
                "ok": True,
                "project": request.project,
                "output": output,
            }
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/api/control/delegation/batch")
    async def delegate_batch(request: DelegateBatchRequest) -> dict[str, Any]:
        try:
            batch_id_msg = await manager.delegate_projects_batch(
                items=[item.model_dump() for item in request.items],
                session_key=request.session_key,
                channel=request.channel,
                chat_id=request.chat_id,
            )
            return {
                "ok": True,
                "message": batch_id_msg,
            }
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/control/contracts/merge-audit")
    async def contract_merge_audit_list(
        provider_module: str | None = None,
        consumer_module: str | None = None,
        interface_name: str | None = None,
        status: str | None = None,
        limit: int = 200,
        merged_only: bool = True,
    ) -> dict[str, Any]:
        safe_limit = max(1, min(limit, 1000))
        filters: dict[str, Any] = {}
        if provider_module:
            filters["provider_module"] = provider_module
        if consumer_module:
            filters["consumer_module"] = consumer_module
        if interface_name:
            filters["interface_name"] = interface_name
        if status:
            filters["status"] = status

        records = manager.list_contract_merge_audits(
            filters=filters,
            limit=safe_limit,
            merged_only=merged_only,
        )
        return {
            "ok": True,
            "count": len(records),
            "records": records,
        }

    @app.get("/api/control/contracts/{contract_id}/merge-audit")
    async def contract_merge_audit_get(contract_id: str) -> dict[str, Any]:
        record = manager.get_contract_merge_audit(contract_id)
        if record is None:
            raise HTTPException(status_code=404, detail=f"Unknown contract id: {contract_id}")
        return {
            "ok": True,
            "record": record,
        }

    @app.get("/api/control/delegation/batch/{batch_id}")
    async def delegation_batch_status(batch_id: str) -> dict[str, Any]:
        try:
            status_text = manager.get_batch_delegation_status(batch_id)
            return {
                "ok": True,
                "batch_id": batch_id,
                "status": status_text,
            }
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/api/control/projects/{project}/attributes")
    async def get_project_attributes(project: str) -> dict[str, Any]:
        try:
            return {
                "ok": True,
                "project": project,
                "attributes": manager.get_project_runtime_attributes(project),
            }
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.put("/api/control/projects/{project}/attributes")
    async def set_project_attributes(project: str, request: ProjectAttributesRequest) -> dict[str, Any]:
        try:
            updated = manager.set_project_runtime_attributes(project, request.attributes)
            return {
                "ok": True,
                "project": project,
                "attributes": updated,
            }
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    return app
