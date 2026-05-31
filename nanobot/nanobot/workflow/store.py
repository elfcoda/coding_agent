"""SQLite-backed workflow persistence for work items and contracts."""

from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from nanobot.utils.helpers import ensure_dir


def _now_iso() -> str:
    return datetime.now().isoformat()


def _encode_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _decode_json(value: str | None, default: Any) -> Any:
    if not value:
        return default
    return json.loads(value)


@dataclass(slots=True)
class WorkItemRecord:
    """Persistent workflow task tracked for one module or project agent."""

    id: str
    module: str
    goal: str
    status: str = "proposed"
    priority: int = 0
    owner_agent: str = ""
    session_key: str = ""
    decision_required: bool = False
    decision_type: str = ""
    blocked_by: list[str] = field(default_factory=list)
    artifacts: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=_now_iso)
    updated_at: str = field(default_factory=_now_iso)


@dataclass(slots=True)
class ContractRecord:
    """Persistent interface contract between a provider and consumer module."""

    id: str
    provider_module: str
    consumer_module: str
    interface_name: str
    version: int = 1
    status: str = "requested"
    functions: list[dict[str, Any]] = field(default_factory=list)
    spec: dict[str, Any] = field(default_factory=dict)
    stub_path: str = ""
    implementation_path: str = ""
    work_item_id: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=_now_iso)
    updated_at: str = field(default_factory=_now_iso)


@dataclass(slots=True)
class DependencyEdgeRecord:
    """Persistent directed dependency between two work items."""

    id: str
    source_work_item_id: str
    target_work_item_id: str
    edge_type: str = "depends_on"
    status: str = "active"
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=_now_iso)
    updated_at: str = field(default_factory=_now_iso)


@dataclass(slots=True)
class DecisionRecord:
    """Persistent human or system decision attached to a work item."""

    id: str
    work_item_id: str
    decision_type: str
    status: str = "pending"
    options: list[dict[str, Any]] = field(default_factory=list)
    chosen_option: str = ""
    decider: str = ""
    rationale: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=_now_iso)
    updated_at: str = field(default_factory=_now_iso)


@dataclass(slots=True)
class MetricsSnapshotRecord:
    """Persistent observability snapshot for time-series dashboards."""

    id: str
    snapshot_type: str
    generated_at: str
    metrics: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=_now_iso)


class WorkflowStore:
    """SQLite persistence for workflow state shared across the agent system."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        ensure_dir(db_path.parent)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS work_items (
                    id TEXT PRIMARY KEY,
                    module TEXT NOT NULL,
                    goal TEXT NOT NULL,
                    status TEXT NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 0,
                    owner_agent TEXT NOT NULL DEFAULT '',
                    session_key TEXT NOT NULL DEFAULT '',
                    decision_required INTEGER NOT NULL DEFAULT 0,
                    decision_type TEXT NOT NULL DEFAULT '',
                    blocked_by_json TEXT NOT NULL DEFAULT '[]',
                    artifacts_json TEXT NOT NULL DEFAULT '[]',
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_work_items_module ON work_items(module);
                CREATE INDEX IF NOT EXISTS idx_work_items_status ON work_items(status);
                CREATE INDEX IF NOT EXISTS idx_work_items_owner_agent ON work_items(owner_agent);
                CREATE INDEX IF NOT EXISTS idx_work_items_decision_required ON work_items(decision_required);

                CREATE TABLE IF NOT EXISTS contracts (
                    id TEXT PRIMARY KEY,
                    provider_module TEXT NOT NULL,
                    consumer_module TEXT NOT NULL,
                    interface_name TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    status TEXT NOT NULL,
                    functions_json TEXT NOT NULL DEFAULT '[]',
                    spec_json TEXT NOT NULL DEFAULT '{}',
                    stub_path TEXT NOT NULL DEFAULT '',
                    implementation_path TEXT NOT NULL DEFAULT '',
                    work_item_id TEXT NOT NULL DEFAULT '',
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(provider_module, consumer_module, interface_name, version),
                    FOREIGN KEY(work_item_id) REFERENCES work_items(id) ON DELETE SET DEFAULT
                );

                CREATE INDEX IF NOT EXISTS idx_contracts_provider_module ON contracts(provider_module);
                CREATE INDEX IF NOT EXISTS idx_contracts_consumer_module ON contracts(consumer_module);
                CREATE INDEX IF NOT EXISTS idx_contracts_status ON contracts(status);
                CREATE INDEX IF NOT EXISTS idx_contracts_interface_name ON contracts(interface_name);
                CREATE INDEX IF NOT EXISTS idx_contracts_work_item_id ON contracts(work_item_id);

                CREATE TABLE IF NOT EXISTS dependency_edges (
                    id TEXT PRIMARY KEY,
                    source_work_item_id TEXT NOT NULL,
                    target_work_item_id TEXT NOT NULL,
                    edge_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(source_work_item_id, target_work_item_id, edge_type),
                    FOREIGN KEY(source_work_item_id) REFERENCES work_items(id) ON DELETE CASCADE,
                    FOREIGN KEY(target_work_item_id) REFERENCES work_items(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_dependency_edges_source_work_item_id ON dependency_edges(source_work_item_id);
                CREATE INDEX IF NOT EXISTS idx_dependency_edges_target_work_item_id ON dependency_edges(target_work_item_id);
                CREATE INDEX IF NOT EXISTS idx_dependency_edges_edge_type ON dependency_edges(edge_type);
                CREATE INDEX IF NOT EXISTS idx_dependency_edges_status ON dependency_edges(status);

                CREATE TABLE IF NOT EXISTS decisions (
                    id TEXT PRIMARY KEY,
                    work_item_id TEXT NOT NULL,
                    decision_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    options_json TEXT NOT NULL DEFAULT '[]',
                    chosen_option TEXT NOT NULL DEFAULT '',
                    decider TEXT NOT NULL DEFAULT '',
                    rationale TEXT NOT NULL DEFAULT '',
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(work_item_id) REFERENCES work_items(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_decisions_work_item_id ON decisions(work_item_id);
                CREATE INDEX IF NOT EXISTS idx_decisions_decision_type ON decisions(decision_type);
                CREATE INDEX IF NOT EXISTS idx_decisions_status ON decisions(status);

                CREATE TABLE IF NOT EXISTS metrics_snapshots (
                    id TEXT PRIMARY KEY,
                    snapshot_type TEXT NOT NULL DEFAULT 'observability',
                    generated_at TEXT NOT NULL,
                    metrics_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_metrics_snapshots_snapshot_type ON metrics_snapshots(snapshot_type);
                CREATE INDEX IF NOT EXISTS idx_metrics_snapshots_generated_at ON metrics_snapshots(generated_at);
                """
            )
            columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(contracts)").fetchall()
            }
            if "functions_json" not in columns:
                connection.execute(
                    "ALTER TABLE contracts ADD COLUMN functions_json TEXT NOT NULL DEFAULT '[]'"
                )

    def create_work_item(self, record: WorkItemRecord) -> WorkItemRecord:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO work_items (
                    id, module, goal, status, priority, owner_agent, session_key,
                    decision_required, decision_type, blocked_by_json,
                    artifacts_json, metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.id,
                    record.module,
                    record.goal,
                    record.status,
                    record.priority,
                    record.owner_agent,
                    record.session_key,
                    int(record.decision_required),
                    record.decision_type,
                    _encode_json(record.blocked_by),
                    _encode_json(record.artifacts),
                    _encode_json(record.metadata),
                    record.created_at,
                    record.updated_at,
                ),
            )
        return record

    def update_work_item(self, work_item_id: str, **changes: Any) -> WorkItemRecord:
        current = self.get_work_item(work_item_id)
        if current is None:
            raise ValueError(f"Unknown work item id: {work_item_id}")

        updated = WorkItemRecord(
            id=current.id,
            module=changes.get("module", current.module),
            goal=changes.get("goal", current.goal),
            status=changes.get("status", current.status),
            priority=changes.get("priority", current.priority),
            owner_agent=changes.get("owner_agent", current.owner_agent),
            session_key=changes.get("session_key", current.session_key),
            decision_required=changes.get("decision_required", current.decision_required),
            decision_type=changes.get("decision_type", current.decision_type),
            blocked_by=list(changes.get("blocked_by", current.blocked_by)),
            artifacts=list(changes.get("artifacts", current.artifacts)),
            metadata=dict(changes.get("metadata", current.metadata)),
            created_at=current.created_at,
            updated_at=changes.get("updated_at", _now_iso()),
        )

        with self._connect() as connection:
            connection.execute(
                """
                UPDATE work_items
                SET module = ?, goal = ?, status = ?, priority = ?, owner_agent = ?,
                    session_key = ?, decision_required = ?, decision_type = ?,
                    blocked_by_json = ?, artifacts_json = ?,
                    metadata_json = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    updated.module,
                    updated.goal,
                    updated.status,
                    updated.priority,
                    updated.owner_agent,
                    updated.session_key,
                    int(updated.decision_required),
                    updated.decision_type,
                    _encode_json(updated.blocked_by),
                    _encode_json(updated.artifacts),
                    _encode_json(updated.metadata),
                    updated.updated_at,
                    work_item_id,
                ),
            )
        return updated

    def get_work_item(self, work_item_id: str) -> WorkItemRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM work_items WHERE id = ?",
                (work_item_id,),
            ).fetchone()
        return self._row_to_work_item(row) if row else None

    def list_work_items(
        self,
        *,
        module: str | None = None,
        status: str | None = None,
        owner_agent: str | None = None,
        decision_required: bool | None = None,
        limit: int = 100,
    ) -> list[WorkItemRecord]:
        query = "SELECT * FROM work_items"
        filters: list[str] = []
        params: list[Any] = []

        if module:
            filters.append("module = ?")
            params.append(module)
        if status:
            filters.append("status = ?")
            params.append(status)
        if owner_agent:
            filters.append("owner_agent = ?")
            params.append(owner_agent)
        if decision_required is not None:
            filters.append("decision_required = ?")
            params.append(int(decision_required))
        if filters:
            query += " WHERE " + " AND ".join(filters)
        query += " ORDER BY updated_at DESC LIMIT ?"
        params.append(limit)

        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [self._row_to_work_item(row) for row in rows]

    def delete_work_item(self, work_item_id: str) -> bool:
        with self._connect() as connection:
            cursor = connection.execute("DELETE FROM work_items WHERE id = ?", (work_item_id,))
        return cursor.rowcount > 0

    def create_contract(self, record: ContractRecord) -> ContractRecord:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO contracts (
                    id, provider_module, consumer_module, interface_name, version, status,
                    functions_json, spec_json, stub_path, implementation_path, work_item_id, metadata_json,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.id,
                    record.provider_module,
                    record.consumer_module,
                    record.interface_name,
                    record.version,
                    record.status,
                    _encode_json(record.functions),
                    _encode_json(record.spec),
                    record.stub_path,
                    record.implementation_path,
                    record.work_item_id,
                    _encode_json(record.metadata),
                    record.created_at,
                    record.updated_at,
                ),
            )
        return record

    def update_contract(self, contract_id: str, **changes: Any) -> ContractRecord:
        current = self.get_contract(contract_id)
        if current is None:
            raise ValueError(f"Unknown contract id: {contract_id}")

        updated = ContractRecord(
            id=current.id,
            provider_module=changes.get("provider_module", current.provider_module),
            consumer_module=changes.get("consumer_module", current.consumer_module),
            interface_name=changes.get("interface_name", current.interface_name),
            version=changes.get("version", current.version),
            status=changes.get("status", current.status),
            functions=[dict(item) for item in changes.get("functions", current.functions)],
            spec=dict(changes.get("spec", current.spec)),
            stub_path=changes.get("stub_path", current.stub_path),
            implementation_path=changes.get("implementation_path", current.implementation_path),
            work_item_id=changes.get("work_item_id", current.work_item_id),
            metadata=dict(changes.get("metadata", current.metadata)),
            created_at=current.created_at,
            updated_at=changes.get("updated_at", _now_iso()),
        )

        with self._connect() as connection:
            connection.execute(
                """
                UPDATE contracts
                SET provider_module = ?, consumer_module = ?, interface_name = ?, version = ?,
                    status = ?, functions_json = ?, spec_json = ?, stub_path = ?, implementation_path = ?,
                    work_item_id = ?, metadata_json = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    updated.provider_module,
                    updated.consumer_module,
                    updated.interface_name,
                    updated.version,
                    updated.status,
                    _encode_json(updated.functions),
                    _encode_json(updated.spec),
                    updated.stub_path,
                    updated.implementation_path,
                    updated.work_item_id,
                    _encode_json(updated.metadata),
                    updated.updated_at,
                    contract_id,
                ),
            )
        return updated

    def get_contract(self, contract_id: str) -> ContractRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM contracts WHERE id = ?",
                (contract_id,),
            ).fetchone()
        return self._row_to_contract(row) if row else None

    def list_contracts(
        self,
        *,
        provider_module: str | None = None,
        consumer_module: str | None = None,
        interface_name: str | None = None,
        status: str | None = None,
        work_item_id: str | None = None,
        limit: int = 100,
    ) -> list[ContractRecord]:
        query = "SELECT * FROM contracts"
        filters: list[str] = []
        params: list[Any] = []

        if provider_module:
            filters.append("provider_module = ?")
            params.append(provider_module)
        if consumer_module:
            filters.append("consumer_module = ?")
            params.append(consumer_module)
        if interface_name:
            filters.append("interface_name = ?")
            params.append(interface_name)
        if status:
            filters.append("status = ?")
            params.append(status)
        if work_item_id:
            filters.append("work_item_id = ?")
            params.append(work_item_id)
        if filters:
            query += " WHERE " + " AND ".join(filters)
        query += " ORDER BY updated_at DESC LIMIT ?"
        params.append(limit)

        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [self._row_to_contract(row) for row in rows]

    def delete_contract(self, contract_id: str) -> bool:
        with self._connect() as connection:
            cursor = connection.execute("DELETE FROM contracts WHERE id = ?", (contract_id,))
        return cursor.rowcount > 0

    def create_dependency_edge(self, record: DependencyEdgeRecord) -> DependencyEdgeRecord:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO dependency_edges (
                    id, source_work_item_id, target_work_item_id, edge_type, status,
                    metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.id,
                    record.source_work_item_id,
                    record.target_work_item_id,
                    record.edge_type,
                    record.status,
                    _encode_json(record.metadata),
                    record.created_at,
                    record.updated_at,
                ),
            )
        return record

    def update_dependency_edge(self, edge_id: str, **changes: Any) -> DependencyEdgeRecord:
        current = self.get_dependency_edge(edge_id)
        if current is None:
            raise ValueError(f"Unknown dependency edge id: {edge_id}")

        updated = DependencyEdgeRecord(
            id=current.id,
            source_work_item_id=changes.get("source_work_item_id", current.source_work_item_id),
            target_work_item_id=changes.get("target_work_item_id", current.target_work_item_id),
            edge_type=changes.get("edge_type", current.edge_type),
            status=changes.get("status", current.status),
            metadata=dict(changes.get("metadata", current.metadata)),
            created_at=current.created_at,
            updated_at=changes.get("updated_at", _now_iso()),
        )

        with self._connect() as connection:
            connection.execute(
                """
                UPDATE dependency_edges
                SET source_work_item_id = ?, target_work_item_id = ?, edge_type = ?,
                    status = ?, metadata_json = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    updated.source_work_item_id,
                    updated.target_work_item_id,
                    updated.edge_type,
                    updated.status,
                    _encode_json(updated.metadata),
                    updated.updated_at,
                    edge_id,
                ),
            )
        return updated

    def get_dependency_edge(self, edge_id: str) -> DependencyEdgeRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM dependency_edges WHERE id = ?",
                (edge_id,),
            ).fetchone()
        return self._row_to_dependency_edge(row) if row else None

    def list_dependency_edges(
        self,
        *,
        source_work_item_id: str | None = None,
        target_work_item_id: str | None = None,
        edge_type: str | None = None,
        status: str | None = None,
        limit: int = 100,
    ) -> list[DependencyEdgeRecord]:
        query = "SELECT * FROM dependency_edges"
        filters: list[str] = []
        params: list[Any] = []

        if source_work_item_id:
            filters.append("source_work_item_id = ?")
            params.append(source_work_item_id)
        if target_work_item_id:
            filters.append("target_work_item_id = ?")
            params.append(target_work_item_id)
        if edge_type:
            filters.append("edge_type = ?")
            params.append(edge_type)
        if status:
            filters.append("status = ?")
            params.append(status)
        if filters:
            query += " WHERE " + " AND ".join(filters)
        query += " ORDER BY updated_at DESC LIMIT ?"
        params.append(limit)

        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [self._row_to_dependency_edge(row) for row in rows]

    def delete_dependency_edge(self, edge_id: str) -> bool:
        with self._connect() as connection:
            cursor = connection.execute("DELETE FROM dependency_edges WHERE id = ?", (edge_id,))
        return cursor.rowcount > 0

    def create_decision(self, record: DecisionRecord) -> DecisionRecord:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO decisions (
                    id, work_item_id, decision_type, status, options_json, chosen_option,
                    decider, rationale, metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.id,
                    record.work_item_id,
                    record.decision_type,
                    record.status,
                    _encode_json(record.options),
                    record.chosen_option,
                    record.decider,
                    record.rationale,
                    _encode_json(record.metadata),
                    record.created_at,
                    record.updated_at,
                ),
            )
        return record

    def update_decision(self, decision_id: str, **changes: Any) -> DecisionRecord:
        current = self.get_decision(decision_id)
        if current is None:
            raise ValueError(f"Unknown decision id: {decision_id}")

        updated = DecisionRecord(
            id=current.id,
            work_item_id=changes.get("work_item_id", current.work_item_id),
            decision_type=changes.get("decision_type", current.decision_type),
            status=changes.get("status", current.status),
            options=list(changes.get("options", current.options)),
            chosen_option=changes.get("chosen_option", current.chosen_option),
            decider=changes.get("decider", current.decider),
            rationale=changes.get("rationale", current.rationale),
            metadata=dict(changes.get("metadata", current.metadata)),
            created_at=current.created_at,
            updated_at=changes.get("updated_at", _now_iso()),
        )

        with self._connect() as connection:
            connection.execute(
                """
                UPDATE decisions
                SET work_item_id = ?, decision_type = ?, status = ?, options_json = ?,
                    chosen_option = ?, decider = ?, rationale = ?, metadata_json = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    updated.work_item_id,
                    updated.decision_type,
                    updated.status,
                    _encode_json(updated.options),
                    updated.chosen_option,
                    updated.decider,
                    updated.rationale,
                    _encode_json(updated.metadata),
                    updated.updated_at,
                    decision_id,
                ),
            )
        return updated

    def get_decision(self, decision_id: str) -> DecisionRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM decisions WHERE id = ?",
                (decision_id,),
            ).fetchone()
        return self._row_to_decision(row) if row else None

    def list_decisions(
        self,
        *,
        work_item_id: str | None = None,
        decision_type: str | None = None,
        status: str | None = None,
        decider: str | None = None,
        limit: int = 100,
    ) -> list[DecisionRecord]:
        query = "SELECT * FROM decisions"
        filters: list[str] = []
        params: list[Any] = []

        if work_item_id:
            filters.append("work_item_id = ?")
            params.append(work_item_id)
        if decision_type:
            filters.append("decision_type = ?")
            params.append(decision_type)
        if status:
            filters.append("status = ?")
            params.append(status)
        if decider:
            filters.append("decider = ?")
            params.append(decider)
        if filters:
            query += " WHERE " + " AND ".join(filters)
        query += " ORDER BY updated_at DESC LIMIT ?"
        params.append(limit)

        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [self._row_to_decision(row) for row in rows]

    def delete_decision(self, decision_id: str) -> bool:
        with self._connect() as connection:
            cursor = connection.execute("DELETE FROM decisions WHERE id = ?", (decision_id,))
        return cursor.rowcount > 0

    def create_metrics_snapshot(self, metrics: dict[str, Any], snapshot_type: str = "observability") -> MetricsSnapshotRecord:
        record = MetricsSnapshotRecord(
            id=str(uuid.uuid4())[:36],
            snapshot_type=str(snapshot_type or "observability"),
            generated_at=str(metrics.get("generated_at") or _now_iso()),
            metrics=dict(metrics),
        )
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO metrics_snapshots (
                    id, snapshot_type, generated_at, metrics_json, created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    record.id,
                    record.snapshot_type,
                    record.generated_at,
                    _encode_json(record.metrics),
                    record.created_at,
                ),
            )
        return record

    def list_metrics_snapshots(self, *, snapshot_type: str | None = None, limit: int = 100) -> list[MetricsSnapshotRecord]:
        query = "SELECT * FROM metrics_snapshots"
        params: list[Any] = []
        if snapshot_type:
            query += " WHERE snapshot_type = ?"
            params.append(snapshot_type)
        query += " ORDER BY generated_at DESC, created_at DESC LIMIT ?"
        params.append(limit)

        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [
            MetricsSnapshotRecord(
                id=row["id"],
                snapshot_type=row["snapshot_type"],
                generated_at=row["generated_at"],
                metrics=_decode_json(row["metrics_json"], {}),
                created_at=row["created_at"],
            )
            for row in rows
        ]

    def _row_to_work_item(self, row: sqlite3.Row) -> WorkItemRecord:
        return WorkItemRecord(
            id=row["id"],
            module=row["module"],
            goal=row["goal"],
            status=row["status"],
            priority=row["priority"],
            owner_agent=row["owner_agent"],
            session_key=row["session_key"],
            decision_required=bool(row["decision_required"]),
            decision_type=row["decision_type"],
            blocked_by=_decode_json(row["blocked_by_json"], []),
            artifacts=_decode_json(row["artifacts_json"], []),
            metadata=_decode_json(row["metadata_json"], {}),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _row_to_contract(self, row: sqlite3.Row) -> ContractRecord:
        return ContractRecord(
            id=row["id"],
            provider_module=row["provider_module"],
            consumer_module=row["consumer_module"],
            interface_name=row["interface_name"],
            version=row["version"],
            status=row["status"],
            functions=_decode_json(row["functions_json"], []),
            spec=_decode_json(row["spec_json"], {}),
            stub_path=row["stub_path"],
            implementation_path=row["implementation_path"],
            work_item_id=row["work_item_id"],
            metadata=_decode_json(row["metadata_json"], {}),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _row_to_dependency_edge(self, row: sqlite3.Row) -> DependencyEdgeRecord:
        return DependencyEdgeRecord(
            id=row["id"],
            source_work_item_id=row["source_work_item_id"],
            target_work_item_id=row["target_work_item_id"],
            edge_type=row["edge_type"],
            status=row["status"],
            metadata=_decode_json(row["metadata_json"], {}),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _row_to_decision(self, row: sqlite3.Row) -> DecisionRecord:
        return DecisionRecord(
            id=row["id"],
            work_item_id=row["work_item_id"],
            decision_type=row["decision_type"],
            status=row["status"],
            options=_decode_json(row["options_json"], []),
            chosen_option=row["chosen_option"],
            decider=row["decider"],
            rationale=row["rationale"],
            metadata=_decode_json(row["metadata_json"], {}),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
