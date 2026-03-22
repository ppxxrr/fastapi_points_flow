from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from sqlalchemy import MetaData, create_engine, func, select, text
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.engine import Engine, URL, make_url

from app.db.base import Base
from app.db.config import build_database_url
import app.db.models  # noqa: F401


ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_SQLITE_SOURCE_URL = f"sqlite:///{(ROOT_DIR / 'data' / 'member_module.db').as_posix()}"
DEFAULT_TABLE_ORDER = [
    "member_level_dict",
    "member_profile",
    "member_profile_attr",
    "member_account",
    "member_level_change_log",
    "member_level_period",
    "device_layout_point",
    "message_board_entry",
    "dim_parking_policy",
    "member_point_flow",
    "parking_record",
    "sync_task_log",
    "sync_job_state",
]


@dataclass(slots=True)
class TableMigrationSummary:
    table_name: str
    source_count: int = 0
    target_count_before: int = 0
    target_count_after: int = 0
    processed_rows: int = 0
    batches: int = 0
    batch_size: int = 0
    status: str = "pending"
    error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class MigrationSummary:
    source_url: str
    target_url: str
    table_names: list[str]
    batch_size: int
    truncate_target: bool = False
    tables: list[TableMigrationSummary] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_url": self.source_url,
            "target_url": self.target_url,
            "table_names": self.table_names,
            "batch_size": self.batch_size,
            "truncate_target": self.truncate_target,
            "tables": [item.to_dict() for item in self.tables],
        }


@dataclass(slots=True)
class TableVerificationSummary:
    table_name: str
    source_count: int = 0
    target_count: int = 0
    count_match: bool = False
    source_min_pk: Any = None
    source_max_pk: Any = None
    target_min_pk: Any = None
    target_max_pk: Any = None
    pk_range_match: bool | None = None
    source_head_keys: list[Any] = field(default_factory=list)
    source_tail_keys: list[Any] = field(default_factory=list)
    target_head_keys: list[Any] = field(default_factory=list)
    target_tail_keys: list[Any] = field(default_factory=list)
    sample_match: bool | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class VerificationSummary:
    source_url: str
    target_url: str
    table_names: list[str]
    sample_size: int = 5
    tables: list[TableVerificationSummary] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_url": self.source_url,
            "target_url": self.target_url,
            "table_names": self.table_names,
            "sample_size": self.sample_size,
            "tables": [item.to_dict() for item in self.tables],
        }


def resolve_source_url(source_url: str | None) -> str:
    return (source_url or DEFAULT_SQLITE_SOURCE_URL).strip()


def resolve_target_mysql_url(target_url: str | None) -> str:
    if target_url and target_url.strip():
        return target_url.strip()

    url = build_database_url()
    if make_url(url).get_backend_name().startswith("mysql"):
        return url
    raise ValueError("target MySQL url is required; set --target-url or MYSQL_* env vars")


def resolve_table_names(table_names: list[str] | None) -> list[str]:
    if not table_names:
        return list(DEFAULT_TABLE_ORDER)
    known = set(Base.metadata.tables.keys())
    invalid = [name for name in table_names if name not in known]
    if invalid:
        raise ValueError(f"unknown tables: {', '.join(invalid)}")
    ordered = [name for name in DEFAULT_TABLE_ORDER if name in table_names]
    remaining = [name for name in table_names if name not in ordered]
    return ordered + remaining


def create_db_engine(url: str) -> Engine:
    db_url = make_url(url)
    connect_args: dict[str, object] = {}
    if db_url.get_backend_name() == "sqlite":
        connect_args["check_same_thread"] = False
    return create_engine(url, future=True, pool_pre_ping=db_url.get_backend_name() != "sqlite", connect_args=connect_args)


def ensure_mysql_database_exists(target_url: str) -> None:
    url = make_url(target_url)
    if not url.get_backend_name().startswith("mysql"):
        raise ValueError("target url must be mysql")
    if not url.database:
        raise ValueError("target mysql url must include database name")

    server_url: URL = url.set(database=None)
    engine = create_engine(server_url.render_as_string(hide_password=False), future=True, isolation_level="AUTOCOMMIT")
    database_name = url.database.replace("`", "``")
    try:
        with engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{database_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"))
    finally:
        engine.dispose()


class MySQLMigrationService:
    def __init__(self, source_engine: Engine, target_engine: Engine):
        self.source_engine = source_engine
        self.target_engine = target_engine
        self.metadata: MetaData = Base.metadata

    def migrate_tables(
        self,
        *,
        table_names: list[str],
        batch_size: int = 2000,
        truncate_target: bool = False,
        logger: callable | None = None,
    ) -> MigrationSummary:
        summary = MigrationSummary(
            source_url=str(self.source_engine.url),
            target_url=str(self.target_engine.url),
            table_names=table_names,
            batch_size=batch_size,
            truncate_target=truncate_target,
        )
        log = logger or (lambda level, message: None)

        with self.source_engine.connect() as source_conn, self.target_engine.connect() as target_conn:
            if truncate_target:
                self._disable_mysql_fk_checks(target_conn)
            try:
                for table_name in table_names:
                    table = self.metadata.tables[table_name]
                    table_summary = TableMigrationSummary(table_name=table_name, batch_size=batch_size)
                    summary.tables.append(table_summary)
                    try:
                        table_summary.source_count = self._count_rows(source_conn, table)
                        table_summary.target_count_before = self._count_rows(target_conn, table)
                        if truncate_target:
                            target_conn.execute(text(f"DELETE FROM `{table_name}`"))
                            target_conn.commit()
                            table_summary.target_count_before = 0

                        if table_summary.source_count == 0:
                            table_summary.target_count_after = self._count_rows(target_conn, table)
                            table_summary.status = "success"
                            continue

                        pk_column = self._get_single_pk_column(table)
                        last_pk = None
                        while True:
                            rows = self._fetch_source_batch(
                                source_conn=source_conn,
                                table=table,
                                pk_column=pk_column,
                                last_pk=last_pk,
                                batch_size=batch_size,
                            )
                            if not rows:
                                break

                            self._upsert_batch(target_conn=target_conn, table=table, rows=rows)
                            target_conn.commit()
                            table_summary.processed_rows += len(rows)
                            table_summary.batches += 1
                            last_pk = rows[-1][pk_column.name]
                            log("INFO", f"[mysql_migration] table={table_name} batch={table_summary.batches} rows={table_summary.processed_rows}")

                        table_summary.target_count_after = self._count_rows(target_conn, table)
                        table_summary.status = "success"
                    except Exception as exc:
                        target_conn.rollback()
                        table_summary.status = "failed"
                        table_summary.error_message = str(exc)
                        raise
            finally:
                if truncate_target:
                    self._enable_mysql_fk_checks(target_conn)

        return summary

    def verify_tables(
        self,
        *,
        table_names: list[str],
        sample_size: int = 5,
    ) -> VerificationSummary:
        summary = VerificationSummary(
            source_url=str(self.source_engine.url),
            target_url=str(self.target_engine.url),
            table_names=table_names,
            sample_size=sample_size,
        )

        with self.source_engine.connect() as source_conn, self.target_engine.connect() as target_conn:
            for table_name in table_names:
                table = self.metadata.tables[table_name]
                item = TableVerificationSummary(table_name=table_name)
                item.source_count = self._count_rows(source_conn, table)
                item.target_count = self._count_rows(target_conn, table)
                item.count_match = item.source_count == item.target_count

                pk_column = self._get_single_pk_column(table)
                if pk_column is not None:
                    item.source_min_pk, item.source_max_pk = self._min_max_pk(source_conn, table, pk_column)
                    item.target_min_pk, item.target_max_pk = self._min_max_pk(target_conn, table, pk_column)
                    item.pk_range_match = (
                        item.source_min_pk == item.target_min_pk and item.source_max_pk == item.target_max_pk
                    )
                    item.source_head_keys = self._sample_keys(source_conn, table, pk_column, sample_size, descending=False)
                    item.source_tail_keys = self._sample_keys(source_conn, table, pk_column, sample_size, descending=True)
                    item.target_head_keys = self._sample_keys(target_conn, table, pk_column, sample_size, descending=False)
                    item.target_tail_keys = self._sample_keys(target_conn, table, pk_column, sample_size, descending=True)
                    item.sample_match = (
                        item.source_head_keys == item.target_head_keys and item.source_tail_keys == item.target_tail_keys
                    )

                summary.tables.append(item)

        return summary

    @staticmethod
    def _count_rows(conn, table) -> int:
        return int(conn.execute(select(func.count()).select_from(table)).scalar_one())

    @staticmethod
    def _get_single_pk_column(table):
        pk_columns = list(table.primary_key.columns)
        return pk_columns[0] if len(pk_columns) == 1 else None

    @staticmethod
    def _fetch_source_batch(*, source_conn, table, pk_column, last_pk, batch_size: int) -> list[dict[str, Any]]:
        stmt = select(table)
        if pk_column is not None:
            if last_pk is not None:
                stmt = stmt.where(pk_column > last_pk)
            stmt = stmt.order_by(pk_column)
        stmt = stmt.limit(batch_size)
        return [dict(row) for row in source_conn.execute(stmt).mappings().all()]

    @staticmethod
    def _upsert_batch(*, target_conn, table, rows: list[dict[str, Any]]) -> None:
        stmt = mysql_insert(table).values(rows)
        update_mapping = {column.name: stmt.inserted[column.name] for column in table.columns if not column.primary_key}
        stmt = stmt.on_duplicate_key_update(**update_mapping)
        target_conn.execute(stmt)

    @staticmethod
    def _disable_mysql_fk_checks(conn) -> None:
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
        conn.commit()

    @staticmethod
    def _enable_mysql_fk_checks(conn) -> None:
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
        conn.commit()

    @staticmethod
    def _min_max_pk(conn, table, pk_column) -> tuple[Any, Any]:
        row = conn.execute(select(func.min(pk_column), func.max(pk_column)).select_from(table)).first()
        if row is None:
            return None, None
        return row[0], row[1]

    @staticmethod
    def _sample_keys(conn, table, pk_column, sample_size: int, *, descending: bool) -> list[Any]:
        order_col = pk_column.desc() if descending else pk_column.asc()
        rows = conn.execute(select(pk_column).select_from(table).order_by(order_col).limit(sample_size)).all()
        values = [row[0] for row in rows]
        return list(reversed(values)) if descending else values
