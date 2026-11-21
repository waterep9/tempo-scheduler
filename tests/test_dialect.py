"""跨方言检查：PostgreSQL DDL 与 UPSERT 语句可编译（无需真实 PG 实例）。"""
from __future__ import annotations

from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.schema import CreateTable

from app.db import Base
from app.models import Task


def test_all_tables_compile_for_postgres():
    for table in Base.metadata.sorted_tables:
        ddl = str(CreateTable(table).compile(dialect=postgresql.dialect()))
        assert "CREATE TABLE" in ddl
        assert table.name in ddl


def test_upsert_compiles_for_both_dialects():
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    from sqlalchemy.dialects.sqlite import insert as sqlite_insert

    values = {
        "job_id": "j1",
        "fire_time": "2026-11-21 00:00:00",
        "shard_index": 0,
        "shard_total": 1,
        "idempotency_key": "k1",
        "status": "PENDING",
        "priority": 0,
        "attempt": 0,
        "max_attempts": 1,
        "scheduled_at": "2026-11-21 00:00:00",
        "next_retry_at": "2026-11-21 00:00:00",
        "created_at": "2026-11-21 00:00:00",
        "updated_at": "2026-11-21 00:00:00",
    }

    pg_sql = str(pg_insert(Task).values(**values).on_conflict_do_nothing().compile(dialect=postgresql.dialect()))
    sqlite_sql = str(
        sqlite_insert(Task).values(**values).on_conflict_do_nothing().compile(dialect=sqlite.dialect())
    )
    assert "ON CONFLICT DO NOTHING" in pg_sql
    assert "ON CONFLICT DO NOTHING" in sqlite_sql
