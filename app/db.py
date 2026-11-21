"""数据库层：SQLite（默认，零依赖）与 PostgreSQL（生产推荐）双支持。

- SQLite 打开 WAL + busy_timeout，缓解多进程写入时的 "database is locked"。
- 测试使用 StaticPool 共享同一内存连接，避免每个连接一个空库。
- `insert_ignore` 抽象了 SQLite / PostgreSQL 的 ON CONFLICT 语法，
  它是"物化幂等"的基础设施（见 services/planner.py）。
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

from sqlalchemy import create_engine, event, insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from sqlalchemy.pool import StaticPool

from .clock import now
from .config import settings


class Base(DeclarativeBase):
    pass


def _make_engine(url: str):
    kwargs: dict[str, Any] = {"future": True, "pool_pre_ping": True}
    if url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False, "timeout": 15}
        if ":memory:" in url or "mode=memory" in url:
            kwargs["poolclass"] = StaticPool
    return create_engine(url, **kwargs)


engine = _make_engine(settings.database_url)


if settings.database_url.startswith("sqlite"):

    @event.listens_for(engine, "connect")
    def _sqlite_pragmas(dbapi_conn, _record):  # pragma: no cover - 依赖真实连接
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=NORMAL")
        cur.execute("PRAGMA busy_timeout=15000")
        cur.execute("PRAGMA foreign_keys=ON")
        cur.close()


SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, future=True)


def init_db() -> None:
    from . import models  # noqa: F401  注册元数据

    Base.metadata.create_all(bind=engine)


@contextmanager
def session_scope() -> Iterator[Session]:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def insert_ignore(session: Session, model, values: dict[str, Any]) -> bool:
    """插入一行；若唯一约束冲突则静默跳过。返回是否真正插入。

    这是任务物化的幂等闸门：同一 (job_id, fire_time, shard) 只会产生一条记录，
    即使调度器在崩溃后重复扫到同一个触发点。
    """
    dialect = session.get_bind().dialect.name
    try:
        if dialect == "sqlite":
            from sqlalchemy.dialects.sqlite import insert as sqlite_insert

            stmt = sqlite_insert(model).values(**values).on_conflict_do_nothing()
            return session.execute(stmt).rowcount == 1
        if dialect == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as pg_insert

            stmt = pg_insert(model).values(**values).on_conflict_do_nothing()
            return session.execute(stmt).rowcount == 1
        session.execute(insert(model).values(**values))
        return True
    except IntegrityError:
        session.rollback()
        return False


def utcnow_naive():
    return now().replace(tzinfo=None)
