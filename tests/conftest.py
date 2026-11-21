"""测试夹具：内存 SQLite + 独立 schema，每个用例前重置。"""
from __future__ import annotations

import itertools
import os

os.environ["DATABASE_URL"] = "sqlite:///:memory:"
os.environ["TEMPO_BOOTSTRAP_TOKEN"] = "test-admin-token"
os.environ["TEMPO_EMBEDDED_PLANNER"] = "0"
os.environ["TEMPO_API_RATE_LIMIT"] = "1000"
os.environ["TEMPO_TASK_LEASE_TTL"] = "3"
os.environ["TEMPO_LEADER_TTL"] = "3"
os.environ["TEMPO_MAX_CATCH_UP"] = "5"

from datetime import timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.clock import naive, now
from app.db import Base, SessionLocal, engine
from app.main import app
from app.models import Job, Task
from app.security import LIMITER, ensure_bootstrap_key
from app.services.jobs import create_job


@pytest.fixture(autouse=True)
def fresh_database():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    with SessionLocal() as session:
        ensure_bootstrap_key(session)
    LIMITER.reset()
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def db() -> Session:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture
def auth() -> dict[str, str]:
    return {"Authorization": "Bearer test-admin-token"}


@pytest.fixture
def make_job(db: Session):
    def _make(**overrides) -> Job:
        data = {
            "name": overrides.pop("name", f"job-{os.urandom(4).hex()}"),
            "schedule_type": "interval",
            "interval_seconds": 300,
            "executor": "shell",
            "command": "echo tempo",
            "max_attempts": 3,
            "shard_count": 1,
            "max_concurrency": 2,
            "timezone": "Asia/Shanghai",
        }
        data.update(overrides)
        job = create_job(db, data, "tester")
        return job

    return _make


@pytest.fixture
def due_job(make_job, db: Session):
    """创建一个"已经到期"的任务：下次触发时间置为 5 秒前。"""

    def _make(**overrides) -> Job:
        job = make_job(**overrides)
        job.next_fire_at = naive(now() - timedelta(seconds=5))
        db.commit()
        return job

    return _make


_TASK_SEQ = itertools.count(1)


def make_task(db: Session, job: Job, status: str = "PENDING") -> Task:
    stamp = naive(now()) - timedelta(seconds=next(_TASK_SEQ))
    task = Task(
        job_id=job.id,
        fire_time=stamp,
        shard_index=0,
        shard_total=1,
        idempotency_key=f"{job.id}:{os.urandom(6).hex()}",
        status=status,
        priority=0,
        attempt=0,
        max_attempts=job.max_attempts,
        scheduled_at=stamp,
        next_retry_at=stamp,
        created_at=stamp,
        updated_at=stamp,
    )
    db.add(task)
    db.commit()
    return task
