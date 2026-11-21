"""任务状态机：成功、重试退避、死信、重放、取消与迟到结果。"""
from __future__ import annotations

import random
from datetime import timedelta

from sqlalchemy import select

from app.clock import naive, now
from app.models import Event, Task
from app.services.runner import (
    backoff_delay,
    cancel_task,
    complete_task,
    replay_task,
    truncate,
)
from tests.conftest import make_task


def running_task(db, job, worker="worker-a", attempt=1):
    task = make_task(db, job)
    task.status = "RUNNING"
    task.worker_id = worker
    task.attempt = attempt
    task.lease_expires_at = naive(now()) + timedelta(seconds=30)
    task.started_at = naive(now())
    db.commit()
    return task


def test_success_marks_succeeded(due_job, db):
    job = due_job()
    task = running_task(db, job)
    outcome = complete_task(
        db, task.id, "worker-a", ok=True, exit_code=0, output="hello", duration_ms=42
    )
    assert outcome["status"] == "SUCCEEDED"
    db.expire_all()
    refreshed = db.get(Task, task.id)
    assert refreshed.output == "hello"
    assert refreshed.exit_code == 0
    assert refreshed.finished_at is not None
    assert refreshed.lease_expires_at is None
    events = {e.type for e in db.scalars(select(Event)).all()}
    assert "TASK_SUCCEEDED" in events


def test_failure_schedules_retry_with_backoff(due_job, db):
    job = due_job(backoff_base_seconds=10, backoff_cap_seconds=60)
    task = running_task(db, job, attempt=1)
    outcome = complete_task(db, task.id, "worker-a", ok=False, error="boom", exit_code=1)

    assert outcome["status"] == "PENDING"
    assert 8.5 <= outcome["retry_in"] <= 11.5
    db.expire_all()
    refreshed = db.get(Task, task.id)
    assert refreshed.status == "PENDING"
    assert refreshed.worker_id is None
    assert refreshed.error == "boom"
    assert refreshed.next_retry_at > naive(now())


def test_attempts_exhausted_goes_dead_and_can_replay(due_job, db):
    job = due_job()
    task = running_task(db, job, attempt=3)
    outcome = complete_task(db, task.id, "worker-a", ok=False, error="still failing")
    assert outcome["status"] == "DEAD"

    db.expire_all()
    assert db.get(Task, task.id).status == "DEAD"

    replayed = replay_task(db, task.id, "tester")
    assert replayed is not None
    db.expire_all()
    refreshed = db.get(Task, task.id)
    assert refreshed.status == "PENDING"
    assert refreshed.attempt == 0
    assert refreshed.error is None
    assert refreshed.next_retry_at <= naive(now())


def test_stale_worker_result_is_rejected(due_job, db):
    job = due_job()
    task = running_task(db, job, worker="worker-a")
    assert complete_task(db, task.id, "worker-b", ok=True) is None
    db.expire_all()
    assert db.get(Task, task.id).status == "RUNNING"


def test_complete_after_cancel_is_rejected(due_job, db):
    job = due_job()
    task = running_task(db, job)
    assert cancel_task(db, task.id, "tester") is not None
    assert complete_task(db, task.id, "worker-a", ok=True) is None


def test_cancel_terminal_task_returns_none(due_job, db):
    job = due_job()
    task = running_task(db, job)
    complete_task(db, task.id, "worker-a", ok=True)
    assert cancel_task(db, task.id, "tester") is None


def test_truncate_keeps_head_and_notes_length():
    text = "x" * 100
    result = truncate(text, 10)
    assert result.startswith("x" * 10)
    assert "截断" in result
    assert truncate(None) is None
    assert truncate("short", 10) == "short"


def test_backoff_is_capped_and_jittered():
    rng = random.Random(7)
    first = backoff_delay(1, base=10, cap=300, rng=rng)
    fifth = backoff_delay(5, base=10, cap=300, rng=rng)
    huge = backoff_delay(20, base=10, cap=300, rng=rng)
    assert 8.5 <= first <= 11.5
    assert fifth > first
    assert huge <= 300
