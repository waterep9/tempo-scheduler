"""任务领取、并发上限、心跳与租约回收。"""
from __future__ import annotations

from datetime import timedelta

from sqlalchemy import select

from app.clock import naive, now
from app.db import session_scope
from app.models import Task, Worker
from app.services.lease import (
    claim_tasks,
    heartbeat,
    reap_expired,
    register_worker,
)
from app.services.planner import Planner


def prepare_tasks(due_job, db, shards: int, **overrides):
    due_job(shard_count=shards, **overrides)
    Planner().tick()
    return db.scalars(select(Task).order_by(Task.shard_index)).all()


def test_multiple_workers_never_share_tasks(due_job, db):
    prepare_tasks(due_job, db, 6, max_concurrency=10)

    first = claim_tasks(db, "worker-a", 3)
    second = claim_tasks(db, "worker-b", 3)
    third = claim_tasks(db, "worker-c", 3)

    assert len(first) == 3
    assert len(second) == 3
    assert third == []
    ids = {t.id for t in first} | {t.id for t in second}
    assert len(ids) == 6
    assert all(t.status == "RUNNING" for t in first + second)
    assert {t.worker_id for t in first} == {"worker-a"}


def test_max_concurrency_limits_parallelism(due_job, db):
    prepare_tasks(due_job, db, 3, max_concurrency=1)

    claimed = claim_tasks(db, "worker-a", 10)
    assert len(claimed) == 1

    # 第一个还在跑，其他 worker 也领不到
    assert claim_tasks(db, "worker-b", 10) == []


def test_priority_orders_claims(due_job, db):
    due_job(shard_count=2, max_concurrency=10)
    Planner().tick()
    tasks = db.scalars(select(Task).order_by(Task.shard_index)).all()
    tasks[0].priority = 5
    db.commit()

    claimed = claim_tasks(db, "worker-a", 1)
    assert claimed[0].id == tasks[0].id
    assert claimed[0].priority == 5


def test_expired_lease_is_requeued(due_job, db):
    prepare_tasks(due_job, db, 1, max_attempts=2)
    claimed = claim_tasks(db, "worker-a", 1, lease_ttl=30)
    task = claimed[0]
    task.lease_expires_at = naive(now()) - timedelta(seconds=1)
    db.commit()

    result = reap_expired(db)
    assert result["requeued"] == 1
    db.expire_all()
    refreshed = db.get(Task, task.id)
    assert refreshed.status == "PENDING"
    assert refreshed.attempt == 1
    assert refreshed.error == "worker_lease_expired"

    # 重新可领取，且尝试次数累加
    again = claim_tasks(db, "worker-b", 1)
    assert len(again) == 1
    assert again[0].attempt == 2


def test_expired_lease_with_no_attempts_left_goes_dead(due_job, db):
    prepare_tasks(due_job, db, 1, max_attempts=1)
    task = claim_tasks(db, "worker-a", 1)[0]
    task.lease_expires_at = naive(now()) - timedelta(seconds=1)
    db.commit()

    result = reap_expired(db)
    assert result["dead"] == 1
    db.expire_all()
    assert db.get(Task, task.id).status == "DEAD"


def test_heartbeat_extends_lease(due_job, db):
    prepare_tasks(due_job, db, 1)
    task = claim_tasks(db, "worker-a", 1, lease_ttl=1)[0]
    task.lease_expires_at = naive(now()) - timedelta(seconds=30)
    db.commit()

    stats = heartbeat(db, "worker-a", capacity=4, lease_ttl=30)
    assert stats["renewed"] == 1
    db.expire_all()
    assert db.get(Task, task.id).lease_expires_at > naive(now())


def test_disabled_job_tasks_are_cancelled(due_job, db, make_job):
    job = make_job(name="paused-job", interval_seconds=300)
    from tests.conftest import make_task

    task = make_task(db, job)
    job.enabled = False
    db.commit()

    assert claim_tasks(db, "worker-a", 10) == []
    db.expire_all()
    assert db.get(Task, task.id).status == "CANCELLED"


def test_worker_registration_and_offline_marking(due_job, db):
    register_worker(db, "worker-x", 4)
    register_worker(db, "worker-x", 8)
    rows = db.scalars(select(Worker)).all()
    assert len(rows) == 1
    assert rows[0].capacity == 8
    assert rows[0].status == "ONLINE"

    rows[0].last_heartbeat_at = naive(now()) - timedelta(seconds=120)
    db.commit()
    result = reap_expired(db)
    assert result["workers_offline"] == 1
    db.expire_all()
    assert db.get(Worker, "worker-x").status == "OFFLINE"


def test_claim_uses_separate_session(due_job, db):
    """领取在独立事务中提交，另一个会话能看到 RUNNING 状态。"""
    prepare_tasks(due_job, db, 1)
    with session_scope() as session:
        claim_tasks(session, "worker-a", 1)
    db.expire_all()
    assert db.scalar(select(Task)).status == "RUNNING"


def test_saturated_job_does_not_starve_others(make_job, db):
    """并发打满的任务不应占满候选窗口，导致其他任务饿死（队头阻塞回归）。"""
    from tests.conftest import make_task

    busy = make_job(name="busy-job", interval_seconds=60, max_concurrency=1)
    for _ in range(60):
        make_task(db, busy)
    assert len(claim_tasks(db, "worker-a", 1)) == 1  # 打满 busy 的并发上限

    free = make_job(name="free-job", interval_seconds=60, max_concurrency=2)
    free_task = make_task(db, free)

    claimed = claim_tasks(db, "worker-b", 2)
    assert [t.id for t in claimed] == [free_task.id]
