"""调度主循环：物化、幂等、补偿策略、选主与 fencing。"""
from __future__ import annotations

from datetime import timedelta

from sqlalchemy import func, select

from app.clock import aware, naive, now
from app.models import SchedulerLease, Task
from app.services.lease import LeaderElector
from app.services.planner import Planner


def count_tasks(db) -> int:
    return db.scalar(select(func.count()).select_from(Task)) or 0


def test_tick_materializes_all_shards(due_job, db):
    job = due_job(shard_count=3)
    result = Planner().tick()

    assert result["leader"] is True
    assert result["jobs_scanned"] >= 1
    assert result["tasks_created"] == 3
    assert count_tasks(db) == 3
    assert sorted(t.shard_index for t in db.scalars(select(Task)).all()) == [0, 1, 2]
    assert {t.shard_total for t in db.scalars(select(Task)).all()} == {3}
    assert job.name  # 触发后 job 仍存在


def test_materialization_is_idempotent(due_job, db):
    job = due_job(shard_count=2)
    Planner().tick()
    first_fire = db.scalars(select(Task).order_by(Task.fire_time)).first().fire_time

    # 模拟"提交后崩溃、重启又扫到同一触发点"：把下次触发时间拨回该触发点
    refreshed = db.get(type(job), job.id)
    refreshed.next_fire_at = first_fire
    db.commit()

    second = Planner().tick()
    assert second["leader"] is True
    assert second["tasks_created"] == 0
    assert count_tasks(db) == 2


def test_catch_up_skip_collapses_backlog(due_job, db):
    job = due_job(interval_seconds=60, catch_up="skip")
    db.get(type(job), job.id).next_fire_at = naive(now() - timedelta(minutes=10))
    db.commit()

    result = Planner().tick()
    assert result["tasks_created"] == 1
    db.expire_all()
    refreshed = db.get(type(job), job.id)
    assert aware(refreshed.next_fire_at) > now()


def test_catch_up_all_fires_each_period(due_job, db):
    job = due_job(interval_seconds=60, catch_up="all")
    db.get(type(job), job.id).next_fire_at = naive(now() - timedelta(seconds=125))
    db.commit()

    result = Planner().tick()
    assert result["tasks_created"] == 3


def test_once_job_disables_itself(due_job, db):
    job = due_job(schedule_type="once", interval_seconds=None)
    db.get(type(job), job.id).next_fire_at = naive(now() - timedelta(seconds=1))
    db.commit()

    result = Planner().tick()
    assert result["tasks_created"] == 1
    db.expire_all()
    refreshed = db.get(type(job), job.id)
    assert refreshed.enabled is False
    assert refreshed.next_fire_at is None


def test_standby_node_does_not_dispatch(due_job, db):
    due_job()
    leader = LeaderElector(holder="node-a", ttl_seconds=30)
    assert leader.acquire(db) == 1

    follower = Planner(elector=LeaderElector(holder="node-b", ttl_seconds=30))
    result = follower.tick()
    assert result["leader"] is False
    assert result["tasks_created"] == 0


def test_takeover_bumps_fencing_token(due_job, db):
    due_job()
    first = LeaderElector(holder="node-a", ttl_seconds=30)
    token_a = first.acquire(db)
    assert token_a == 1

    # 模拟 node-a 假死：租约过期
    lease = db.get(SchedulerLease, 1)
    lease.expires_at = naive(now() - timedelta(seconds=1))
    db.commit()

    second = LeaderElector(holder="node-b", ttl_seconds=30)
    token_b = second.acquire(db)
    assert token_b == token_a + 1
    assert second.verify(db) is True

    # 老主节点提交前校验会失败 —— 这正是 fencing 的作用
    assert first.verify(db) is False


def test_broken_job_does_not_block_others(due_job, db, make_job):
    healthy = due_job(name="healthy-job")
    broken = make_job(name="broken-job", interval_seconds=60)
    broken.schedule_type = "cron"
    broken.cron_expr = "这是非法表达式"
    broken.next_fire_at = naive(now() - timedelta(seconds=5))
    db.commit()

    result = Planner().tick()
    assert result["jobs_failed"] >= 1
    assert result["tasks_created"] >= 1
    created = db.scalars(select(Task).where(Task.job_id == healthy.id)).all()
    assert len(created) == 1


def test_reaper_runs_on_tick(make_job, db):
    job = make_job()
    stamp = naive(now())
    task = Task(
        job_id=job.id,
        fire_time=stamp,
        shard_index=0,
        shard_total=1,
        idempotency_key="k-1",
        status="RUNNING",
        attempt=1,
        max_attempts=3,
        scheduled_at=stamp,
        next_retry_at=stamp,
        worker_id="ghost-worker",
        lease_expires_at=stamp - timedelta(seconds=5),
        created_at=stamp,
        updated_at=stamp,
    )
    db.add(task)
    db.commit()

    result = Planner().tick()
    assert result["requeued"] == 1
    db.expire_all()
    refreshed = db.get(Task, task.id)
    assert refreshed.status == "PENDING"
    assert refreshed.worker_id is None
    assert refreshed.attempt == 1
