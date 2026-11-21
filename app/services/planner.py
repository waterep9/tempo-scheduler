"""调度主循环：唯一写"任务实例"的地方。

一次 tick 做的事：
1. 抢/续主节点租约（非主节点直接返回，不产生任何写操作）；
2. 回收过期租约（worker 崩溃的兜底）；
3. 扫描到期 job，按补偿策略补齐触发期，物化任务实例（幂等）；
4. 推进 next_fire_at；
5. 提交前校验 fencing token，发现已被接管则回滚。

幂等由数据库唯一约束 `uq_task_fire_shard` 保证，而不是靠"相信只有一个主节点"。
即便发生脑裂，也只会产生重复的**扫描**，不会产生重复的**任务**。
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..clock import aware, naive, now
from ..config import settings
from ..db import insert_ignore, session_scope
from ..events import audit, emit
from ..metrics import METRICS
from ..models import Job, Task
from .lease import LeaderElector, reap_expired
from .triggers import next_fire_after


class Planner:
    def __init__(self, elector: LeaderElector | None = None) -> None:
        self.elector = elector or LeaderElector()

    def tick(self) -> dict:
        result = {
            "leader": False,
            "fencing_token": None,
            "jobs_scanned": 0,
            "tasks_created": 0,
            "jobs_failed": 0,
            "requeued": 0,
            "dead": 0,
        }
        with session_scope() as session:
            token = self.elector.acquire(session)
            if token is None:
                METRICS.inc("tempo_planner_tick_total", {"role": "standby"})
                return result

            result["leader"] = True
            result["fencing_token"] = token

            reaped = reap_expired(session)
            result["requeued"] = reaped["requeued"]
            result["dead"] = reaped["dead"]

            stamp = now()
            due_jobs = session.scalars(
                select(Job)
                .where(
                    Job.enabled.is_(True),
                    Job.next_fire_at.is_not(None),
                    Job.next_fire_at <= naive(stamp),
                )
                .order_by(Job.next_fire_at.asc())
                .limit(500)
            ).all()
            result["jobs_scanned"] = len(due_jobs)

            for job in due_jobs:
                try:
                    with session.begin_nested():
                        result["tasks_created"] += self._materialize(session, job, stamp)
                except Exception as exc:  # 单个任务的坏表达式不应阻断整轮调度
                    result["jobs_failed"] += 1
                    audit(
                        session,
                        "planner",
                        "JOB_MATERIALIZE_FAILED",
                        target_type="job",
                        target_id=job.id,
                        error=str(exc)[:300],
                    )

            if not self.elector.verify(session):
                # 已被新主节点接管：本轮的推进全部作废，由新主节点重算
                session.rollback()
                METRICS.inc("tempo_planner_tick_total", {"role": "fenced"})
                result["leader"] = False
                result["fenced_out"] = True
                return result

        METRICS.inc("tempo_planner_tick_total", {"role": "leader"})
        return result

    # ------------------------------------------------------------------
    def _materialize(self, session: Session, job: Job, current: datetime) -> int:
        fires: list[datetime] = []
        probe = aware(job.next_fire_at) if job.next_fire_at else None
        guard = 0
        while probe is not None and probe <= current and guard < settings.max_catch_up * 4:
            fires.append(probe)
            guard += 1
            probe = next_fire_after(job, probe)

        if not fires:
            return 0

        dropped = 0
        if job.catch_up == "skip" and len(fires) > 1:
            dropped = len(fires) - 1
            fires = [fires[-1]]
        if len(fires) > settings.max_catch_up:
            dropped += len(fires) - settings.max_catch_up
            fires = fires[-settings.max_catch_up:]

        shards = max(1, int(job.shard_count or 1))
        created = 0
        for fire in fires:
            for shard in range(shards):
                key = f"{job.id}:{int(fire.timestamp())}:{shard}"
                inserted = insert_ignore(
                    session,
                    Task,
                    {
                        "job_id": job.id,
                        "fire_time": naive(fire),
                        "shard_index": shard,
                        "shard_total": shards,
                        "idempotency_key": key,
                        "status": "PENDING",
                        "priority": job.priority or 0,
                        "attempt": 0,
                        "max_attempts": job.max_attempts or 1,
                        "scheduled_at": naive(fire),
                        "next_retry_at": naive(fire),
                        "created_at": naive(current),
                        "updated_at": naive(current),
                    },
                )
                if inserted:
                    created += 1

            emit(
                session,
                "JOB_FIRED",
                job_id=job.id,
                fire_time=aware(fire).isoformat(),
                shards=shards,
                misfire_dropped=dropped,
            )

        nxt = next_fire_after(job, fires[-1])
        skipped_forward = 0
        if job.catch_up == "skip":
            while nxt is not None and nxt <= current and skipped_forward < 1000:
                nxt = next_fire_after(job, nxt)
                skipped_forward += 1

        job.next_fire_at = naive(nxt) if nxt else None
        job.last_fire_at = naive(fires[-1])
        job.updated_at = naive(current)
        if job.schedule_type == "once":
            job.enabled = False
            job.next_fire_at = None

        if created:
            METRICS.inc("tempo_planner_tasks_created_total", None, created)
        return created
