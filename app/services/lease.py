"""多实例协作：主节点选举、任务领取、心跳与租约回收。

关键约束：
1. 同一时刻只有一个调度主节点在"物化"任务（避免重复派发）；
2. 任务领取是 CAS（compare-and-set），多个 worker 竞争时只有一个能拿到；
3. worker 崩溃后，租约过期由回收器重新投递 —— 语义是 **至少一次**。
"""
from __future__ import annotations

import os
import socket
from datetime import timedelta

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from ..clock import naive, now
from ..config import settings
from ..db import insert_ignore
from ..events import emit
from ..models import Job, SchedulerLease, Task, Worker


class LeaderElector:
    """基于单行租约表的选主。

    `fencing_token` 每次易主递增，持有者在提交前校验 token 未变，
    用于识别"自己已被接管"的过期主节点。
    """

    def __init__(self, holder: str | None = None, ttl_seconds: float | None = None) -> None:
        # 节点身份 = 主机名 + 进程号：同一进程内多个 Planner 实例共享身份，
        # 不同进程/容器才是不同节点（这是选主能够"续约"的前提）。
        self.holder = holder or f"scheduler-{socket.gethostname()}-{os.getpid()}"
        self.ttl = ttl_seconds or settings.leader_ttl
        self.token: int | None = None

    def acquire(self, session: Session) -> int | None:
        stamp = naive(now())
        expires = stamp + timedelta(seconds=self.ttl)
        row = session.get(SchedulerLease, 1)

        if row is None:
            inserted = insert_ignore(
                session,
                SchedulerLease,
                {
                    "id": 1,
                    "holder": self.holder,
                    "fencing_token": 1,
                    "acquired_at": stamp,
                    "expires_at": expires,
                },
            )
            session.commit()
            if inserted:
                self.token = 1
                return self.token
            row = session.get(SchedulerLease, 1)
            if row is None:
                return None

        if row.holder == self.holder and row.expires_at > stamp:
            # 续约：不换 token，避免自己把自己踢下线
            row.expires_at = expires
            session.commit()
            self.token = row.fencing_token
            return self.token

        if row.expires_at <= stamp:
            next_token = row.fencing_token + 1
            result = session.execute(
                update(SchedulerLease)
                .where(
                    SchedulerLease.id == 1,
                    SchedulerLease.fencing_token == row.fencing_token,
                    SchedulerLease.expires_at <= stamp,
                )
                .values(
                    holder=self.holder,
                    fencing_token=next_token,
                    acquired_at=stamp,
                    expires_at=expires,
                )
            )
            session.commit()
            if result.rowcount == 1:
                self.token = next_token
                return self.token
        return None

    def verify(self, session: Session) -> bool:
        """提交前校验：token 仍属于自己。"""
        if self.token is None:
            return False
        current = session.scalar(
            select(SchedulerLease.fencing_token).where(
                SchedulerLease.id == 1, SchedulerLease.holder == self.holder
            )
        )
        return current == self.token

    def release(self, session: Session) -> None:
        if self.token is None:
            return
        session.execute(
            update(SchedulerLease)
            .where(
                SchedulerLease.id == 1,
                SchedulerLease.holder == self.holder,
                SchedulerLease.fencing_token == self.token,
            )
            .values(expires_at=naive(now()))
        )
        session.commit()
        self.token = None


def register_worker(
    session: Session, worker_id: str, capacity: int, pool: str = "default"
) -> None:
    stamp = naive(now())
    inserted = insert_ignore(
        session,
        Worker,
        {
            "id": worker_id,
            "hostname": socket.gethostname(),
            "pool": pool,
            "capacity": capacity,
            "status": "ONLINE",
            "started_at": stamp,
            "last_heartbeat_at": stamp,
        },
    )
    if not inserted:
        session.execute(
            update(Worker)
            .where(Worker.id == worker_id)
            .values(
                capacity=capacity,
                status="ONLINE",
                last_heartbeat_at=stamp,
                hostname=socket.gethostname(),
            )
        )
    session.commit()


def heartbeat(
    session: Session, worker_id: str, capacity: int, lease_ttl: float | None = None
) -> dict:
    stamp = naive(now())
    ttl = lease_ttl or settings.task_lease_ttl
    session.execute(
        update(Worker)
        .where(Worker.id == worker_id)
        .values(capacity=capacity, status="ONLINE", last_heartbeat_at=stamp)
    )
    renewed = session.execute(
        update(Task)
        .where(Task.worker_id == worker_id, Task.status == "RUNNING")
        .values(lease_expires_at=stamp + timedelta(seconds=ttl))
    ).rowcount
    session.commit()
    return {"renewed": renewed, "at": stamp}


def claim_tasks(
    session: Session, worker_id: str, limit: int, lease_ttl: float | None = None
) -> list[Task]:
    """领取最多 limit 个可执行任务。

    两步：
    1. 先用聚合查询算出"并发已达上限"的 job，直接从候选集中排除 —— 否则
       队首一堆无法执行的任务会把候选窗口占满，造成其他任务饥饿；
    2. 对候选逐条做条件更新（CAS）。同时校验 status=PENDING，因此两个
       worker 不可能拿到同一条。
    """
    stamp = naive(now())
    ttl = lease_ttl or settings.task_lease_ttl
    lease_until = stamp + timedelta(seconds=ttl)

    jobs_meta = {
        job.id: job
        for job in session.scalars(select(Job).where(Job.enabled.is_(True)))
    }
    running: dict[str, int] = {
        job_id: count
        for job_id, count in session.execute(
            select(Task.job_id, func.count())
            .where(Task.status == "RUNNING")
            .group_by(Task.job_id)
        ).all()
    }
    saturated = {
        job_id
        for job_id, count in running.items()
        if job_id in jobs_meta
        and (jobs_meta[job_id].max_concurrency or 0) > 0
        and count >= (jobs_meta[job_id].max_concurrency or 0)
    }

    stmt = select(Task).where(Task.status == "PENDING", Task.next_retry_at <= stamp)
    if saturated:
        stmt = stmt.where(Task.job_id.notin_(saturated))
    candidates = session.scalars(
        stmt.order_by(Task.priority.desc(), Task.scheduled_at.asc()).limit(max(limit * 2, 20))
    ).all()

    claimed_ids: list[str] = []

    for candidate in candidates:
        if len(claimed_ids) >= limit:
            break

        job = jobs_meta.get(candidate.job_id)
        if job is None:
            # 任务定义被删除或停用：取消僵尸任务，避免无限堆积
            session.execute(
                update(Task)
                .where(Task.id == candidate.id, Task.status == "PENDING")
                .values(
                    status="CANCELLED",
                    finished_at=stamp,
                    updated_at=stamp,
                    error="job_disabled_or_deleted",
                )
            )
            emit(
                session,
                "TASK_CANCELLED",
                job_id=candidate.job_id,
                task_id=candidate.id,
                reason="job_disabled_or_deleted",
            )
            continue

        cap = job.max_concurrency or 0
        if cap > 0 and running.get(job.id, 0) >= cap:
            continue

        result = session.execute(
            update(Task)
            .where(Task.id == candidate.id, Task.status == "PENDING")
            .values(
                status="RUNNING",
                worker_id=worker_id,
                lease_expires_at=lease_until,
                attempt=Task.attempt + 1,
                started_at=stamp,
                updated_at=stamp,
            )
        )
        if result.rowcount == 1:
            claimed_ids.append(candidate.id)
            running[job.id] = running.get(job.id, 0) + 1
            emit(
                session,
                "TASK_STARTED",
                job_id=job.id,
                task_id=candidate.id,
                worker_id=worker_id,
                attempt=candidate.attempt + 1,
            )

    session.commit()
    if not claimed_ids:
        return []
    rows = session.scalars(select(Task).where(Task.id.in_(claimed_ids))).all()
    order = {task_id: idx for idx, task_id in enumerate(claimed_ids)}
    return sorted(rows, key=lambda t: order.get(t.id, 0))


def reap_expired(session: Session, lease_ttl: float | None = None) -> dict:
    """回收租约过期的任务，并把长时间无心跳的 worker 标记离线。"""
    stamp = naive(now())
    requeued = dead = 0

    expired = session.scalars(
        select(Task).where(Task.status == "RUNNING", Task.lease_expires_at.is_not(None), Task.lease_expires_at <= stamp)
    ).all()

    for task in expired:
        if task.attempt < task.max_attempts:
            task.status = "PENDING"
            task.worker_id = None
            task.lease_expires_at = None
            task.next_retry_at = stamp
            task.error = "worker_lease_expired"
            task.updated_at = stamp
            requeued += 1
            emit(
                session,
                "TASK_REQUEUED",
                job_id=task.job_id,
                task_id=task.id,
                reason="worker_lease_expired",
                attempt=task.attempt,
            )
        else:
            task.status = "DEAD"
            task.lease_expires_at = None
            task.finished_at = stamp
            task.updated_at = stamp
            task.error = "worker_lease_expired_and_attempts_exhausted"
            dead += 1
            emit(
                session,
                "TASK_DEAD",
                job_id=task.job_id,
                task_id=task.id,
                reason="lease_expired",
            )

    offline_after = stamp - timedelta(seconds=settings.worker_offline_after)
    offline = session.execute(
        update(Worker)
        .where(Worker.status == "ONLINE", Worker.last_heartbeat_at < offline_after)
        .values(status="OFFLINE")
    ).rowcount

    session.commit()
    return {"requeued": requeued, "dead": dead, "workers_offline": offline}


def prune_workers(session: Session, keep_hours: int = 24) -> int:
    cutoff = naive(now()) - timedelta(hours=keep_hours)
    result = session.execute(
        Worker.__table__.delete().where(
            Worker.status == "OFFLINE", Worker.last_heartbeat_at < cutoff
        )
    )
    session.commit()
    return result.rowcount
