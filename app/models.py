"""持久化模型。

表设计要点：
- `tasks` 上的唯一索引 `uq_task_fire_shard` 是"至多物化一次"的硬约束；
- `tasks` 的 (status, next_retry_at) 复合索引支撑领取扫描；
- `jobs.version` 做乐观锁，避免并发编辑互相覆盖；
- `events` 是追加型事件流，供外部系统按游标消费（不强依赖 MQ）。
"""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from .db import Base


def new_id() -> str:
    return uuid.uuid4().hex


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    name: Mapped[str] = mapped_column(String(120), unique=True, nullable=False)
    description: Mapped[str] = mapped_column(Text, default="")

    # 触发方式：cron / interval / once
    schedule_type: Mapped[str] = mapped_column(String(16), default="cron", nullable=False)
    cron_expr: Mapped[str | None] = mapped_column(String(120))
    interval_seconds: Mapped[int | None] = mapped_column(Integer)
    run_at: Mapped[datetime | None] = mapped_column(DateTime)
    timezone: Mapped[str] = mapped_column(String(64), default="Asia/Shanghai")

    # 执行体：shell / http
    executor: Mapped[str] = mapped_column(String(16), default="shell", nullable=False)
    command: Mapped[str | None] = mapped_column(Text)
    http_url: Mapped[str | None] = mapped_column(String(500))
    http_method: Mapped[str | None] = mapped_column(String(10), default="POST")
    http_headers: Mapped[dict | None] = mapped_column(JSON, default=dict)
    payload: Mapped[dict | None] = mapped_column(JSON, default=dict)

    timeout_seconds: Mapped[int] = mapped_column(Integer, default=60)
    shard_count: Mapped[int] = mapped_column(Integer, default=1)
    max_concurrency: Mapped[int] = mapped_column(Integer, default=1)
    max_attempts: Mapped[int] = mapped_column(Integer, default=3)
    backoff_base_seconds: Mapped[float] = mapped_column(Float, default=5.0)
    backoff_cap_seconds: Mapped[float] = mapped_column(Float, default=300.0)
    # 停机补偿策略：skip=只补最近一期，all=逐期补齐（受 max_catch_up 限制）
    catch_up: Mapped[str] = mapped_column(String(8), default="skip")
    priority: Mapped[int] = mapped_column(Integer, default=0)

    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    next_fire_at: Mapped[datetime | None] = mapped_column(DateTime)
    last_fire_at: Mapped[datetime | None] = mapped_column(DateTime)

    created_by: Mapped[str] = mapped_column(String(64), default="system")
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)

    __table_args__ = (Index("ix_jobs_due", "enabled", "next_fire_at"),)


class Task(Base):
    __tablename__ = "tasks"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    job_id: Mapped[str] = mapped_column(String(32), nullable=False)
    fire_time: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    shard_index: Mapped[int] = mapped_column(Integer, default=0)
    shard_total: Mapped[int] = mapped_column(Integer, default=1)
    idempotency_key: Mapped[str] = mapped_column(String(160), nullable=False, unique=True)
    # 单次运行参数：优先于 job.payload，用于手工触发/回填
    payload: Mapped[dict | None] = mapped_column(JSON, default=dict)

    status: Mapped[str] = mapped_column(String(16), default="PENDING", nullable=False)
    priority: Mapped[int] = mapped_column(Integer, default=0)
    attempt: Mapped[int] = mapped_column(Integer, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, default=3)

    scheduled_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    next_retry_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime)

    worker_id: Mapped[str | None] = mapped_column(String(64))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime)

    exit_code: Mapped[int | None] = mapped_column(Integer)
    output: Mapped[str | None] = mapped_column(Text)
    error: Mapped[str | None] = mapped_column(Text)
    duration_ms: Mapped[int | None] = mapped_column(Integer)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    __table_args__ = (
        UniqueConstraint("job_id", "fire_time", "shard_index", name="uq_task_fire_shard"),
        Index("ix_tasks_claim", "status", "next_retry_at"),
        Index("ix_tasks_job", "job_id", "created_at"),
        Index("ix_tasks_lease", "status", "lease_expires_at"),
    )


class Worker(Base):
    __tablename__ = "workers"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    hostname: Mapped[str] = mapped_column(String(120), default="")
    pool: Mapped[str] = mapped_column(String(60), default="default")
    capacity: Mapped[int] = mapped_column(Integer, default=4)
    status: Mapped[str] = mapped_column(String(16), default="ONLINE")
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    last_heartbeat_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    version: Mapped[str] = mapped_column(String(40), default="1.0.0")


class Event(Base):
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    type: Mapped[str] = mapped_column(String(40), nullable=False)
    job_id: Mapped[str | None] = mapped_column(String(32))
    task_id: Mapped[str | None] = mapped_column(String(32))
    worker_id: Mapped[str | None] = mapped_column(String(64))
    payload: Mapped[dict | None] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    __table_args__ = (Index("ix_events_id", "id"),)


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    actor: Mapped[str] = mapped_column(String(64), default="unknown")
    action: Mapped[str] = mapped_column(String(60), nullable=False)
    target_type: Mapped[str] = mapped_column(String(30), default="")
    target_id: Mapped[str] = mapped_column(String(64), default="")
    detail: Mapped[dict | None] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class ApiKey(Base):
    __tablename__ = "api_keys"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    name: Mapped[str] = mapped_column(String(80), nullable=False)
    prefix: Mapped[str] = mapped_column(String(16), nullable=False)
    key_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    role: Mapped[str] = mapped_column(String(16), default="viewer", nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime)


class SchedulerLease(Base):
    """单行表（id 恒为 1）：主节点选举与 fencing token。"""

    __tablename__ = "scheduler_lease"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    holder: Mapped[str] = mapped_column(String(64), default="")
    fencing_token: Mapped[int] = mapped_column(Integer, default=0)
    acquired_at: Mapped[datetime | None] = mapped_column(DateTime)
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
