"""请求/响应模型（Pydantic v2）。"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_serializer, model_validator

from .clock import iso
from .services.triggers import ScheduleError, validate_schedule


class JobCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    description: str = ""
    schedule_type: Literal["cron", "interval", "once"] = "cron"
    cron_expr: str | None = Field(default=None, max_length=120)
    interval_seconds: int | None = Field(default=None, ge=1, le=2_592_000)
    run_at: datetime | None = None
    timezone: str = Field(default="Asia/Shanghai", max_length=64)

    executor: Literal["shell", "http"] = "shell"
    command: str | None = Field(default=None, max_length=4000)
    http_url: str | None = Field(default=None, max_length=500)
    http_method: str = Field(default="POST", max_length=10)
    http_headers: dict[str, str] = Field(default_factory=dict)
    payload: dict[str, Any] = Field(default_factory=dict)

    timeout_seconds: int = Field(default=60, ge=1, le=86_400)
    shard_count: int = Field(default=1, ge=1, le=200)
    max_concurrency: int = Field(default=1, ge=0, le=100)
    max_attempts: int = Field(default=3, ge=1, le=20)
    backoff_base_seconds: float = Field(default=5.0, ge=0.1, le=3600)
    backoff_cap_seconds: float = Field(default=300.0, ge=1, le=86_400)
    catch_up: Literal["skip", "all"] = "skip"
    priority: int = Field(default=0, ge=-100, le=100)
    enabled: bool = True

    @model_validator(mode="after")
    def _check(self) -> "JobCreate":
        try:
            validate_schedule(
                self.schedule_type,
                cron_expr=self.cron_expr,
                interval_seconds=self.interval_seconds,
                run_at=self.run_at,
                timezone_name=self.timezone,
            )
        except ScheduleError as exc:
            raise ValueError(str(exc)) from exc
        if self.executor == "shell" and not (self.command or "").strip():
            raise ValueError("shell 执行器必须提供 command")
        if self.executor == "http" and not (self.http_url or "").strip():
            raise ValueError("http 执行器必须提供 http_url")
        if self.backoff_cap_seconds < self.backoff_base_seconds:
            raise ValueError("backoff_cap_seconds 不能小于 backoff_base_seconds")
        return self


class JobUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=120)
    description: str | None = None
    schedule_type: Literal["cron", "interval", "once"] | None = None
    cron_expr: str | None = None
    interval_seconds: int | None = Field(default=None, ge=1, le=2_592_000)
    run_at: datetime | None = None
    timezone: str | None = None
    executor: Literal["shell", "http"] | None = None
    command: str | None = None
    http_url: str | None = None
    http_method: str | None = None
    http_headers: dict[str, str] | None = None
    payload: dict[str, Any] | None = None
    timeout_seconds: int | None = Field(default=None, ge=1, le=86_400)
    shard_count: int | None = Field(default=None, ge=1, le=200)
    max_concurrency: int | None = Field(default=None, ge=0, le=100)
    max_attempts: int | None = Field(default=None, ge=1, le=20)
    backoff_base_seconds: float | None = Field(default=None, ge=0.1, le=3600)
    backoff_cap_seconds: float | None = Field(default=None, ge=1, le=86_400)
    catch_up: Literal["skip", "all"] | None = None
    priority: int | None = Field(default=None, ge=-100, le=100)
    enabled: bool | None = None
    version: int | None = Field(default=None, ge=1)


class TriggerRequest(BaseModel):
    payload: dict[str, Any] | None = None
    shard_count: int | None = Field(default=None, ge=1, le=200)


class JobOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    description: str
    schedule_type: str
    cron_expr: str | None
    interval_seconds: int | None
    run_at: datetime | None
    timezone: str
    executor: str
    command: str | None
    http_url: str | None
    http_method: str | None
    payload: dict | None
    timeout_seconds: int
    shard_count: int
    max_concurrency: int
    max_attempts: int
    backoff_base_seconds: float
    backoff_cap_seconds: float
    catch_up: str
    priority: int
    enabled: bool
    next_fire_at: datetime | None
    last_fire_at: datetime | None
    created_by: str
    created_at: datetime
    updated_at: datetime
    version: int

    @field_serializer(
        "run_at", "next_fire_at", "last_fire_at", "created_at", "updated_at"
    )
    def _dt(self, value: datetime | None) -> str | None:
        return iso(value)


class TaskOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    job_id: str
    fire_time: datetime
    shard_index: int
    shard_total: int
    status: str
    priority: int
    attempt: int
    max_attempts: int
    scheduled_at: datetime
    next_retry_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    worker_id: str | None
    exit_code: int | None
    duration_ms: int | None
    error: str | None
    created_at: datetime

    @field_serializer(
        "fire_time", "scheduled_at", "next_retry_at", "started_at", "finished_at", "created_at"
    )
    def _dt(self, value: datetime | None) -> str | None:
        return iso(value)


class TaskDetail(TaskOut):
    output: str | None
    payload: dict | None
    idempotency_key: str


class WorkerOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    hostname: str
    pool: str
    capacity: int
    status: str
    started_at: datetime
    last_heartbeat_at: datetime

    @field_serializer("started_at", "last_heartbeat_at")
    def _dt(self, value: datetime | None) -> str | None:
        return iso(value)


class EventOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    type: str
    job_id: str | None
    task_id: str | None
    worker_id: str | None
    payload: dict | None
    created_at: datetime

    @field_serializer("created_at")
    def _dt(self, value: datetime | None) -> str | None:
        return iso(value)


class AuditOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    actor: str
    action: str
    target_type: str
    target_id: str
    detail: dict | None
    created_at: datetime

    @field_serializer("created_at")
    def _dt(self, value: datetime | None) -> str | None:
        return iso(value)


class Page(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[dict]


class ApiKeyCreate(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    role: Literal["viewer", "operator", "admin"] = "viewer"


class ApiKeyOut(BaseModel):
    id: str
    name: str
    prefix: str
    role: str
    created_at: datetime
    last_used_at: datetime | None
    revoked_at: datetime | None

    @field_serializer("created_at", "last_used_at", "revoked_at")
    def _dt(self, value: datetime | None) -> str | None:
        return iso(value)
