"""事件流与审计。

任务状态机的每次流转都会落一条 `events` 记录，外部系统可通过
`GET /api/v1/events?after_id=N` 做游标消费，或以轮询方式实现轻量订阅。
这里刻意不引入 Kafka：单库事务内写事件即可获得"业务与事件同源"的一致性，
后续若需要外发，可在此表上挂 CDC。
"""
from __future__ import annotations

from sqlalchemy.orm import Session

from .clock import now
from .models import AuditLog, Event


def emit(
    session: Session,
    event_type: str,
    *,
    job_id: str | None = None,
    task_id: str | None = None,
    worker_id: str | None = None,
    **payload,
) -> Event:
    event = Event(
        type=event_type,
        job_id=job_id,
        task_id=task_id,
        worker_id=worker_id,
        payload=payload or {},
        created_at=now().replace(tzinfo=None),
    )
    session.add(event)
    return event


def audit(
    session: Session,
    actor: str,
    action: str,
    *,
    target_type: str = "",
    target_id: str = "",
    **detail,
) -> AuditLog:
    record = AuditLog(
        actor=actor or "unknown",
        action=action,
        target_type=target_type,
        target_id=target_id,
        detail=detail or {},
        created_at=now().replace(tzinfo=None),
    )
    session.add(record)
    return record
