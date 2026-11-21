r"""任务状态机与重试退避。

    PENDING --claim--> RUNNING --ok--> SUCCEEDED
                          |  \--fail(还有次数)--> PENDING(延迟重试)
                          |   \--fail(次数用尽)--> DEAD
                          \--lease 过期--> PENDING / DEAD
    PENDING/RUNNING --cancel--> CANCELLED
    DEAD/FAILED --replay--> PENDING

失败重试采用指数退避 + 抖动：避免大量任务在同一秒再次撞库形成重试风暴。
"""
from __future__ import annotations

import random
from datetime import timedelta

from sqlalchemy.orm import Session

from ..clock import naive, now
from ..config import settings
from ..events import audit, emit
from ..metrics import METRICS
from ..models import Job, Task

TERMINAL = ("SUCCEEDED", "FAILED", "DEAD", "CANCELLED")


def truncate(text: str | None, limit: int | None = None) -> str | None:
    if text is None:
        return None
    cap = limit or settings.output_limit
    if len(text) <= cap:
        return text
    return text[:cap] + f"\n...[截断，原始长度 {len(text)}]"


def backoff_delay(
    attempt: int,
    base: float = 5.0,
    cap: float = 300.0,
    rng: random.Random | None = None,
) -> float:
    """第 attempt 次失败后的等待秒数：min(cap, base*2^(attempt-1)) * [0.85,1.15)。"""
    generator = rng or random
    exponent = max(0, attempt - 1)
    raw = min(cap, base * (2**exponent))
    jitter = 0.85 + 0.30 * generator.random()
    return round(min(cap, raw * jitter), 3)


def complete_task(
    session: Session,
    task_id: str,
    worker_id: str,
    *,
    ok: bool,
    exit_code: int | None = None,
    output: str | None = None,
    error: str | None = None,
    duration_ms: int | None = None,
) -> dict | None:
    """上报执行结果。返回 None 表示任务已不归该 worker（租约丢失/被取消），结果作废。"""
    task = session.get(Task, task_id)
    if task is None or task.status != "RUNNING" or task.worker_id != worker_id:
        return None

    stamp = naive(now())
    task.exit_code = exit_code
    task.output = truncate(output)
    task.duration_ms = duration_ms
    task.updated_at = stamp
    job = session.get(Job, task.job_id)

    if ok:
        task.status = "SUCCEEDED"
        task.finished_at = stamp
        task.error = None
        task.lease_expires_at = None
        emit(
            session,
            "TASK_SUCCEEDED",
            job_id=task.job_id,
            task_id=task.id,
            worker_id=worker_id,
            duration_ms=duration_ms,
        )
        session.commit()
        METRICS.inc("tempo_task_total", {"status": "succeeded"})
        return {"status": task.status, "attempt": task.attempt}

    task.error = truncate(error or "execution_failed", 2000)
    max_attempts = task.max_attempts or 1

    if task.attempt < max_attempts:
        delay = backoff_delay(
            task.attempt,
            (job.backoff_base_seconds if job else 5.0),
            (job.backoff_cap_seconds if job else 300.0),
        )
        task.status = "PENDING"
        task.worker_id = None
        task.lease_expires_at = None
        task.finished_at = None
        task.next_retry_at = stamp + timedelta(seconds=delay)
        emit(
            session,
            "TASK_RETRY_SCHEDULED",
            job_id=task.job_id,
            task_id=task.id,
            worker_id=worker_id,
            attempt=task.attempt,
            delay_seconds=delay,
        )
        session.commit()
        METRICS.inc("tempo_task_total", {"status": "retried"})
        return {"status": "PENDING", "attempt": task.attempt, "retry_in": delay}

    task.status = "DEAD"
    # 保留最后一次执行的 worker，便于排障（租约已释放）
    task.lease_expires_at = None
    task.finished_at = stamp
    emit(session, "TASK_DEAD", job_id=task.job_id, task_id=task.id, reason="attempts_exhausted")
    session.commit()
    METRICS.inc("tempo_task_total", {"status": "dead"})
    return {"status": "DEAD", "attempt": task.attempt}


def replay_task(session: Session, task_id: str, actor: str) -> Task | None:
    task = session.get(Task, task_id)
    if task is None or task.status not in ("DEAD", "FAILED", "CANCELLED"):
        return None
    stamp = naive(now())
    task.status = "PENDING"
    task.attempt = 0
    task.worker_id = None
    task.lease_expires_at = None
    task.next_retry_at = stamp
    task.finished_at = None
    task.error = None
    task.output = None
    task.updated_at = stamp
    emit(session, "TASK_REPLAYED", job_id=task.job_id, task_id=task.id, actor=actor)
    audit(session, actor, "TASK_REPLAY", target_type="task", target_id=task.id)
    session.commit()
    return task


def cancel_task(session: Session, task_id: str, actor: str) -> Task | None:
    task = session.get(Task, task_id)
    if task is None or task.status in TERMINAL:
        return None
    stamp = naive(now())
    task.status = "CANCELLED"
    task.finished_at = stamp
    task.updated_at = stamp
    task.lease_expires_at = None
    emit(session, "TASK_CANCELLED", job_id=task.job_id, task_id=task.id, actor=actor)
    audit(session, actor, "TASK_CANCEL", target_type="task", target_id=task.id)
    session.commit()
    return task
