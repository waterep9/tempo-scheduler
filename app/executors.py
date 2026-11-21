"""执行器：把一次任务实例真正跑起来。

- shell：调用系统 shell，注入 TEMPO_* 环境变量与分片信息；
- http：把 job.payload / task.payload 作为 JSON body 发出请求。

安全边界：shell 执行器等价于在宿主上执行任意命令，只应对受信任的
管理员开放（本项目的 API Key 分角色就是为此设计的）。生产环境建议
把 worker 放进受限容器/沙箱，或只用 http 执行器。
"""
from __future__ import annotations

import json
import os
import subprocess
import time
from dataclasses import dataclass

import httpx

from .config import settings
from .models import Job, Task

MAX_INLINE_OUTPUT = 64 * 1024


@dataclass
class ExecutionResult:
    ok: bool
    exit_code: int | None
    output: str
    error: str | None
    duration_ms: int


def merged_payload(job: Job, task: Task) -> dict:
    payload: dict = {}
    if isinstance(job.payload, dict):
        payload.update(job.payload)
    if isinstance(task.payload, dict):
        payload.update(task.payload)
    return payload


def _environment(job: Job, task: Task) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "TEMPO_JOB_ID": job.id,
            "TEMPO_JOB_NAME": job.name,
            "TEMPO_TASK_ID": task.id,
            "TEMPO_ATTEMPT": str(task.attempt),
            "TEMPO_SHARD_INDEX": str(task.shard_index),
            "TEMPO_SHARD_TOTAL": str(task.shard_total),
            "TEMPO_FIRE_TIME": task.fire_time.isoformat() if task.fire_time else "",
            "TEMPO_PAYLOAD": json.dumps(merged_payload(job, task), ensure_ascii=False),
        }
    )
    return env


def _run_shell(job: Job, task: Task, timeout: float) -> ExecutionResult:
    command = (job.command or "").strip()
    if not command:
        return ExecutionResult(False, None, "", "shell 执行器缺少 command", 0)
    started = time.perf_counter()
    try:
        completed = subprocess.run(  # noqa: S602 - 受控管理面执行
            command,
            shell=True,
            capture_output=True,
            text=True,
            errors="replace",
            timeout=timeout,
            env=_environment(job, task),
        )
        duration = int((time.perf_counter() - started) * 1000)
        output = (completed.stdout or "") + (completed.stderr or "")
        return ExecutionResult(
            ok=completed.returncode == 0,
            exit_code=completed.returncode,
            output=output[-MAX_INLINE_OUTPUT:],
            error=None if completed.returncode == 0 else f"exit_code={completed.returncode}",
            duration_ms=duration,
        )
    except subprocess.TimeoutExpired as exc:
        duration = int((time.perf_counter() - started) * 1000)
        partial = ""
        if exc.stdout:
            partial = exc.stdout if isinstance(exc.stdout, str) else exc.stdout.decode(errors="replace")
        return ExecutionResult(False, None, partial[-MAX_INLINE_OUTPUT:], f"timeout>{timeout}s", duration)
    except Exception as exc:  # pragma: no cover - 平台相关
        duration = int((time.perf_counter() - started) * 1000)
        return ExecutionResult(False, None, "", f"executor_error: {exc}", duration)


def _run_http(job: Job, task: Task, timeout: float) -> ExecutionResult:
    if not job.http_url:
        return ExecutionResult(False, None, "", "http 执行器缺少 http_url", 0)
    method = (job.http_method or "POST").upper()
    headers = {"User-Agent": "tempo-scheduler/1.0"}
    if isinstance(job.http_headers, dict):
        headers.update({str(k): str(v) for k, v in job.http_headers.items()})
    payload = merged_payload(job, task)
    started = time.perf_counter()
    try:
        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            response = client.request(
                method,
                job.http_url,
                headers=headers,
                json=None if method in ("GET", "HEAD") else payload,
                params=payload if method in ("GET", "HEAD") else None,
            )
        duration = int((time.perf_counter() - started) * 1000)
        body = response.text[:MAX_INLINE_OUTPUT]
        ok = 200 <= response.status_code < 300
        return ExecutionResult(
            ok=ok,
            exit_code=response.status_code,
            output=f"HTTP {response.status_code}\n{body}",
            error=None if ok else f"http_status={response.status_code}",
            duration_ms=duration,
        )
    except httpx.TimeoutException:
        duration = int((time.perf_counter() - started) * 1000)
        return ExecutionResult(False, None, "", f"timeout>{timeout}s", duration)
    except Exception as exc:
        duration = int((time.perf_counter() - started) * 1000)
        return ExecutionResult(False, None, "", f"http_error: {exc}", duration)


def execute(job: Job, task: Task, timeout: float | None = None) -> ExecutionResult:
    limit = float(timeout or job.timeout_seconds or settings.default_timeout)
    if job.executor == "http":
        return _run_http(job, task, limit)
    return _run_shell(job, task, limit)
