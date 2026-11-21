"""端到端：调度 -> 领取 -> 真实执行 -> 结果落库，含失败重试与崩溃恢复。"""
from __future__ import annotations

import threading
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from sqlalchemy import select

from app.clock import naive, now
from app.executors import execute
from app.models import Task
from app.services import runner
from app.services.lease import claim_tasks
from app.services.planner import Planner
from app.worker import Worker


class _HookHandler(BaseHTTPRequestHandler):
    received: list[str] = []

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length") or 0)
        _HookHandler.received.append(self.rfile.read(length).decode("utf-8"))
        body = b'{"ok": true}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):  # 静默
        return


@pytest.fixture
def http_endpoint():
    _HookHandler.received = []
    server = HTTPServer(("127.0.0.1", 0), _HookHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/hook"
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture
def worker():
    return Worker(worker_id="worker-e2e", capacity=4, poll_interval=0.1)


def tasks_of(db, job_id):
    return db.scalars(select(Task).where(Task.job_id == job_id)).all()


def test_shell_job_runs_end_to_end(due_job, db, worker):
    job = due_job(command="echo tempo-e2e-ok", shard_count=2, max_concurrency=2)

    assert Planner().tick()["tasks_created"] == 2
    assert worker.run_once() == 2

    db.expire_all()
    tasks = tasks_of(db, job.id)
    assert len(tasks) == 2
    assert {t.status for t in tasks} == {"SUCCEEDED"}
    assert all("tempo-e2e-ok" in (t.output or "") for t in tasks)
    assert all(t.exit_code == 0 for t in tasks)
    assert all(t.worker_id == "worker-e2e" for t in tasks)


def test_failing_job_retries_then_dies(due_job, db, worker):
    job = due_job(command="exit 3", max_attempts=2, max_concurrency=1)
    Planner().tick()

    assert worker.run_once() == 1
    db.expire_all()
    task = tasks_of(db, job.id)[0]
    assert task.status == "PENDING"
    assert task.attempt == 1
    assert task.error and "exit_code=3" in task.error
    assert task.next_retry_at > naive(now())

    # 跳过退避等待，直接允许重试
    task.next_retry_at = naive(now()) - timedelta(seconds=1)
    db.commit()
    assert worker.run_once() == 1

    db.expire_all()
    task = tasks_of(db, job.id)[0]
    assert task.status == "DEAD"
    assert task.attempt == 2

    # 死信可人工重放
    replayed = runner.replay_task(db, task.id, "ops")
    assert replayed.status == "PENDING"
    assert replayed.attempt == 0


def test_http_executor_posts_payload(due_job, db, worker, http_endpoint):
    job = due_job(
        executor="http",
        command=None,
        http_url=http_endpoint,
        http_method="POST",
        payload={"biz_date": "2026-11-21"},
    )
    Planner().tick()
    assert worker.run_once() == 1

    db.expire_all()
    task = tasks_of(db, job.id)[0]
    assert task.status == "SUCCEEDED", task.error
    assert "HTTP 200" in (task.output or "")
    assert '"ok": true' in (task.output or "")
    assert _HookHandler.received
    assert "biz_date" in _HookHandler.received[0]


def test_worker_crash_recovery(due_job, db, worker):
    """worker 领取后崩溃：租约过期 -> 回收重投 -> 另一个 worker 执行成功。"""
    job = due_job(command="echo recovered", max_attempts=3)
    Planner().tick()

    ghost = claim_tasks(db, "ghost-worker", 1, lease_ttl=1)[0]
    assert ghost.status == "RUNNING"
    ghost.lease_expires_at = naive(now()) - timedelta(seconds=1)
    db.commit()

    result = Planner().tick()
    assert result["requeued"] == 1

    assert worker.run_once() == 1
    db.expire_all()
    task = tasks_of(db, job.id)[0]
    assert task.status == "SUCCEEDED"
    assert task.attempt == 2
    assert task.worker_id == "worker-e2e"


def test_executor_timeout_is_reported(due_job, worker):
    job = due_job(command='python -c "import time; time.sleep(5)"', timeout_seconds=1)
    snapshot = type("S", (), {
        "executor": "shell",
        "command": job.command,
        "http_url": None,
        "http_method": "POST",
        "http_headers": {},
        "payload": {},
        "timeout_seconds": 1,
        "id": job.id,
        "name": job.name,
    })()
    task = type("T", (), {
        "id": "t1",
        "payload": {},
        "attempt": 1,
        "shard_index": 0,
        "shard_total": 1,
        "fire_time": naive(now()),
    })()
    result = execute(snapshot, task, timeout=1)
    assert result.ok is False
    assert "timeout" in (result.error or "")
