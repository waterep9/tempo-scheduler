"""基础压测：调度物化、任务领取、API 延迟、端到端执行吞吐。

    python benchmark.py --jobs 300 --tasks 2000

使用临时目录下的独立 SQLite 库，不影响业务数据。
"""
from __future__ import annotations

import argparse
import os
import statistics
import tempfile
import threading
import time
from pathlib import Path

_TMP = Path(tempfile.mkdtemp(prefix="tempo-bench-"))
os.environ["DATABASE_URL"] = f"sqlite:///{(_TMP / 'bench.db').as_posix()}"
os.environ["TEMPO_EMBEDDED_PLANNER"] = "0"
os.environ["TEMPO_API_RATE_LIMIT"] = "1000000"
os.environ["TEMPO_BOOTSTRAP_TOKEN"] = "bench-token"

from datetime import timedelta  # noqa: E402

from sqlalchemy import func, select  # noqa: E402

from app.clock import naive, now  # noqa: E402
from app.cron import CronExpr  # noqa: E402
from app.db import SessionLocal, init_db  # noqa: E402
from app.models import Job, Task  # noqa: E402
from app.services.jobs import create_job  # noqa: E402
from app.services.lease import claim_tasks  # noqa: E402
from app.security import ensure_bootstrap_key  # noqa: E402
from app.services.planner import Planner  # noqa: E402
from app.worker import Worker  # noqa: E402


def _line(title: str) -> None:
    print(f"\n=== {title} ===")


def bench_cron(iterations: int = 20000) -> None:
    _line("Cron 解析与下次触发点")
    expr = CronExpr.parse("*/5 9-18 * * MON-FRI")
    moment = now()
    started = time.perf_counter()
    for _ in range(iterations):
        moment = expr.next_after(moment, moment.tzinfo)
    elapsed = time.perf_counter() - started
    print(f"迭代 {iterations} 次，耗时 {elapsed:.2f}s，约 {iterations / max(elapsed, 1e-9):,.0f} 次/秒")


def bench_planner(jobs: int = 300) -> float:
    _line("调度物化（单轮扫描 N 个到期任务）")
    init_db()
    with SessionLocal() as session:
        ensure_bootstrap_key(session)
    templates = [
        ("*/5 * * * *", 2),
        ("*/15 * * * *", 4),
        ("0 */2 * * *", 1),
    ]
    with SessionLocal() as session:
        for idx in range(jobs):
            cron_expr, shards = templates[idx % len(templates)]
            create_job(
                session,
                {
                    "name": f"bench-job-{idx}",
                    "schedule_type": "cron",
                    "cron_expr": cron_expr,
                    "timezone": "Asia/Shanghai",
                    "executor": "shell",
                    "command": "echo bench",
                    "shard_count": shards,
                    "max_concurrency": shards,
                },
                "bench",
            )
        session.query(Job).update({Job.next_fire_at: naive(now() - timedelta(seconds=5))})
        session.commit()

    planner = Planner()
    started = time.perf_counter()
    result = planner.tick()
    elapsed = time.perf_counter() - started
    with SessionLocal() as session:
        total = session.scalar(select(func.count()).select_from(Task)) or 0
    rate = result["tasks_created"] / max(elapsed, 1e-9)
    print(
        f"到期任务 {jobs} 个，物化 {result['tasks_created']} 个实例（库内共 {total}），"
        f"耗时 {elapsed * 1000:.0f}ms，{rate:,.0f} 实例/秒"
    )
    return elapsed


def bench_claim(tasks: int = 2000, workers: int = 4, batch: int = 50) -> None:
    _line("并发领取（多 worker 抢同一队列）")
    with SessionLocal() as session:
        job = session.scalars(select(Job).limit(1)).first()
        stamp = naive(now())
        rows = [
            Task(
                job_id=job.id,
                fire_time=stamp - timedelta(seconds=idx),
                shard_index=0,
                shard_total=1,
                idempotency_key=f"bench-claim-{idx}",
                status="PENDING",
                priority=0,
                attempt=0,
                max_attempts=3,
                scheduled_at=stamp,
                next_retry_at=stamp,
                created_at=stamp,
                updated_at=stamp,
            )
            for idx in range(tasks)
        ]
        session.add_all(rows)
        session.commit()

    claimed: list[str] = []
    lock = threading.Lock()
    failures: list[str] = []

    def run_worker(worker_id: str) -> None:
        while True:
            with SessionLocal() as session:
                got = claim_tasks(session, worker_id, batch)
            if not got:
                return
            with lock:
                claimed.extend(t.id for t in got)
            if len(got) < batch:
                time.sleep(0)

    threads = [threading.Thread(target=run_worker, args=(f"bench-w{i}",)) for i in range(workers)]
    started = time.perf_counter()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    elapsed = time.perf_counter() - started

    total = len(claimed)
    duplicates = total - len(set(claimed))
    if duplicates:
        failures.append(f"重复领取 {duplicates} 个任务")
    print(
        f"{workers} 个 worker 领取 {total} 个任务，耗时 {elapsed:.2f}s，"
        f"{total / max(elapsed, 1e-9):,.0f} 任务/秒，重复 {duplicates}"
    )


def bench_api(iterations: int = 200) -> None:
    _line("控制台读接口延迟（TestClient）")
    from fastapi.testclient import TestClient

    from app.main import app

    client = TestClient(app)
    headers = {"Authorization": "Bearer bench-token"}
    targets = ["/api/v1/stats", "/api/v1/runs?page_size=50", "/api/v1/jobs?page_size=50"]
    for path in targets:
        samples = []
        for _ in range(iterations):
            started = time.perf_counter()
            response = client.get(path, headers=headers)
            samples.append((time.perf_counter() - started) * 1000)
            assert response.status_code == 200, response.text
        samples.sort()
        p50 = statistics.median(samples)
        p95 = samples[int(len(samples) * 0.95) - 1]
        print(f"{path:32s} p50={p50:6.2f}ms p95={p95:6.2f}ms")


def bench_worker(batch: int = 200, capacity: int = 8) -> None:
    _line("端到端执行吞吐（shell echo）")
    with SessionLocal() as session:
        job = create_job(
            session,
            {
                "name": "bench-exec",
                "schedule_type": "interval",
                "interval_seconds": 3600,
                "executor": "shell",
                "command": "echo bench-exec",
                "shard_count": 1,
                "max_concurrency": capacity,
            },
            "bench",
        )
        stamp = naive(now())
        session.add_all(
            [
                Task(
                    job_id=job.id,
                    fire_time=stamp - timedelta(seconds=idx),
                    shard_index=0,
                    shard_total=1,
                    idempotency_key=f"bench-exec-{idx}",
                    status="PENDING",
                    priority=0,
                    attempt=0,
                    max_attempts=1,
                    scheduled_at=stamp,
                    next_retry_at=stamp,
                    created_at=stamp,
                    updated_at=stamp,
                )
                for idx in range(batch)
            ]
        )
        session.commit()

    worker = Worker(worker_id="bench-worker", capacity=capacity)
    started = time.perf_counter()
    handled = 0
    while True:
        done = worker.run_once()
        handled += done
        if done == 0:
            break
    elapsed = time.perf_counter() - started

    with SessionLocal() as session:
        ok = session.scalar(
            select(func.count())
            .select_from(Task)
            .where(Task.job_id == job.id, Task.status == "SUCCEEDED")
        )
    print(
        f"执行 {handled} 个任务，成功 {ok}，并发 {capacity}，耗时 {elapsed:.2f}s，"
        f"{handled / max(elapsed, 1e-9):,.0f} 任务/秒"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--jobs", type=int, default=300)
    parser.add_argument("--tasks", type=int, default=2000)
    parser.add_argument("--batch", type=int, default=200)
    args = parser.parse_args()

    print(f"临时库：{_TMP / 'bench.db'}")
    bench_cron()
    bench_planner(args.jobs)
    bench_claim(args.tasks)
    bench_api()
    bench_worker(args.batch)


if __name__ == "__main__":
    main()
