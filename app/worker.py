"""执行器进程：领取任务 -> 执行 -> 上报结果，并周期性续约心跳。

多开即分布式：多个 worker 共享同一数据库即可水平扩容，无需额外协调服务。
worker 崩溃时任务不会丢：租约到期后由调度器的回收器重新投递（至少一次）。

用法：
    python -m app.worker --capacity 4
    python -m app.worker --once          # 处理一轮后退出（便于测试/demo）
"""
from __future__ import annotations

import argparse
import logging
import signal
import socket
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from .clock import now
from .config import settings
from .db import init_db, session_scope
from .executors import execute
from .models import Job, Task
from .services.lease import claim_tasks, heartbeat, register_worker
from .services.runner import complete_task

log = logging.getLogger("tempo.worker")


@dataclass
class JobSnapshot:
    """执行期间脱离 Session 的任务定义副本，避免长时间占用数据库连接。"""

    id: str
    name: str
    executor: str
    command: str | None
    http_url: str | None
    http_method: str | None
    http_headers: dict | None
    payload: dict | None
    timeout_seconds: int

    @classmethod
    def of(cls, job: Job) -> "JobSnapshot":
        return cls(
            id=job.id,
            name=job.name,
            executor=job.executor,
            command=job.command,
            http_url=job.http_url,
            http_method=job.http_method,
            http_headers=dict(job.http_headers or {}),
            payload=dict(job.payload or {}),
            timeout_seconds=job.timeout_seconds or settings.default_timeout,
        )


class Worker:
    def __init__(
        self,
        worker_id: str | None = None,
        capacity: int = 4,
        poll_interval: float = 1.0,
        pool: str = "default",
        lease_ttl: float | None = None,
    ) -> None:
        self.id = worker_id or f"worker-{socket.gethostname()}-{uuid.uuid4().hex[:6]}"
        self.capacity = max(1, capacity)
        self.poll_interval = max(0.05, poll_interval)
        self.pool = pool
        self.lease_ttl = lease_ttl or settings.task_lease_ttl
        self._stop = threading.Event()
        self._executor = ThreadPoolExecutor(
            max_workers=self.capacity, thread_name_prefix="tempo-task"
        )
        self._inflight: set[str] = set()
        self._lock = threading.Lock()
        self.stats = {"claimed": 0, "succeeded": 0, "failed": 0}
        self._heartbeat_thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    def start(self) -> None:
        with session_scope() as session:
            register_worker(session, self.id, self.capacity, self.pool)
        log.info("worker %s 已注册，容量 %s", self.id, self.capacity)
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, name="tempo-heartbeat", daemon=True
        )
        self._heartbeat_thread.start()
        try:
            while not self._stop.is_set():
                claimed = self.poll_once()
                if claimed == 0:
                    self._stop.wait(self.poll_interval)
        finally:
            self._shutdown()

    def stop(self) -> None:
        self._stop.set()

    def _shutdown(self) -> None:
        log.info("worker %s 正在排空…", self.id)
        self._executor.shutdown(wait=True, cancel_futures=False)
        log.info("worker %s 已停止，统计 %s", self.id, self.stats)

    # ------------------------------------------------------------------
    def poll_once(self) -> int:
        with self._lock:
            free = self.capacity - len(self._inflight)
        if free <= 0:
            return 0

        with session_scope() as session:
            tasks = claim_tasks(session, self.id, free, self.lease_ttl)

        for task in tasks:
            with self._lock:
                self._inflight.add(task.id)
                self.stats["claimed"] += 1
            self._executor.submit(self._run_task, task.id, task.job_id)
        return len(tasks)

    def run_once(self) -> int:
        """同步执行一轮（测试与 --once 使用）。"""
        with session_scope() as session:
            register_worker(session, self.id, self.capacity, self.pool)
            tasks = claim_tasks(session, self.id, self.capacity, self.lease_ttl)
        for task in tasks:
            self._run_task(task.id, task.job_id, sync=True)
        return len(tasks)

    # ------------------------------------------------------------------
    def _run_task(self, task_id: str, job_id: str, sync: bool = False) -> None:
        snapshot: JobSnapshot | None = None
        task_ref: Task | None = None
        try:
            with session_scope() as session:
                task = session.get(Task, task_id)
                job = session.get(Job, job_id)
                if task is None or job is None:
                    log.warning("任务 %s 或任务定义 %s 已消失，跳过", task_id, job_id)
                    return
                snapshot = JobSnapshot.of(job)
                task_ref = task

            result = execute(snapshot, task_ref)  # type: ignore[arg-type]
            with session_scope() as session:
                outcome = complete_task(
                    session,
                    task_id,
                    self.id,
                    ok=result.ok,
                    exit_code=result.exit_code,
                    output=result.output,
                    error=result.error,
                    duration_ms=result.duration_ms,
                )
            with self._lock:
                if outcome is None:
                    self.stats["failed"] += 1
                elif outcome["status"] == "SUCCEEDED":
                    self.stats["succeeded"] += 1
                else:
                    self.stats["failed"] += 1
            log.info(
                "任务 %s 完成 ok=%s status=%s 耗时 %sms",
                task_id,
                result.ok,
                outcome and outcome.get("status"),
                result.duration_ms,
            )
        except Exception:  # pragma: no cover - 兜底，避免线程悄悄死掉
            log.exception("执行任务 %s 时发生未捕获异常", task_id)
        finally:
            if not sync:
                with self._lock:
                    self._inflight.discard(task_id)

    def _heartbeat_loop(self) -> None:
        interval = max(1.0, self.lease_ttl / 3.0)
        while not self._stop.is_set():
            try:
                with self._lock:
                    running = len(self._inflight)
                with session_scope() as session:
                    heartbeat(session, self.id, self.capacity)
                log.debug("worker %s 心跳，running=%s at %s", self.id, running, now().isoformat())
            except Exception:  # pragma: no cover
                log.exception("心跳失败")
            self._stop.wait(interval)


def main() -> None:
    parser = argparse.ArgumentParser(description="Tempo 任务执行器")
    parser.add_argument("--id", default=None, help="worker id（默认自动生成）")
    parser.add_argument("--capacity", type=int, default=4, help="并发执行槽位")
    parser.add_argument("--poll", type=float, default=1.0, help="空闲轮询间隔（秒）")
    parser.add_argument("--pool", default="default", help="执行器分组标签")
    parser.add_argument("--once", action="store_true", help="只处理一轮后退出")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    init_db()
    worker = Worker(
        worker_id=args.id,
        capacity=args.capacity,
        poll_interval=args.poll,
        pool=args.pool,
    )

    if args.once:
        handled = worker.run_once()
        log.info("单轮处理完成，任务数 %s", handled)
        return

    def _graceful(signum, _frame):
        log.info("收到信号 %s，准备退出", signum)
        worker.stop()

    signal.signal(signal.SIGINT, _graceful)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _graceful)
    worker.start()


if __name__ == "__main__":
    main()
