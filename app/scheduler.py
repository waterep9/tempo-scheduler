"""独立调度器进程：`python -m app.scheduler`。

多实例部署时启动 2 个即可观察选主：只有一个真正派发任务，
另一个处于 standby；持主节点崩溃后 TTL 到期自动接管。
"""
from __future__ import annotations

import logging
import signal
import threading

from .config import settings
from .db import init_db, session_scope
from .security import ensure_bootstrap_key
from .services.planner import Planner

log = logging.getLogger("tempo.scheduler")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    init_db()
    with session_scope() as session:
        ensure_bootstrap_key(session)

    stop = threading.Event()
    planner = Planner()

    def _graceful(signum, _frame):
        log.info("收到信号 %s，释放主节点租约", signum)
        stop.set()

    signal.signal(signal.SIGINT, _graceful)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _graceful)

    log.info("调度器 %s 启动（TTL=%.1fs, 间隔=%.1fs）", planner.elector.holder, planner.elector.ttl, settings.planner_interval)
    while not stop.is_set():
        try:
            result = planner.tick()
            if result.get("leader"):
                log.info(
                    "tick: 扫描 %s 个到期任务, 物化 %s 个实例, 回收 %s, 死信 %s",
                    result["jobs_scanned"],
                    result["tasks_created"],
                    result["requeued"],
                    result["dead"],
                )
        except Exception:
            log.exception("调度循环异常")
        stop.wait(settings.planner_interval)

    with session_scope() as session:
        planner.elector.release(session)
    log.info("调度器已退出")


if __name__ == "__main__":
    main()
