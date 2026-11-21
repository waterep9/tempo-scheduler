"""演示数据：创建几个覆盖不同调度语义的任务，便于打开控制台立即体验。

    python seed.py
"""
from __future__ import annotations

from app.clock import naive, now
from app.db import init_db, session_scope
from app.models import Job, Task
from app.security import ensure_bootstrap_key
from app.services.jobs import create_job


DEMO_JOBS = [
    {
        "name": "demo-heartbeat",
        "description": "每 2 分钟打印一次心跳，演示 interval 触发与重试",
        "schedule_type": "interval",
        "interval_seconds": 120,
        "executor": "shell",
        "command": "echo heartbeat-from-tempo",
        "max_attempts": 3,
        "max_concurrency": 1,
    },
    {
        "name": "demo-sharded-rollup",
        "description": "分片汇总：4 个分片并行处理不同分区，演示分片与并发上限",
        "schedule_type": "cron",
        "cron_expr": "*/10 * * * *",
        "timezone": "Asia/Shanghai",
        "executor": "shell",
        "command": "python -c \"import os;print('rollup shard', os.environ['TEMPO_SHARD_INDEX'], '/', os.environ['TEMPO_SHARD_TOTAL'])\"",
        "shard_count": 4,
        "max_concurrency": 2,
        "max_attempts": 3,
        "backoff_base_seconds": 10,
        "catch_up": "skip",
    },
    {
        "name": "demo-failure-drill",
        "description": "故意失败，演示指数退避重试与死信（可手工重放）",
        "schedule_type": "cron",
        "cron_expr": "*/15 * * * *",
        "executor": "shell",
        "command": "echo boom && exit 9",
        "max_attempts": 2,
        "backoff_base_seconds": 5,
        "backoff_cap_seconds": 30,
    },
]


def main() -> None:
    init_db()
    with session_scope() as session:
        ensure_bootstrap_key(session)
        existing = {row.name for row in session.query(Job).all()}
        for spec in DEMO_JOBS:
            if spec["name"] in existing:
                print(f"跳过已存在的任务：{spec['name']}")
                continue
            job = create_job(session, spec, "seed")
            print(f"已创建 {job.name} -> 下次触发 {job.next_fire_at} (UTC)")

        # 让 demo-heartbeat 立刻可被调度一次，打开控制台就能看到运行记录
        first = session.query(Job).filter(Job.name == "demo-heartbeat").one()
        first.next_fire_at = naive(now())
        session.commit()
        print("已把 demo-heartbeat 的下次触发提前到当前时刻，启动 worker 后会立即执行。")
        print(f"当前任务数：{session.query(Job).count()}，任务实例数：{session.query(Task).count()}")


if __name__ == "__main__":
    main()
