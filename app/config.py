"""全局配置：全部来自环境变量，便于容器化与多实例部署。"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, "") or default)
    except ValueError:
        return default


def _float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, "") or default)
    except ValueError:
        return default


def _bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    app_name: str = "tempo-scheduler"
    database_url: str = field(
        default_factory=lambda: os.getenv("DATABASE_URL", "sqlite:///./tempo.db")
    )
    # 引导管理员 Key（首次启动写入 api_keys，之后可停用）
    bootstrap_token: str = field(
        default_factory=lambda: os.getenv("TEMPO_BOOTSTRAP_TOKEN", "tempo-admin-token")
    )
    # 调度器主循环间隔（秒）
    planner_interval: float = field(default_factory=lambda: _float("TEMPO_PLANNER_INTERVAL", 1.0))
    # 主节点租约 TTL：持有者崩溃后最多 TTL 秒被接管
    leader_ttl: float = field(default_factory=lambda: _float("TEMPO_LEADER_TTL", 10.0))
    # 任务租约 TTL：worker 崩溃后任务最多 TTL 秒被重新投递
    task_lease_ttl: float = field(default_factory=lambda: _float("TEMPO_TASK_LEASE_TTL", 30.0))
    # 单次扫描最多补齐的触发期数，防止停机后雪崩
    max_catch_up: int = field(default_factory=lambda: _int("TEMPO_MAX_CATCH_UP", 50))
    # 单个 API Key 每分钟写操作配额
    api_rate_limit: int = field(default_factory=lambda: _int("TEMPO_API_RATE_LIMIT", 120))
    # 执行输出截断长度
    output_limit: int = field(default_factory=lambda: _int("TEMPO_OUTPUT_LIMIT", 8192))
    # 任务默认超时
    default_timeout: int = field(default_factory=lambda: _int("TEMPO_DEFAULT_TIMEOUT", 60))
    # worker 无心跳多久判定离线（秒）
    worker_offline_after: float = field(
        default_factory=lambda: _float("TEMPO_WORKER_OFFLINE_AFTER", 30.0)
    )
    auth_disabled: bool = field(default_factory=lambda: _bool("TEMPO_AUTH_DISABLED", False))
    # API 进程内嵌调度循环（单机演示默认开启；多实例部署时置 0，独立跑 app.scheduler）
    embedded_planner: bool = field(
        default_factory=lambda: _bool("TEMPO_EMBEDDED_PLANNER", True)
    )


settings = Settings()
