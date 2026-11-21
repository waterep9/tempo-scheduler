"""时间约定：数据库中一律存 **naive UTC**，领域层一律用 aware UTC。

SQLite 不保存时区信息，混用 aware/naive 是最常见的隐蔽 bug 来源，
因此在边界处显式转换，内部只保留一种表示。
"""
from __future__ import annotations

from datetime import datetime, timezone

UTC = timezone.utc


def now() -> datetime:
    """当前时间（aware UTC）。"""
    return datetime.now(UTC)


def naive(dt: datetime) -> datetime:
    """转为数据库使用的 naive UTC。"""
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(UTC).replace(tzinfo=None)


def aware(dt: datetime) -> datetime:
    """数据库 naive UTC -> 领域层 aware UTC。"""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return aware(dt).isoformat().replace("+00:00", "Z")
