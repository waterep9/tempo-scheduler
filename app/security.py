"""认证、授权与写接口限流。

角色：viewer（只读） < operator（触发/重放/运维动作） < admin（任务与密钥管理）。
密钥只存 SHA-256 摘要，明文仅在创建时返回一次；前缀用于人工识别。
"""
from __future__ import annotations

import hashlib
import secrets
import threading
import time
from dataclasses import dataclass
from sqlalchemy import select
from sqlalchemy.orm import Session

from .clock import now
from .config import settings
from .models import ApiKey

ROLE_LEVEL = {"viewer": 1, "operator": 2, "admin": 3}
TOKEN_PREFIX = "tk_"


def hash_token(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def new_token() -> tuple[str, str, str]:
    """返回 (明文, 摘要, 展示前缀)。"""
    raw = f"{TOKEN_PREFIX}{secrets.token_urlsafe(32)}"
    return raw, hash_token(raw), raw[:10]


def ensure_bootstrap_key(session: Session) -> None:
    if not settings.bootstrap_token:
        return
    digest = hash_token(settings.bootstrap_token)
    exists = session.scalar(select(ApiKey).where(ApiKey.key_hash == digest))
    if exists:
        return
    session.add(
        ApiKey(
            name="bootstrap-admin",
            prefix=settings.bootstrap_token[:10],
            key_hash=digest,
            role="admin",
            created_at=now().replace(tzinfo=None),
        )
    )
    session.commit()


@dataclass
class Principal:
    id: str
    name: str
    role: str

    @property
    def is_admin(self) -> bool:
        return self.role == "admin"

    def allows(self, minimum: str) -> bool:
        return ROLE_LEVEL.get(self.role, 0) >= ROLE_LEVEL.get(minimum, 99)


def authenticate(session: Session, raw: str | None) -> Principal | None:
    if settings.auth_disabled:
        return Principal(id="dev", name="dev-mode", role="admin")
    if not raw:
        return None
    raw = raw.strip()
    if raw.lower().startswith("bearer "):
        raw = raw[7:].strip()
    record = session.scalar(select(ApiKey).where(ApiKey.key_hash == hash_token(raw)))
    if record is None or record.revoked_at is not None:
        return None
    # last_used_at 只在间隔超过 60s 时更新，避免每个请求都写库
    stamp = now().replace(tzinfo=None)
    if record.last_used_at is None or (stamp - record.last_used_at).total_seconds() > 60:
        record.last_used_at = stamp
        session.commit()
    return Principal(id=record.id, name=record.name, role=record.role)


class TokenBucketLimiter:
    """按 Key 的令牌桶：容量 = 每分钟配额，匀速补充。"""

    def __init__(self, capacity: int, window_seconds: float = 60.0) -> None:
        self.capacity = max(1, capacity)
        self.window = window_seconds
        self._state: dict[str, tuple[float, float]] = {}
        self._lock = threading.Lock()

    def allow(self, key: str, cost: float = 1.0) -> bool:
        rate = self.capacity / self.window
        moment = time.monotonic()
        with self._lock:
            tokens, last = self._state.get(key, (float(self.capacity), moment))
            tokens = min(float(self.capacity), tokens + (moment - last) * rate)
            if tokens < cost:
                self._state[key] = (tokens, moment)
                return False
            self._state[key] = (tokens - cost, moment)
            return True

    def reset(self) -> None:
        with self._lock:
            self._state.clear()


LIMITER = TokenBucketLimiter(settings.api_rate_limit)
