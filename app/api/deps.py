"""FastAPI 依赖：数据库会话、认证、授权、限流。"""
from __future__ import annotations

from typing import Iterator

from fastapi import Depends, Header, HTTPException, status
from sqlalchemy.orm import Session

from ..db import SessionLocal
from ..security import LIMITER, Principal, authenticate


def get_db() -> Iterator[Session]:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def _extract_token(authorization: str | None, x_api_key: str | None) -> str | None:
    if x_api_key:
        return x_api_key
    if authorization:
        return authorization
    return None


def current_principal(
    authorization: str | None = Header(default=None),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    db: Session = Depends(get_db),
) -> Principal:
    principal = authenticate(db, _extract_token(authorization, x_api_key))
    if principal is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="缺少或无效的 API Key",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return principal


def require_role(minimum: str):
    def dependency(principal: Principal = Depends(current_principal)) -> Principal:
        if not principal.allows(minimum):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"需要 {minimum} 及以上权限",
            )
        return principal

    return dependency


def guard(minimum: str, cost: float = 1.0):
    """认证 + 授权 + 写操作限流三合一。"""

    def dependency(principal: Principal = Depends(require_role(minimum))) -> Principal:
        if not LIMITER.allow(principal.id, cost):
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="写操作过于频繁，请稍后重试",
                headers={"Retry-After": "5"},
            )
        return principal

    return dependency


ReadAccess = require_role("viewer")
WriteAccess = guard("operator")
AdminAccess = guard("admin")
