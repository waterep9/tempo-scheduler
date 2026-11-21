"""FastAPI 应用入口：API + 内嵌调度循环 + 静态控制台。"""
from __future__ import annotations

import logging
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from .api import jobs, ops, runs, system
from .config import settings
from .db import init_db, session_scope
from .metrics import METRICS
from .security import ensure_bootstrap_key
from .services.planner import Planner

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("tempo.api")

VERSION = "1.0.0"
_stop = threading.Event()
_planner_thread: threading.Thread | None = None


def _planner_loop() -> None:
    planner = Planner()
    while not _stop.is_set():
        try:
            planner.tick()
        except Exception:  # 调度异常不能让主循环退出
            log.exception("调度循环异常")
        _stop.wait(settings.planner_interval)


@asynccontextmanager
async def lifespan(_: FastAPI):
    global _planner_thread
    init_db()
    with session_scope() as session:
        ensure_bootstrap_key(session)
    if settings.embedded_planner:
        _stop.clear()
        _planner_thread = threading.Thread(target=_planner_loop, name="tempo-planner", daemon=True)
        _planner_thread.start()
        log.info("内嵌调度循环已启动，间隔 %.1fs", settings.planner_interval)
    yield
    _stop.set()
    if _planner_thread is not None:
        _planner_thread.join(timeout=settings.planner_interval + 5)


app = FastAPI(
    title="Tempo 分布式任务调度平台",
    description=(
        "支持 cron/interval/once 触发、分片执行、租约续约、指数退避重试、"
        "死信重放与事件流的多实例任务调度平台。"
    ),
    version=VERSION,
    lifespan=lifespan,
)


@app.middleware("http")
async def observe_requests(request: Request, call_next):
    started = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        METRICS.inc(
            "tempo_http_requests_total",
            {"path": request.url.path, "method": request.method, "status": "500"},
        )
        raise
    elapsed = time.perf_counter() - started
    route = request.scope.get("route")
    path = getattr(route, "path", request.url.path)
    METRICS.inc(
        "tempo_http_requests_total",
        {"path": path, "method": request.method, "status": str(response.status_code)},
    )
    METRICS.observe("tempo_http_request_duration_seconds", elapsed, {"path": path})
    response.headers["X-Tempo-Version"] = VERSION
    return response


@app.exception_handler(Exception)
async def unhandled_exception(_: Request, exc: Exception):  # pragma: no cover
    log.exception("未处理异常", exc_info=exc)
    return JSONResponse(status_code=500, content={"detail": "服务内部错误"})


app.include_router(system.router)
app.include_router(jobs.router)
app.include_router(runs.router)
app.include_router(ops.router)

_static_dir = Path(__file__).parent / "static"
if _static_dir.exists():
    # 必须放在所有 API 路由之后：mount("/") 会吞掉后续注册的路径
    app.mount("/", StaticFiles(directory=_static_dir, html=True), name="console")
