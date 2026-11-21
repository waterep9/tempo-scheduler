"""轻量指标注册表：手写 Prometheus 文本暴露格式，避免额外依赖。

设计取舍：只需要 counter / gauge / histogram 三种原语，且希望
`/metrics` 与业务代码解耦（业务层只调用 inc/observe），因此不引入
prometheus_client。
"""
from __future__ import annotations

import copy
import threading
from collections import defaultdict

_DEFAULT_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0)


def _escape(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")


def _labels(labels: dict[str, str] | None) -> str:
    if not labels:
        return ""
    body = ",".join(f'{k}="{_escape(v)}"' for k, v in sorted(labels.items()))
    return "{" + body + "}"


class Metrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[tuple[str, str], float] = defaultdict(float)
        self._gauges: dict[tuple[str, str], float] = {}
        self._hist: dict[tuple[str, str], list] = {}
        self._help: dict[str, str] = {}

    def describe(self, name: str, help_text: str) -> None:
        self._help[name] = help_text

    def inc(self, name: str, labels: dict[str, str] | None = None, value: float = 1.0) -> None:
        key = (name, _labels(labels))
        with self._lock:
            self._counters[key] += value

    def set_gauge(self, name: str, value: float, labels: dict[str, str] | None = None) -> None:
        key = (name, _labels(labels))
        with self._lock:
            self._gauges[key] = float(value)

    def observe(
        self,
        name: str,
        value: float,
        labels: dict[str, str] | None = None,
        buckets: tuple[float, ...] = _DEFAULT_BUCKETS,
    ) -> None:
        key = (name, _labels(labels))
        with self._lock:
            entry = self._hist.get(key)
            if entry is None:
                entry = {
                    "buckets": buckets,
                    "counts": [0] * len(buckets),
                    "inf": 0,
                    "sum": 0.0,
                    "count": 0,
                }
                self._hist[key] = entry
            for idx, bound in enumerate(entry["buckets"]):
                if value <= bound:
                    entry["counts"][idx] += 1
            entry["inf"] += 1
            entry["sum"] += float(value)
            entry["count"] += 1

    def render(self) -> str:
        lines: list[str] = []
        with self._lock:
            counters = dict(self._counters)
            gauges = dict(self._gauges)
            hist = copy.deepcopy(self._hist)

        emitted: set[str] = set()
        for (name, labels), value in sorted(counters.items()):
            if name not in emitted:
                if name in self._help:
                    lines.append(f"# HELP {name} {self._help[name]}")
                lines.append(f"# TYPE {name} counter")
                emitted.add(name)
            lines.append(f"{name}{labels} {value:g}")

        for (name, labels), value in sorted(gauges.items()):
            if name not in emitted:
                if name in self._help:
                    lines.append(f"# HELP {name} {self._help[name]}")
                lines.append(f"# TYPE {name} gauge")
                emitted.add(name)
            lines.append(f"{name}{labels} {value:g}")

        for (name, labels), entry in sorted(hist.items()):
            if name not in emitted:
                if name in self._help:
                    lines.append(f"# HELP {name} {self._help[name]}")
                lines.append(f"# TYPE {name} histogram")
                emitted.add(name)
            label_pairs = labels[1:-1] if labels else ""
            for idx, bound in enumerate(entry["buckets"]):
                le = "Inf" if bound == float("inf") else f"{bound:g}"
                inner = f'le="{le}"'
                merged = f"{label_pairs},{inner}" if label_pairs else inner
                lines.append(f"{name}_bucket{{{merged}}} {entry['counts'][idx]}")
            merged = f'{label_pairs},le="+Inf"' if label_pairs else 'le="+Inf"'
            lines.append(f"{name}_bucket{{{merged}}} {entry['inf']}")
            lines.append(f"{name}_sum{labels} {entry['sum']:g}")
            lines.append(f"{name}_count{labels} {entry['count']}")
        return "\n".join(lines) + "\n"


METRICS = Metrics()
METRICS.describe("tempo_task_total", "任务状态流转次数")
METRICS.describe("tempo_http_requests_total", "API 请求数")
METRICS.describe("tempo_http_request_duration_seconds", "API 请求耗时")
METRICS.describe("tempo_planner_tick_total", "调度器主循环执行次数")
METRICS.describe("tempo_planner_tasks_created_total", "物化出的任务实例数")
METRICS.describe("tempo_queue_depth", "待执行任务数（含重试等待）")
METRICS.describe("tempo_running_tasks", "执行中任务数")
METRICS.describe("tempo_dead_tasks", "进入死信的任务数")
METRICS.describe("tempo_online_workers", "在线执行器数")
