"""指标渲染与计数。"""
from __future__ import annotations

from app.metrics import Metrics


def test_counters_gauges_and_histogram_render():
    metrics = Metrics()
    metrics.describe("demo_total", "演示计数器")
    metrics.inc("demo_total", {"status": "ok"})
    metrics.inc("demo_total", {"status": "ok"}, 2)
    metrics.set_gauge("demo_queue", 7)
    metrics.observe("demo_seconds", 0.3)
    metrics.observe("demo_seconds", 2.0)

    text = metrics.render()
    assert "# TYPE demo_total counter" in text
    assert 'demo_total{status="ok"} 3' in text
    assert "# TYPE demo_queue gauge" in text
    assert "demo_queue 7" in text
    assert 'demo_seconds_bucket{le="0.5"} 1' in text
    assert 'demo_seconds_bucket{le="2.5"} 2' in text
    assert 'demo_seconds_bucket{le="+Inf"} 2' in text
    assert "demo_seconds_sum 2.3" in text
    assert "demo_seconds_count 2" in text


def test_label_escaping():
    metrics = Metrics()
    metrics.inc("weird_total", {"path": '/a"b'})
    assert 'weird_total{path="/a\\"b"} 1' in metrics.render()
