"""Cron 解析与触发点计算。"""
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from app.cron import CronError, CronExpr, validate

SH = ZoneInfo("Asia/Shanghai")


def at(y, mo, d, h=0, mi=0, s=0):
    return datetime(y, mo, d, h, mi, s, tzinfo=SH)


@pytest.mark.parametrize(
    "expr,start,expected",
    [
        ("*/5 * * * *", at(2026, 11, 21, 9, 7, 33), at(2026, 11, 21, 9, 10)),
        ("0 0 * * *", at(2026, 11, 21, 9, 7), at(2026, 11, 22, 0, 0)),
        ("30 2 * * *", at(2026, 11, 21, 3, 0), at(2026, 11, 22, 2, 30)),
        ("0 9 * * MON-FRI", at(2026, 11, 21, 9, 7), at(2026, 11, 23, 9, 0)),
        ("0 0 1 * *", at(2026, 11, 21), at(2026, 12, 1)),
        ("0 0 1 1 *", at(2026, 11, 21), at(2027, 1, 1)),
        ("0 0 29 2 *", at(2026, 3, 1), at(2028, 2, 29)),
        ("@hourly", at(2026, 11, 21, 9, 7), at(2026, 11, 21, 10, 0)),
        ("@daily", at(2026, 11, 21, 23, 59), at(2026, 11, 22, 0, 0)),
        ("0 0 1,15 * *", at(2026, 11, 2), at(2026, 11, 15)),
        ("10-20/5 * * * *", at(2026, 11, 21, 9, 21), at(2026, 11, 21, 10, 10)),
        ("*/10 * * * * *", at(2026, 11, 21, 9, 7, 33), at(2026, 11, 21, 9, 7, 40)),
        ("0 0 1 * 1", at(2026, 11, 21), at(2026, 11, 23)),
        ("0 12 * * SUN", at(2026, 11, 21), at(2026, 11, 22, 12, 0)),
    ],
)
def test_next_fire(expr, start, expected):
    assert CronExpr.parse(expr).next_after(start, SH) == expected


def test_next_is_strictly_after():
    expr = CronExpr.parse("0 * * * *")
    moment = at(2026, 11, 21, 9, 0)
    assert expr.next_after(moment, SH) == at(2026, 11, 21, 10, 0)


def test_dom_and_dow_are_ored():
    # 每月 1 号 或 每周一，两者同时受限时按标准 cron 取"或"
    expr = CronExpr.parse("0 0 1 * 1")
    assert expr.dom_restricted and expr.dow_restricted
    assert expr.next_after(at(2026, 11, 21), SH) == at(2026, 11, 23)
    assert expr.next_after(at(2026, 11, 24), SH) == at(2026, 11, 30)


def test_dow_sunday_aliases():
    seven = CronExpr.parse("0 0 * * 7")
    zero = CronExpr.parse("0 0 * * 0")
    moment = at(2026, 11, 21)
    assert seven.next_after(moment, SH) == zero.next_after(moment, SH)


@pytest.mark.parametrize(
    "expr",
    [
        "",
        "* * * *",
        "* * * * * * *",
        "60 * * * *",
        "*/0 * * * *",
        "* 24 * * *",
        "* * 32 * *",
        "* * * 13 *",
        "* * * * 8",
        "a * * * *",
        "5-1 * * * *",
        "@reboot",
        "*/x * * * *",
    ],
)
def test_invalid_expressions(expr):
    with pytest.raises(CronError):
        CronExpr.parse(expr)


def test_validate_returns_expression():
    assert validate("*/5 * * * *") == "*/5 * * * *"


def test_impossible_date_raises():
    # 2 月 31 日永远不会到来
    with pytest.raises(CronError):
        CronExpr.parse("0 0 31 2 *").next_after(at(2026, 1, 1), SH)


def test_utc_timezone_evaluation():
    expr = CronExpr.parse("0 9 * * *")
    moment = datetime(2026, 11, 21, 1, 0, tzinfo=ZoneInfo("UTC"))
    assert expr.next_after(moment, ZoneInfo("UTC")) == datetime(
        2026, 11, 21, 9, 0, tzinfo=ZoneInfo("UTC")
    )
