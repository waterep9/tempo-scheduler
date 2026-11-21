"""Cron 解析与下次触发时间计算（自研，不依赖 croniter）。

支持：
- 5 段（分 时 日 月 周）或 6 段（秒 分 时 日 月 周）
- `*`、`a-b`、`a,b,c`、`*/n`、`a-b/n`、`a/n`
- 月份/星期英文缩写（JAN、MON-FRI）
- 宏：@yearly @annually @monthly @weekly @daily @midnight @hourly @minutely
- 标准 cron 的「日与周同时受限时取或」语义
- 指定 IANA 时区求值（如 Asia/Shanghai）

实现是"逐级跳进"而非逐秒暴力扫描：月不匹配直接跳到下月 1 日，
日不匹配跳到次日 0 点，因此在极端表达式（如 2 月 29 日）上也能快速收敛。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

MAX_STEPS = 200_000

_RANGES = {
    "second": (0, 59),
    "minute": (0, 59),
    "hour": (0, 23),
    "dom": (1, 31),
    "month": (1, 12),
    "dow": (0, 6),
}

_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}
_DOWS = {"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6}

_MACROS = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
    "@minutely": "* * * * *",
}


class CronError(ValueError):
    """非法 cron 表达式。"""


def _to_int(token: str, kind: str) -> int:
    name = token.strip().lower()
    if kind == "month" and name in _MONTHS:
        return _MONTHS[name]
    if kind == "dow" and name in _DOWS:
        return _DOWS[name]
    try:
        return int(name)
    except ValueError as exc:
        raise CronError(f"无法识别的取值 {token!r}（字段 {kind}）") from exc


def _parse_field(raw: str, kind: str) -> tuple[frozenset[int], bool]:
    lo, hi = _RANGES[kind]
    values: set[int] = set()
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            raise CronError(f"字段 {kind} 存在空片段：{raw!r}")
        step = 1
        body = chunk
        if "/" in chunk:
            body, step_raw = chunk.split("/", 1)
            try:
                step = int(step_raw)
            except ValueError as exc:
                raise CronError(f"步长必须是整数：{chunk!r}") from exc
            if step <= 0:
                raise CronError(f"步长必须大于 0：{chunk!r}")

        body = body.strip()
        if body == "*":
            start, end = lo, hi
        elif "-" in body[1:]:
            left, right = body.split("-", 1)
            start = _to_int(left, kind)
            end = _to_int(right, kind)
        else:
            start = _to_int(body, kind)
            end = start if step == 1 else hi

        if kind == "dow":
            start = 0 if start == 7 else start
            end = 0 if end == 7 else end
            if start > end:  # 7-1 之类，归一化后不合法
                raise CronError(f"星期范围非法：{chunk!r}")
        if start < lo or end > hi:
            raise CronError(f"取值越界：{chunk!r}（允许 {lo}-{hi}）")
        if start > end:
            raise CronError(f"范围起点大于终点：{chunk!r}")
        values.update(range(start, end + 1, step))

    restricted = values != set(range(lo, hi + 1))
    return frozenset(values), restricted


def _next_at_or_after(sorted_values: tuple[int, ...], current: int) -> int | None:
    for value in sorted_values:
        if value >= current:
            return value
    return None


@dataclass(frozen=True)
class CronExpr:
    expr: str
    seconds: tuple[int, ...]
    minutes: tuple[int, ...]
    hours: tuple[int, ...]
    doms: tuple[int, ...]
    months: tuple[int, ...]
    dows: tuple[int, ...]
    dom_restricted: bool
    dow_restricted: bool

    # ---------- 构造 ----------
    @classmethod
    def parse(cls, expr: str) -> "CronExpr":
        if not isinstance(expr, str) or not expr.strip():
            raise CronError("cron 表达式不能为空")
        text = expr.strip()
        lowered = text.lower()
        if lowered.startswith("@"):
            if lowered not in _MACROS:
                raise CronError(f"不支持的宏：{expr!r}")
            text = _MACROS[lowered]

        parts = text.split()
        if len(parts) == 5:
            parts = ["0"] + parts
        if len(parts) != 6:
            raise CronError(f"cron 需要 5 或 6 段，实际 {len(parts)} 段：{expr!r}")

        sec, minute, hour, dom, month, dow = (
            _parse_field(parts[0], "second"),
            _parse_field(parts[1], "minute"),
            _parse_field(parts[2], "hour"),
            _parse_field(parts[3], "dom"),
            _parse_field(parts[4], "month"),
            _parse_field(parts[5], "dow"),
        )
        return cls(
            expr=text,
            seconds=tuple(sorted(sec[0])),
            minutes=tuple(sorted(minute[0])),
            hours=tuple(sorted(hour[0])),
            doms=tuple(sorted(dom[0])),
            months=tuple(sorted(month[0])),
            dows=tuple(sorted(dow[0])),
            dom_restricted=dom[1],
            dow_restricted=dow[1],
        )

    # ---------- 语义 ----------
    def _day_matches(self, moment: datetime) -> bool:
        dom_ok = moment.day in self.doms
        # cron 星期 0=周日，Python isoweekday 1=周一..7=周日
        dow = moment.isoweekday() % 7
        dow_ok = dow in self.dows
        if self.dom_restricted and self.dow_restricted:
            return dom_ok or dow_ok
        if self.dom_restricted:
            return dom_ok
        if self.dow_restricted:
            return dow_ok
        return True

    def next_after(self, moment: datetime, tz: ZoneInfo | timezone) -> datetime:
        """严格晚于 moment 的下一个触发时间（返回带时区的 datetime）。"""
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=timezone.utc)
        local = moment.astimezone(tz).replace(microsecond=0)
        candidate = local + timedelta(seconds=1)

        for _ in range(MAX_STEPS):
            if candidate.month not in self.months:
                year = candidate.year + (1 if candidate.month == 12 else 0)
                month = 1 if candidate.month == 12 else candidate.month + 1
                candidate = candidate.replace(
                    year=year, month=month, day=1, hour=0, minute=0, second=0
                )
                continue

            if not self._day_matches(candidate):
                candidate = (candidate + timedelta(days=1)).replace(
                    hour=0, minute=0, second=0
                )
                continue

            if candidate.hour not in self.hours:
                nxt = _next_at_or_after(self.hours, candidate.hour)
                if nxt is None:
                    candidate = (candidate + timedelta(days=1)).replace(
                        hour=0, minute=0, second=0
                    )
                else:
                    candidate = candidate.replace(hour=nxt, minute=0, second=0)
                continue

            if candidate.minute not in self.minutes:
                nxt = _next_at_or_after(self.minutes, candidate.minute)
                if nxt is None:
                    candidate = (candidate + timedelta(hours=1)).replace(
                        minute=0, second=0
                    )
                else:
                    candidate = candidate.replace(minute=nxt, second=0)
                continue

            if candidate.second not in self.seconds:
                nxt = _next_at_or_after(self.seconds, candidate.second)
                if nxt is None:
                    candidate = (candidate + timedelta(minutes=1)).replace(second=0)
                else:
                    candidate = candidate.replace(second=nxt)
                continue

            return candidate

        raise CronError(f"在可行时间范围内找不到下次触发点：{self.expr!r}")


def parse(expr: str) -> CronExpr:
    return CronExpr.parse(expr)


def validate(expr: str) -> str:
    """校验并返回表达式本身，供 Pydantic 校验器使用。"""
    CronExpr.parse(expr)
    return expr
