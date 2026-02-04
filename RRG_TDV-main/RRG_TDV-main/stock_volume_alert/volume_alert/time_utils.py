from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Iterable, Optional
from zoneinfo import ZoneInfo

from volume_alert.config import MarketSession


def now_in_tz(tz_name: str) -> datetime:
    tz = ZoneInfo(tz_name)
    return datetime.now(tz=tz)


def is_weekday(dt: datetime) -> bool:
    return int(dt.weekday()) < 5


def within_sessions(dt: datetime, sessions: Iterable[MarketSession]) -> bool:
    t = dt.timetz().replace(tzinfo=None)
    for s in sessions:
        if s.start <= t <= s.end:
            return True
    return False


def minute_floor(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


def seconds_until_next_minute(dt: datetime) -> float:
    nxt = (minute_floor(dt) + timedelta(minutes=1)).replace(second=0, microsecond=0)
    return max(0.0, (nxt - dt).total_seconds())


def hour_key(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H")


def day_key(dt: datetime) -> str:
    return dt.date().isoformat()


def should_send_daily_close(*, dt: datetime, daily_time: time, last_sent_day: Optional[str]) -> bool:
    if not is_weekday(dt):
        return False
    if dt.timetz().replace(tzinfo=None) < daily_time:
        return False
    today = day_key(dt)
    return today != (last_sent_day or "")


def should_send_hourly(
    *,
    dt: datetime,
    last_sent_hour: Optional[str],
    require_market_open: bool = True,
    market_open: bool = False,
) -> bool:
    if require_market_open and not market_open:
        return False
    key = hour_key(dt)
    return key != (last_sent_hour or "")


@dataclass(frozen=True)
class MarketClock:
    tz_name: str
    sessions: list[MarketSession]
    daily_close_time: time

    def now(self) -> datetime:
        return now_in_tz(self.tz_name)

    def is_market_open(self, dt: datetime) -> bool:
        return is_weekday(dt) and within_sessions(dt, self.sessions)

    def should_daily(self, dt: datetime, last_sent_day: Optional[str]) -> bool:
        return should_send_daily_close(dt=dt, daily_time=self.daily_close_time, last_sent_day=last_sent_day)

