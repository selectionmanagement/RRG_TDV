from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Optional


@dataclass(frozen=True)
class SnapshotRow:
    symbol: str
    vol_today: Optional[float]
    close: Optional[float]
    chg_pct: Optional[float]
    avg5: Optional[float]
    avg10: Optional[float]
    avg20: Optional[float]
    avg50: Optional[float]
    ratio5: Optional[float]
    break5: bool


def _fmt_num(x: Optional[float]) -> str:
    if x is None:
        return "-"
    try:
        if abs(x) >= 1_000_000:
            return f"{x/1_000_000:.1f}M"
        if abs(x) >= 1_000:
            return f"{x/1_000:.1f}K"
        return f"{x:.0f}"
    except Exception:
        return "-"


def _fmt_price(x: Optional[float]) -> str:
    if x is None:
        return "-"
    try:
        return f"{x:.2f}"
    except Exception:
        return "-"


def _fmt_pct(x: Optional[float]) -> str:
    if x is None:
        return "-"
    try:
        return f"{x:+.2f}%"
    except Exception:
        return "-"


def _fmt_ratio(x: Optional[float]) -> str:
    if x is None:
        return "-"
    try:
        return f"{x:.2f}"
    except Exception:
        return "-"


def build_hourly_report(
    *,
    dt: datetime,
    universe_size: int,
    new_in_hour: int,
    rows: Iterable[SnapshotRow],
    top_n: int = 20,
) -> str:
    rows = list(rows)
    breaks = [r for r in rows if r.break5]
    breaks_sorted = sorted(breaks, key=lambda r: (r.ratio5 or 0.0), reverse=True)[: max(0, int(top_n))]

    header = "[Volume Break AVG5] Hourly Report"
    lines: List[str] = [
        header,
        f"เวลา: {dt.strftime('%Y-%m-%d %H:%M')} (Asia/Bangkok)",
        "Signal: vol_today > avg(vol_prev_5d)",
        f"Universe: {universe_size} symbols | Break: {len(breaks)} | New in last hour: {new_in_hour}",
        "",
    ]

    if not breaks_sorted:
        lines.append("ผลลัพธ์: พบ 0 ตัว")
        return "\n".join(lines).strip() + "\n"

    lines.append("Top (เรียงตาม ratio5)")
    for i, r in enumerate(breaks_sorted, start=1):
        lines.append(
            f"{i}) {r.symbol}  vol={_fmt_num(r.vol_today)}  avg5={_fmt_num(r.avg5)}  ratio5={_fmt_ratio(r.ratio5)}  "
            f"close={_fmt_price(r.close)}  chg1D={_fmt_pct(r.chg_pct)}"
        )
    return "\n".join(lines).strip() + "\n"


def build_daily_close_report(
    *,
    dt: datetime,
    universe_size: int,
    rows: Iterable[SnapshotRow],
    top_n: int = 30,
) -> str:
    rows = list(rows)
    breaks = [r for r in rows if r.break5]
    breaks_sorted = sorted(breaks, key=lambda r: (r.ratio5 or 0.0), reverse=True)[: max(0, int(top_n))]

    header = "[Volume Break] Daily Close Report"
    lines: List[str] = [
        header,
        f"วัน: {dt.strftime('%Y-%m-%d (%a)')} เวลา: {dt.strftime('%H:%M')} (Asia/Bangkok)",
        "Signal: vol_today > avg5(prev) | avg10/20/50 exclude today",
        f"Universe: {universe_size} symbols | Break AVG5: {len(breaks)}",
        "",
    ]

    if not breaks_sorted:
        lines.append("ผลลัพธ์: พบ 0 ตัว")
        return "\n".join(lines).strip() + "\n"

    lines.append("Top by ratio5")
    lines.append("symbol   vol_today  avg5   avg10  avg20  avg50  ratio5  close  chg1D")
    for r in breaks_sorted:
        lines.append(
            f"{r.symbol}  {_fmt_num(r.vol_today):>8}  {_fmt_num(r.avg5):>6}  {_fmt_num(r.avg10):>6}  {_fmt_num(r.avg20):>6}  "
            f"{_fmt_num(r.avg50):>6}  {_fmt_ratio(r.ratio5):>5}  {_fmt_price(r.close):>5}  {_fmt_pct(r.chg_pct):>7}"
        )
    return "\n".join(lines).strip() + "\n"

