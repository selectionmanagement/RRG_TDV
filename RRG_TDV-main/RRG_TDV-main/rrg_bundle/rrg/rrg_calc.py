from __future__ import annotations

import math
from typing import Literal

import numpy as np
import pandas as pd


Quadrant = Literal["Leading", "Weakening", "Lagging", "Improving"]


def _quadrant(rs_ratio: float, rs_mom: float) -> Quadrant:
    if rs_ratio >= 100 and rs_mom >= 100:
        return "Leading"
    if rs_ratio >= 100 and rs_mom < 100:
        return "Weakening"
    if rs_ratio < 100 and rs_mom < 100:
        return "Lagging"
    return "Improving"


def compute_rrg_for_symbol(
    *,
    close_symbol: pd.Series,
    close_benchmark: pd.Series,
    ratio_len: int,
    mom_len: int,
) -> pd.DataFrame:
    close_symbol = close_symbol.astype(float)
    close_benchmark = close_benchmark.astype(float)

    rs = 100.0 * (close_symbol / close_benchmark)
    rs_ma = rs.ewm(span=ratio_len, adjust=False).mean()
    rs_ratio = 100.0 * (rs / rs_ma)

    ratio_ma = rs_ratio.ewm(span=mom_len, adjust=False).mean()
    rs_mom = 100.0 * (rs_ratio / ratio_ma)

    df = pd.DataFrame(
        {
            "rs": rs,
            "rs_ratio": rs_ratio,
            "rs_mom": rs_mom,
        }
    ).dropna()

    df["d_rs_ratio"] = df["rs_ratio"].diff()
    df["d_rs_mom"] = df["rs_mom"].diff()
    df["speed"] = np.sqrt(df["d_rs_ratio"].to_numpy() ** 2 + df["d_rs_mom"].to_numpy() ** 2)

    quad = []
    dist = []
    ang = []
    for x, y in zip(df["rs_ratio"].to_numpy(), df["rs_mom"].to_numpy(), strict=True):
        quad.append(_quadrant(float(x), float(y)))
        dx = float(x) - 100.0
        dy = float(y) - 100.0
        dist.append(math.sqrt(dx * dx + dy * dy))
        ang.append(math.degrees(math.atan2(dy, dx)))

    df["quadrant"] = quad
    df["distance"] = np.array(dist, dtype=float)
    df["angle_deg"] = np.array(ang, dtype=float)
    return df


def _month_start(ts: pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(ts)
    if ts.tz is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _week_start(ts: pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(ts)
    if ts.tz is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    ts = ts.replace(hour=0, minute=0, second=0, microsecond=0)
    return ts - pd.Timedelta(days=int(ts.weekday()))


def _three_month_high_by_month_start(high: pd.Series) -> dict[pd.Timestamp, float]:
    high = high.astype(float).dropna()
    if high.empty:
        return {}
    idx = pd.to_datetime(high.index)
    idx = idx.tz_localize(None) if getattr(idx, "tz", None) is not None else idx
    high = pd.Series(high.to_numpy(), index=idx).sort_index()

    month_starts = pd.to_datetime(high.index.to_period("M").to_timestamp()).unique()
    out: dict[pd.Timestamp, float] = {}
    for ms in month_starts:
        ms = _month_start(pd.Timestamp(ms))
        window_start = ms - pd.DateOffset(months=3)
        window_end = ms
        w = high[(high.index >= window_start) & (high.index < window_end)]
        if w.empty:
            continue
        out[ms] = float(w.max())
    return out


def _fifty_two_week_high_by_week_start(high: pd.Series) -> dict[pd.Timestamp, float]:
    high = high.astype(float).dropna()
    if high.empty:
        return {}
    idx = pd.to_datetime(high.index)
    idx = idx.tz_localize(None) if getattr(idx, "tz", None) is not None else idx
    high = pd.Series(high.to_numpy(), index=idx).sort_index()

    week_starts = pd.to_datetime(high.index.map(_week_start)).unique()
    out: dict[pd.Timestamp, float] = {}
    for ws in week_starts:
        ws = _week_start(pd.Timestamp(ws))
        window_start = ws - pd.Timedelta(weeks=52)
        window_end = ws
        w = high[(high.index >= window_start) & (high.index < window_end)]
        if w.empty:
            continue
        out[ws] = float(w.max())
    return out


def _fifty_two_week_low_by_week_start(low: pd.Series) -> dict[pd.Timestamp, float]:
    low = low.astype(float).dropna()
    if low.empty:
        return {}
    idx = pd.to_datetime(low.index)
    idx = idx.tz_localize(None) if getattr(idx, "tz", None) is not None else idx
    low = pd.Series(low.to_numpy(), index=idx).sort_index()

    week_starts = pd.to_datetime(low.index.map(_week_start)).unique()
    out: dict[pd.Timestamp, float] = {}
    for ws in week_starts:
        ws = _week_start(pd.Timestamp(ws))
        window_start = ws - pd.Timedelta(weeks=52)
        window_end = ws
        w = low[(low.index >= window_start) & (low.index < window_end)]
        if w.empty:
            continue
        out[ws] = float(w.min())
    return out


def compute_rrg_for_symbol_three_month_high(
    *,
    close_symbol: pd.Series,
    high_symbol: pd.Series,
    close_benchmark: pd.Series,
    high_benchmark: pd.Series,
    mom_lookback: int,
) -> pd.DataFrame:
    """
    3 Month - High RRG (no EMA smoothing):
    - For each date, define the reference high as the maximum OHLCV `high` from the previous 3 full calendar months.
    - Convert price into a 'strength vs own 3M high': close / 3M_high.
    - Build relative strength vs benchmark: (sym_strength / bench_strength) normalized around 100.
    - Momentum is rate-of-change over `mom_lookback` bars: rs_ratio / rs_ratio.shift(mom_lookback) normalized around 100.
    """
    mom_lookback = max(1, int(mom_lookback))

    close_symbol = close_symbol.astype(float)
    close_benchmark = close_benchmark.astype(float)
    high_symbol = high_symbol.astype(float)
    high_benchmark = high_benchmark.astype(float)

    sym_3m = _three_month_high_by_month_start(high_symbol)
    bench_3m = _three_month_high_by_month_start(high_benchmark)
    if not sym_3m or not bench_3m:
        return pd.DataFrame()

    # Align dates
    merged = pd.concat({"sym": close_symbol, "bench": close_benchmark}, axis=1, join="inner").dropna()
    if merged.empty:
        return pd.DataFrame()

    month_start_index = pd.to_datetime(merged.index.to_period("M").to_timestamp()).map(_month_start)
    sym_ref = pd.Series([sym_3m.get(ms, float("nan")) for ms in month_start_index], index=merged.index, dtype=float)
    bench_ref = pd.Series([bench_3m.get(ms, float("nan")) for ms in month_start_index], index=merged.index, dtype=float)

    strength_sym = merged["sym"] / sym_ref
    strength_bench = merged["bench"] / bench_ref
    rs_ratio = 100.0 * (strength_sym / strength_bench)
    rs_mom = 100.0 * (rs_ratio / rs_ratio.shift(mom_lookback))

    df = pd.DataFrame({"rs_ratio": rs_ratio, "rs_mom": rs_mom}).replace([np.inf, -np.inf], np.nan).dropna()
    if df.empty:
        return df

    df["d_rs_ratio"] = df["rs_ratio"].diff()
    df["d_rs_mom"] = df["rs_mom"].diff()
    df["speed"] = np.sqrt(df["d_rs_ratio"].to_numpy() ** 2 + df["d_rs_mom"].to_numpy() ** 2)

    quad = []
    dist = []
    ang = []
    for x, y in zip(df["rs_ratio"].to_numpy(), df["rs_mom"].to_numpy(), strict=True):
        quad.append(_quadrant(float(x), float(y)))
        dx = float(x) - 100.0
        dy = float(y) - 100.0
        dist.append(math.sqrt(dx * dx + dy * dy))
        ang.append(math.degrees(math.atan2(dy, dx)))

    df["quadrant"] = quad
    df["distance"] = np.array(dist, dtype=float)
    df["angle_deg"] = np.array(ang, dtype=float)
    return df


def compute_rrg_for_symbol_fifty_two_week_high(
    *,
    close_symbol: pd.Series,
    high_symbol: pd.Series,
    close_benchmark: pd.Series,
    high_benchmark: pd.Series,
    mom_lookback: int,
) -> pd.DataFrame:
    """
    52 Week - High RRG (no EMA smoothing):
    - For each date, define the reference high as the maximum OHLCV `high` from the previous 52 full weeks
      (excluding the current week of the date).
    - Convert price into a 'strength vs own 52W high': close / 52W_high.
    - Build relative strength vs benchmark: (sym_strength / bench_strength) normalized around 100.
    - Momentum is rate-of-change over `mom_lookback` bars: rs_ratio / rs_ratio.shift(mom_lookback) normalized around 100.
    """
    mom_lookback = max(1, int(mom_lookback))

    close_symbol = close_symbol.astype(float)
    close_benchmark = close_benchmark.astype(float)
    high_symbol = high_symbol.astype(float)
    high_benchmark = high_benchmark.astype(float)

    sym_52w = _fifty_two_week_high_by_week_start(high_symbol)
    bench_52w = _fifty_two_week_high_by_week_start(high_benchmark)
    if not sym_52w or not bench_52w:
        return pd.DataFrame()

    merged = pd.concat({"sym": close_symbol, "bench": close_benchmark}, axis=1, join="inner").dropna()
    if merged.empty:
        return pd.DataFrame()

    week_start_index = pd.to_datetime(merged.index.map(_week_start))
    if getattr(week_start_index, "tz", None) is not None:
        week_start_index = week_start_index.tz_localize(None)
    sym_ref = pd.Series([sym_52w.get(ws, float("nan")) for ws in week_start_index], index=merged.index, dtype=float)
    bench_ref = pd.Series([bench_52w.get(ws, float("nan")) for ws in week_start_index], index=merged.index, dtype=float)

    strength_sym = merged["sym"] / sym_ref
    strength_bench = merged["bench"] / bench_ref
    rs_ratio = 100.0 * (strength_sym / strength_bench)
    rs_mom = 100.0 * (rs_ratio / rs_ratio.shift(mom_lookback))

    df = pd.DataFrame({"rs_ratio": rs_ratio, "rs_mom": rs_mom}).replace([np.inf, -np.inf], np.nan).dropna()
    if df.empty:
        return df

    df["d_rs_ratio"] = df["rs_ratio"].diff()
    df["d_rs_mom"] = df["rs_mom"].diff()
    df["speed"] = np.sqrt(df["d_rs_ratio"].to_numpy() ** 2 + df["d_rs_mom"].to_numpy() ** 2)

    quad = []
    dist = []
    ang = []
    for x, y in zip(df["rs_ratio"].to_numpy(), df["rs_mom"].to_numpy(), strict=True):
        quad.append(_quadrant(float(x), float(y)))
        dx = float(x) - 100.0
        dy = float(y) - 100.0
        dist.append(math.sqrt(dx * dx + dy * dy))
        ang.append(math.degrees(math.atan2(dy, dx)))

    df["quadrant"] = quad
    df["distance"] = np.array(dist, dtype=float)
    df["angle_deg"] = np.array(ang, dtype=float)
    return df


def compute_rrg_for_symbol_fifty_two_week_low(
    *,
    close_symbol: pd.Series,
    low_symbol: pd.Series,
    close_benchmark: pd.Series,
    low_benchmark: pd.Series,
    mom_lookback: int,
) -> pd.DataFrame:
    """
    52 Week - Low RRG (no EMA smoothing):
    - For each date, define the reference low as the minimum OHLCV `low` from the previous 52 full weeks
      (excluding the current week of the date).
    - Convert price into a 'strength vs own 52W low': close / 52W_low.
    - Build relative strength vs benchmark: (sym_strength / bench_strength) normalized around 100.
    - Momentum is rate-of-change over `mom_lookback` bars: rs_ratio / rs_ratio.shift(mom_lookback) normalized around 100.
    """
    mom_lookback = max(1, int(mom_lookback))

    close_symbol = close_symbol.astype(float)
    close_benchmark = close_benchmark.astype(float)
    low_symbol = low_symbol.astype(float)
    low_benchmark = low_benchmark.astype(float)

    sym_52w = _fifty_two_week_low_by_week_start(low_symbol)
    bench_52w = _fifty_two_week_low_by_week_start(low_benchmark)
    if not sym_52w or not bench_52w:
        return pd.DataFrame()

    merged = pd.concat({"sym": close_symbol, "bench": close_benchmark}, axis=1, join="inner").dropna()
    if merged.empty:
        return pd.DataFrame()

    week_start_index = pd.to_datetime(merged.index.map(_week_start))
    if getattr(week_start_index, "tz", None) is not None:
        week_start_index = week_start_index.tz_localize(None)
    sym_ref = pd.Series([sym_52w.get(ws, float("nan")) for ws in week_start_index], index=merged.index, dtype=float)
    bench_ref = pd.Series([bench_52w.get(ws, float("nan")) for ws in week_start_index], index=merged.index, dtype=float)

    strength_sym = merged["sym"] / sym_ref
    strength_bench = merged["bench"] / bench_ref
    rs_ratio = 100.0 * (strength_sym / strength_bench)
    rs_mom = 100.0 * (rs_ratio / rs_ratio.shift(mom_lookback))

    df = pd.DataFrame({"rs_ratio": rs_ratio, "rs_mom": rs_mom}).replace([np.inf, -np.inf], np.nan).dropna()
    if df.empty:
        return df

    df["d_rs_ratio"] = df["rs_ratio"].diff()
    df["d_rs_mom"] = df["rs_mom"].diff()
    df["speed"] = np.sqrt(df["d_rs_ratio"].to_numpy() ** 2 + df["d_rs_mom"].to_numpy() ** 2)

    quad = []
    dist = []
    ang = []
    for x, y in zip(df["rs_ratio"].to_numpy(), df["rs_mom"].to_numpy(), strict=True):
        quad.append(_quadrant(float(x), float(y)))
        dx = float(x) - 100.0
        dy = float(y) - 100.0
        dist.append(math.sqrt(dx * dx + dy * dy))
        ang.append(math.degrees(math.atan2(dy, dx)))

    df["quadrant"] = quad
    df["distance"] = np.array(dist, dtype=float)
    df["angle_deg"] = np.array(ang, dtype=float)
    return df
