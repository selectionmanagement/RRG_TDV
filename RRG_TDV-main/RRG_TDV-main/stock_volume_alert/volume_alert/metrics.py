from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class VolumeAverages:
    asof_ts: int
    avg5: Optional[float]
    avg10: Optional[float]
    avg20: Optional[float]
    avg50: Optional[float]


def _mean_or_none(values: np.ndarray) -> Optional[float]:
    values = values.astype(float)
    values = values[~np.isnan(values)]
    if values.size == 0:
        return None
    return float(values.mean())


def compute_volume_averages(df: pd.DataFrame) -> Optional[VolumeAverages]:
    if df is None or df.empty:
        return None
    if "volume" not in df.columns:
        return None

    df = df.dropna(subset=["volume"]).sort_index()
    if df.empty:
        return None

    # Exclude the latest bar (treated as "today" / in-progress bar during market hours).
    if len(df) >= 2:
        hist = df.iloc[:-1]
        asof_ts = int(df.index[-2].timestamp())
    else:
        hist = df
        asof_ts = int(df.index[-1].timestamp())

    v = hist["volume"].astype(float).to_numpy()

    def avg(n: int) -> Optional[float]:
        if v.size < n:
            return None
        return _mean_or_none(v[-n:])

    return VolumeAverages(asof_ts=asof_ts, avg5=avg(5), avg10=avg(10), avg20=avg(20), avg50=avg(50))

