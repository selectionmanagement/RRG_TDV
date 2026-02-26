from __future__ import annotations

import os
import pickle
import time
from dataclasses import dataclass
from typing import Optional

import pandas as pd


@dataclass(frozen=True)
class CachePayload:
    fetched_at: float
    df: pd.DataFrame


class DiskCache:
    def __init__(self, base_dir: str) -> None:
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)

    def _path_for_key(self, key: str) -> str:
        safe = (
            key.replace(":", "_")
            .replace("|", "__")
            .replace("/", "_")
            .replace("\\", "_")
            .replace("?", "_")
            .replace("*", "_")
        )
        return os.path.join(self.base_dir, f"{safe}.pkl")

    def get_df(self, key: str, *, ttl_seconds: int) -> Optional[pd.DataFrame]:
        path = self._path_for_key(key)
        if not os.path.exists(path):
            return None
        try:
            with open(path, "rb") as f:
                payload: CachePayload = pickle.load(f)
            if (time.time() - float(payload.fetched_at)) > ttl_seconds:
                return None
            return payload.df
        except Exception:
            return None

    def set_df(self, key: str, df: pd.DataFrame) -> None:
        path = self._path_for_key(key)
        payload = CachePayload(fetched_at=time.time(), df=df)
        with open(path, "wb") as f:
            pickle.dump(payload, f, protocol=pickle.HIGHEST_PROTOCOL)

