from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

import requests


class TradingViewScannerError(RuntimeError):
    pass


@dataclass(frozen=True)
class ScannerQuote:
    symbol: str
    name: str
    close: Optional[float]
    chg_pct: Optional[float]
    volume: Optional[float]


_DEFAULT_COLUMNS = ["name", "close", "change", "volume"]


def _chunked(items: List[str], size: int) -> List[List[str]]:
    size = max(1, int(size))
    return [items[i : i + size] for i in range(0, len(items), size)]


def fetch_quotes(
    *,
    url: str,
    symbols: Iterable[str],
    timeout: int = 20,
    batch_size: int = 200,
    columns: Optional[List[str]] = None,
) -> Dict[str, ScannerQuote]:
    tickers = [s for s in symbols if s]
    if not tickers:
        return {}

    columns = columns or list(_DEFAULT_COLUMNS)
    headers = {
        "Origin": "https://www.tradingview.com",
        "User-Agent": "Mozilla/5.0",
    }

    out: Dict[str, ScannerQuote] = {}
    for batch in _chunked(tickers, batch_size):
        payload = {
            "filter": [],
            "symbols": {"query": {"types": []}, "tickers": batch},
            "columns": columns,
            "options": {"lang": "en"},
            "sort": {"sortBy": "name", "sortOrder": "asc"},
            "range": [0, max(0, len(batch) - 1)],
        }
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:  # noqa: BLE001
            raise TradingViewScannerError(str(exc)) from exc

        rows = (data or {}).get("data") or []
        for row in rows:
            sym = (row.get("s") or "").strip().upper()
            vals = row.get("d") or []
            col_map = {columns[i]: vals[i] for i in range(min(len(columns), len(vals)))}
            name = str(col_map.get("name") or sym)

            def _to_float(x) -> Optional[float]:
                try:
                    if x is None:
                        return None
                    return float(x)
                except Exception:
                    return None

            close = _to_float(col_map.get("close"))
            volume = _to_float(col_map.get("volume"))
            chg_pct = _to_float(col_map.get("change"))
            out[sym] = ScannerQuote(symbol=sym, name=name, close=close, chg_pct=chg_pct, volume=volume)

    return out

