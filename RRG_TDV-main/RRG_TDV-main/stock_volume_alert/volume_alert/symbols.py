from __future__ import annotations

import re
from typing import Iterable, List


def _strip_quotes(token: str) -> str:
    s = (token or "").strip()
    # Clean common copy/paste forms like "ADVANC" or 'ADVANC'.
    if len(s) >= 2 and s[0] == s[-1] and s[0] in {'"', "'"}:
        s = s[1:-1].strip()
    return s


def parse_symbol_list(raw: str) -> List[str]:
    if not raw:
        return []
    parts = re.split(r"[\n,;]+", raw)
    symbols: List[str] = []
    seen = set()
    for p in parts:
        s = _strip_quotes(p).upper()
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        symbols.append(s)
    return symbols


def normalize_symbol(symbol: str, *, default_exchange: str = "SET") -> str:
    s = _strip_quotes(symbol).upper()
    if not s:
        return ""
    if ":" in s:
        ex, ticker = s.split(":", 1)
        ex = _strip_quotes(ex).upper()
        ticker = _strip_quotes(ticker).upper()
        if not ex or not ticker:
            return ""
        return f"{ex}:{ticker}"
    default_exchange = (default_exchange or "").strip().upper() or "SET"
    return f"{default_exchange}:{s}"


def normalize_symbols(symbols: Iterable[str], *, default_exchange: str = "SET") -> List[str]:
    out: List[str] = []
    seen = set()
    for s in symbols:
        ns = normalize_symbol(s, default_exchange=default_exchange)
        if not ns or ns in seen:
            continue
        seen.add(ns)
        out.append(ns)
    return out
