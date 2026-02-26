from __future__ import annotations

import json
import random
import string
import time
from dataclasses import dataclass
from typing import Dict, List

import pandas as pd
import websocket


class TradingViewWSError(RuntimeError):
    pass


def _rand_session(prefix: str) -> str:
    return prefix + "".join(random.choice(string.ascii_lowercase) for _ in range(12))


def _pack(obj: Dict) -> str:
    payload = json.dumps(obj, separators=(",", ":"))
    return f"~m~{len(payload)}~m~{payload}"


def _iter_frames(raw: str) -> List[str]:
    frames: List[str] = []
    i = 0
    while i < len(raw):
        if raw.startswith("~h~", i):
            j = raw.find("~m~", i)
            if j == -1:
                frames.append(raw[i:])
                break
            frames.append(raw[i:j])
            i = j
            continue

        if not raw.startswith("~m~", i):
            j = raw.find("~m~", i)
            if j == -1:
                break
            i = j
            continue

        i += 3
        j = raw.find("~m~", i)
        if j == -1:
            break
        length_str = raw[i:j]
        try:
            length = int(length_str)
        except ValueError:
            break
        i = j + 3
        payload = raw[i : i + length]
        i += length
        frames.append(payload)
    return frames


@dataclass
class TradingViewWSClient:
    url: str = "wss://data.tradingview.com/socket.io/websocket"
    timeout: int = 20

    def _connect(self) -> websocket.WebSocket:
        headers = [
            "Origin: https://www.tradingview.com",
            "User-Agent: Mozilla/5.0",
        ]
        return websocket.create_connection(self.url, header=headers, timeout=self.timeout, enable_multithread=True)

    def _send(self, ws: websocket.WebSocket, obj: Dict) -> None:
        ws.send(_pack(obj))

    def get_ohlcv(self, *, symbol: str, resolution: str = "D", bars: int = 120) -> pd.DataFrame:
        symbol = (symbol or "").strip().upper()
        if not symbol or ":" not in symbol:
            raise TradingViewWSError(f"Invalid symbol: {symbol!r} (expected like 'SET:ADVANC')")

        resolution = (resolution or "").strip().upper()
        if resolution not in {"D", "W"}:
            raise TradingViewWSError(f"Unsupported resolution: {resolution!r} (use 'D' or 'W')")

        bars = int(bars)
        if bars <= 0:
            raise TradingViewWSError("bars must be > 0")

        ws = self._connect()
        try:
            try:
                ws.recv()
            except Exception:
                pass

            chart_session = _rand_session("cs_")
            quote_session = _rand_session("qs_")

            self._send(ws, {"m": "set_auth_token", "p": ["unauthorized_user_token"]})
            self._send(ws, {"m": "chart_create_session", "p": [chart_session, ""]})
            self._send(ws, {"m": "quote_create_session", "p": [quote_session]})
            self._send(
                ws,
                {
                    "m": "quote_set_fields",
                    "p": [quote_session, "lp", "ch", "chp", "volume", "short_name", "exchange", "description", "type"],
                },
            )
            self._send(ws, {"m": "quote_add_symbols", "p": [quote_session, symbol]})
            self._send(
                ws,
                {
                    "m": "resolve_symbol",
                    "p": [
                        chart_session,
                        "symbol_1",
                        f'={{"symbol":"{symbol}","adjustment":"splits","session":"regular"}}',
                    ],
                },
            )
            self._send(ws, {"m": "create_series", "p": [chart_session, "s1", "s1", "symbol_1", resolution, bars]})
            self._send(ws, {"m": "switch_timezone", "p": [chart_session, "Etc/UTC"]})

            ts: List[int] = []
            o: List[float] = []
            h: List[float] = []
            l: List[float] = []
            c: List[float] = []
            v: List[float] = []

            series_done = False
            start = time.time()

            while True:
                if time.time() - start > self.timeout:
                    raise TradingViewWSError(f"Timeout while fetching {symbol} ({resolution}, bars={bars})")

                raw = ws.recv()
                for frame in _iter_frames(raw):
                    if frame.startswith("~h~"):
                        ws.send(f"~m~{len(frame)}~m~{frame}")
                        continue

                    try:
                        msg = json.loads(frame)
                    except json.JSONDecodeError:
                        continue

                    m = msg.get("m")
                    if m == "timescale_update":
                        payload = msg.get("p", [None, None])[1] or {}
                        s1 = payload.get("s1")
                        if not isinstance(s1, dict):
                            continue

                        rows = s1.get("s")
                        if isinstance(rows, list) and rows:
                            for row in rows:
                                if not isinstance(row, dict):
                                    continue
                                vals = row.get("v")
                                if not isinstance(vals, list) or len(vals) < 5:
                                    continue
                                try:
                                    t = int(float(vals[0]))
                                except Exception:
                                    continue
                                ts.append(t)
                                o.append(float(vals[1]) if vals[1] is not None else float("nan"))
                                h.append(float(vals[2]) if vals[2] is not None else float("nan"))
                                l.append(float(vals[3]) if vals[3] is not None else float("nan"))
                                c.append(float(vals[4]) if vals[4] is not None else float("nan"))
                                vol = float(vals[5]) if len(vals) > 5 and vals[5] is not None else float("nan")
                                v.append(vol)
                            continue

                        t_arr = s1.get("t")
                        if not isinstance(t_arr, list) or not t_arr:
                            continue
                        n = len(t_arr)
                        ts.extend(t_arr)

                        def _norm(arr) -> List[float]:
                            if not isinstance(arr, list) or not arr:
                                return [float("nan")] * n
                            if len(arr) < n:
                                return list(arr) + [float("nan")] * (n - len(arr))
                            if len(arr) > n:
                                return list(arr[:n])
                            return list(arr)

                        o.extend(_norm(s1.get("o")))
                        h.extend(_norm(s1.get("h")))
                        l.extend(_norm(s1.get("l")))
                        c.extend(_norm(s1.get("c")))
                        v.extend(_norm(s1.get("v")))

                    if m == "series_completed":
                        series_done = True

                if series_done and ts:
                    break

            df = pd.DataFrame({"time": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})
            df["time"] = pd.to_numeric(df["time"], errors="coerce")
            df = df.dropna(subset=["time"])
            df = df.drop_duplicates(subset=["time"]).sort_values("time")
            df.index = pd.to_datetime(df["time"].astype("int64"), unit="s", utc=True)
            df = df.drop(columns=["time"])
            return df
        except TradingViewWSError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise TradingViewWSError(str(exc)) from exc
        finally:
            try:
                ws.close()
            except Exception:
                pass

