from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Optional


def _utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


@dataclass(frozen=True)
class AvgCacheRow:
    symbol: str
    asof_ts: int
    avg5: Optional[float]
    avg10: Optional[float]
    avg20: Optional[float]
    avg50: Optional[float]
    updated_at: str


class Database:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    def init(self) -> None:
        cur = self.conn.cursor()
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS symbols (
              symbol TEXT PRIMARY KEY,
              enabled INTEGER NOT NULL DEFAULT 1,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS avg_cache (
              symbol TEXT PRIMARY KEY,
              asof_ts INTEGER NOT NULL,
              avg5 REAL,
              avg10 REAL,
              avg20 REAL,
              avg50 REAL,
              updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS snapshot_latest (
              symbol TEXT PRIMARY KEY,
              scanned_at TEXT NOT NULL,
              vol_today REAL,
              close REAL,
              chg_pct REAL,
              avg5 REAL,
              avg10 REAL,
              avg20 REAL,
              avg50 REAL,
              ratio5 REAL,
              break5 INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS events (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts TEXT NOT NULL,
              symbol TEXT NOT NULL,
              event_type TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);

            CREATE TABLE IF NOT EXISTS reports (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              kind TEXT NOT NULL,
              period_start TEXT NOT NULL,
              generated_at TEXT NOT NULL,
              n_total INTEGER NOT NULL,
              n_break INTEGER NOT NULL,
              content TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_reports_kind_gen ON reports(kind, generated_at);

            CREATE TABLE IF NOT EXISTS state (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS errors (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts TEXT NOT NULL,
              scope TEXT NOT NULL,
              message TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_errors_ts ON errors(ts);
            """
        )
        self.conn.commit()

    # --- state ---
    def get_state(self, key: str) -> Optional[str]:
        row = self.conn.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
        return str(row["value"]) if row else None

    def set_state(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO state(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        self.conn.commit()

    # --- symbols ---
    def upsert_symbols(self, symbols: Iterable[str]) -> int:
        now = _utc_now_iso()
        n = 0
        for s in symbols:
            if not s:
                continue
            self.conn.execute(
                """
                INSERT INTO symbols(symbol, enabled, created_at, updated_at)
                VALUES(?, 1, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET updated_at=excluded.updated_at
                """,
                (s, now, now),
            )
            n += 1
        self.conn.commit()
        return n

    def set_symbol_enabled(self, symbol: str, enabled: bool) -> None:
        now = _utc_now_iso()
        self.conn.execute(
            "UPDATE symbols SET enabled=?, updated_at=? WHERE symbol=?",
            (1 if enabled else 0, now, symbol),
        )
        self.conn.commit()

    def get_enabled_symbols(self) -> List[str]:
        rows = self.conn.execute("SELECT symbol FROM symbols WHERE enabled=1 ORDER BY symbol").fetchall()
        return [str(r["symbol"]) for r in rows]

    def get_all_symbols(self) -> List[sqlite3.Row]:
        return self.conn.execute("SELECT symbol, enabled, updated_at FROM symbols ORDER BY symbol").fetchall()

    # --- avg cache ---
    def upsert_avg_cache(
        self,
        *,
        symbol: str,
        asof_ts: int,
        avg5: Optional[float],
        avg10: Optional[float],
        avg20: Optional[float],
        avg50: Optional[float],
    ) -> None:
        now = _utc_now_iso()
        self.conn.execute(
            """
            INSERT INTO avg_cache(symbol, asof_ts, avg5, avg10, avg20, avg50, updated_at)
            VALUES(?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
              asof_ts=excluded.asof_ts,
              avg5=excluded.avg5,
              avg10=excluded.avg10,
              avg20=excluded.avg20,
              avg50=excluded.avg50,
              updated_at=excluded.updated_at
            """,
            (symbol, int(asof_ts), avg5, avg10, avg20, avg50, now),
        )
        self.conn.commit()

    def get_avg_cache(self, symbol: str) -> Optional[AvgCacheRow]:
        row = self.conn.execute(
            "SELECT symbol, asof_ts, avg5, avg10, avg20, avg50, updated_at FROM avg_cache WHERE symbol=?",
            (symbol,),
        ).fetchone()
        if not row:
            return None
        return AvgCacheRow(
            symbol=str(row["symbol"]),
            asof_ts=int(row["asof_ts"]),
            avg5=row["avg5"],
            avg10=row["avg10"],
            avg20=row["avg20"],
            avg50=row["avg50"],
            updated_at=str(row["updated_at"]),
        )

    # --- snapshot latest ---
    def get_snapshot_rows(self) -> List[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT s.symbol, s.enabled,
                   sn.scanned_at, sn.vol_today, sn.close, sn.chg_pct,
                   sn.avg5, sn.avg10, sn.avg20, sn.avg50, sn.ratio5, sn.break5
            FROM symbols s
            LEFT JOIN snapshot_latest sn ON sn.symbol = s.symbol
            ORDER BY s.symbol
            """
        ).fetchall()

    def get_break5(self, symbol: str) -> bool:
        row = self.conn.execute("SELECT break5 FROM snapshot_latest WHERE symbol=?", (symbol,)).fetchone()
        if not row:
            return False
        return bool(int(row["break5"] or 0))

    def upsert_snapshot(
        self,
        *,
        symbol: str,
        scanned_at: str,
        vol_today: Optional[float],
        close: Optional[float],
        chg_pct: Optional[float],
        avg5: Optional[float],
        avg10: Optional[float],
        avg20: Optional[float],
        avg50: Optional[float],
        ratio5: Optional[float],
        break5: bool,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO snapshot_latest(symbol, scanned_at, vol_today, close, chg_pct, avg5, avg10, avg20, avg50, ratio5, break5)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
              scanned_at=excluded.scanned_at,
              vol_today=excluded.vol_today,
              close=excluded.close,
              chg_pct=excluded.chg_pct,
              avg5=excluded.avg5,
              avg10=excluded.avg10,
              avg20=excluded.avg20,
              avg50=excluded.avg50,
              ratio5=excluded.ratio5,
              break5=excluded.break5
            """,
            (
                symbol,
                scanned_at,
                vol_today,
                close,
                chg_pct,
                avg5,
                avg10,
                avg20,
                avg50,
                ratio5,
                1 if break5 else 0,
            ),
        )
        self.conn.commit()

    # --- events ---
    def insert_event(self, *, ts: str, symbol: str, event_type: str) -> None:
        self.conn.execute("INSERT INTO events(ts, symbol, event_type) VALUES(?, ?, ?)", (ts, symbol, event_type))
        self.conn.commit()

    def count_events_since(self, *, ts_iso: str, event_type: str = "break5") -> int:
        row = self.conn.execute(
            "SELECT COUNT(1) AS n FROM events WHERE ts >= ? AND event_type=?",
            (ts_iso, event_type),
        ).fetchone()
        return int(row["n"] or 0) if row else 0

    # --- reports ---
    def insert_report(
        self,
        *,
        kind: str,
        period_start: str,
        generated_at: str,
        n_total: int,
        n_break: int,
        content: str,
    ) -> None:
        self.conn.execute(
            "INSERT INTO reports(kind, period_start, generated_at, n_total, n_break, content) VALUES(?, ?, ?, ?, ?, ?)",
            (kind, period_start, generated_at, int(n_total), int(n_break), content),
        )
        self.conn.commit()

    def get_latest_report(self, kind: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM reports WHERE kind=? ORDER BY generated_at DESC, id DESC LIMIT 1",
            (kind,),
        ).fetchone()

    def get_recent_reports(self, kind: str, limit: int = 20) -> List[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM reports WHERE kind=? ORDER BY generated_at DESC, id DESC LIMIT ?",
            (kind, int(limit)),
        ).fetchall()

    # --- errors ---
    def log_error(self, *, scope: str, message: str) -> None:
        self.conn.execute(
            "INSERT INTO errors(ts, scope, message) VALUES(?, ?, ?)",
            (_utc_now_iso(), scope, str(message)),
        )
        self.conn.commit()

    def get_recent_errors(self, limit: int = 50) -> List[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM errors ORDER BY ts DESC, id DESC LIMIT ?",
            (int(limit),),
        ).fetchall()

