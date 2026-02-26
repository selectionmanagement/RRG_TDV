from __future__ import annotations

import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from volume_alert.bootstrap import bootstrap_symbols
from volume_alert.config import DEFAULT_CONFIG, AppConfig
from volume_alert.db import Database
from volume_alert.metrics import compute_volume_averages
from volume_alert.reporting import SnapshotRow, build_daily_close_report, build_hourly_report
from volume_alert.time_utils import MarketClock, seconds_until_next_minute, should_send_hourly
from volume_alert.tv_scanner import TradingViewScannerError, fetch_quotes
from volume_alert.tv_ws import TradingViewWSClient


def _iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat()


def refresh_avg_cache(*, db: Database, cfg: AppConfig, symbols: list[str]) -> None:
    ws = TradingViewWSClient(url=cfg.ws_url, timeout=cfg.ws_timeout_seconds)

    def job(sym: str):
        df = ws.get_ohlcv(symbol=sym, resolution="D", bars=cfg.avg_history_bars)
        av = compute_volume_averages(df)
        return sym, av

    errors = 0
    with ThreadPoolExecutor(max_workers=max(1, int(cfg.avg_refresh_workers))) as ex:
        futs = {ex.submit(job, s): s for s in symbols}
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                _sym, av = fut.result()
                if av is None:
                    db.log_error(scope="avg_cache", message=f"{sym}: not enough data")
                    continue
                db.upsert_avg_cache(
                    symbol=sym,
                    asof_ts=av.asof_ts,
                    avg5=av.avg5,
                    avg10=av.avg10,
                    avg20=av.avg20,
                    avg50=av.avg50,
                )
            except Exception as exc:  # noqa: BLE001
                errors += 1
                db.log_error(scope="avg_cache", message=f"{sym}: {exc}")

    if errors:
        db.log_error(scope="avg_cache", message=f"refresh completed with {errors} errors")


def _missing_avg_cache_symbols(db: Database, symbols: list[str]) -> list[str]:
    return [sym for sym in symbols if db.get_avg_cache(sym) is None]


def main() -> int:
    cfg = DEFAULT_CONFIG
    db_path = str(ROOT / "data" / "volume_alert.sqlite")
    db = Database(db_path)
    db.init()
    bootstrap_symbols(db=db, cfg=cfg)

    clock = MarketClock(tz_name=cfg.timezone, sessions=cfg.market_sessions, daily_close_time=cfg.daily_close_report_time)

    last_avg_refresh_day = db.get_state("avg_refresh_day")  # yyyy-mm-dd (Bangkok)

    while True:
        now = clock.now()
        market_open = clock.is_market_open(now)
        symbols = db.get_enabled_symbols()

        if symbols and market_open:
            today_key = now.date().isoformat()
            need_daily_refresh = today_key != (last_avg_refresh_day or "")
            missing_avg_symbols = _missing_avg_cache_symbols(db, symbols)
            refresh_targets = symbols if need_daily_refresh else missing_avg_symbols
            if refresh_targets:
                refresh_avg_cache(db=db, cfg=cfg, symbols=refresh_targets)
            if need_daily_refresh:
                db.set_state("avg_refresh_day", today_key)
                last_avg_refresh_day = today_key

            try:
                quotes = fetch_quotes(
                    url=cfg.scanner_url,
                    symbols=symbols,
                    timeout=cfg.scanner_timeout_seconds,
                    batch_size=cfg.scanner_batch_size,
                )
            except TradingViewScannerError as exc:
                db.log_error(scope="scanner", message=str(exc))
                quotes = {}

            for sym in symbols:
                q = quotes.get(sym)
                avg = db.get_avg_cache(sym)
                if q is None or avg is None:
                    continue

                vol_today = q.volume
                avg5 = float(avg.avg5) if avg.avg5 is not None else None
                avg10 = float(avg.avg10) if avg.avg10 is not None else None
                avg20 = float(avg.avg20) if avg.avg20 is not None else None
                avg50 = float(avg.avg50) if avg.avg50 is not None else None

                break5 = bool(vol_today is not None and avg5 is not None and avg5 > 0 and float(vol_today) > avg5)
                ratio5 = (float(vol_today) / avg5) if (vol_today is not None and avg5 is not None and avg5 > 0) else None

                prev_break = db.get_break5(sym)
                if break5 and not prev_break:
                    db.insert_event(ts=_iso(now), symbol=sym, event_type="break5")

                db.upsert_snapshot(
                    symbol=sym,
                    scanned_at=_iso(now),
                    vol_today=float(vol_today) if vol_today is not None else None,
                    close=float(q.close) if q.close is not None else None,
                    chg_pct=float(q.chg_pct) if q.chg_pct is not None else None,
                    avg5=avg5,
                    avg10=avg10,
                    avg20=avg20,
                    avg50=avg50,
                    ratio5=ratio5,
                    break5=break5,
                )

            # Hourly report (ต้องสร้างแม้ Break=0)
            last_sent_hour = db.get_state("last_hourly_key")
            if should_send_hourly(dt=now, last_sent_hour=last_sent_hour, require_market_open=True, market_open=True):
                rows = db.get_snapshot_rows()
                snap_rows = [
                    SnapshotRow(
                        symbol=str(r["symbol"]),
                        vol_today=r["vol_today"],
                        close=r["close"],
                        chg_pct=r["chg_pct"],
                        avg5=r["avg5"],
                        avg10=r["avg10"],
                        avg20=r["avg20"],
                        avg50=r["avg50"],
                        ratio5=r["ratio5"],
                        break5=bool(int(r["break5"] or 0)),
                    )
                    for r in rows
                    if int(r["enabled"] or 0) == 1
                ]

                new_in_hour = db.count_events_since(ts_iso=_iso(now - timedelta(hours=1)), event_type="break5")
                content = build_hourly_report(dt=now, universe_size=len(symbols), new_in_hour=new_in_hour, rows=snap_rows)
                db.insert_report(
                    kind="hourly",
                    period_start=now.replace(minute=0, second=0, microsecond=0).isoformat(),
                    generated_at=_iso(now),
                    n_total=len(symbols),
                    n_break=sum(1 for r in snap_rows if r.break5),
                    content=content,
                )
                db.set_state("last_hourly_key", now.strftime("%Y-%m-%d %H"))

        # Daily close report (16:30, Mon-Fri) regardless of market_open
        last_daily_day = db.get_state("last_daily_day")
        if symbols and clock.should_daily(now, last_sent_day=last_daily_day):
            rows = db.get_snapshot_rows()
            snap_rows = [
                SnapshotRow(
                    symbol=str(r["symbol"]),
                    vol_today=r["vol_today"],
                    close=r["close"],
                    chg_pct=r["chg_pct"],
                    avg5=r["avg5"],
                    avg10=r["avg10"],
                    avg20=r["avg20"],
                    avg50=r["avg50"],
                    ratio5=r["ratio5"],
                    break5=bool(int(r["break5"] or 0)),
                )
                for r in rows
                if int(r["enabled"] or 0) == 1
            ]
            content = build_daily_close_report(dt=now, universe_size=len(symbols), rows=snap_rows)
            db.insert_report(
                kind="daily",
                period_start=now.date().isoformat(),
                generated_at=_iso(now),
                n_total=len(symbols),
                n_break=sum(1 for r in snap_rows if r.break5),
                content=content,
            )
            db.set_state("last_daily_day", now.date().isoformat())

        interval = max(1, int(cfg.scan_interval_seconds))
        if interval == 60:
            time.sleep(seconds_until_next_minute(clock.now()))
        else:
            time.sleep(float(interval))


if __name__ == "__main__":
    raise SystemExit(main())
