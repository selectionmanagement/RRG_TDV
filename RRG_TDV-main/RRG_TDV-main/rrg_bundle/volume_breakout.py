from __future__ import annotations

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
VOLUME_ROOT = ROOT / "stock_volume_alert"
if str(VOLUME_ROOT) not in sys.path:
    sys.path.insert(0, str(VOLUME_ROOT))

try:
    from volume_alert.config import DEFAULT_CONFIG
    from volume_alert.metrics import compute_volume_averages
    from volume_alert.symbols import normalize_symbols, parse_symbol_list
    from volume_alert.tv_scanner import TradingViewScannerError, fetch_quotes
    from volume_alert.tv_ws import TradingViewWSClient, TradingViewWSError
except Exception as exc:  # noqa: BLE001
    DEFAULT_CONFIG = None
    compute_volume_averages = None
    normalize_symbols = None
    parse_symbol_list = None
    TradingViewScannerError = None
    TradingViewWSError = None
    TradingViewWSClient = None
    fetch_quotes = None
    _IMPORT_ERROR = exc
else:
    _IMPORT_ERROR = None

TH_TZ = ZoneInfo("Asia/Bangkok")
SYMBOLS_FILE = VOLUME_ROOT / "data" / "symbols.txt"


@st.cache_resource
def _get_ws() -> TradingViewWSClient:  # type: ignore[valid-type]
    cfg = DEFAULT_CONFIG
    return TradingViewWSClient(url=cfg.ws_url, timeout=cfg.ws_timeout_seconds)  # type: ignore[call-arg]


def _fmt_dt(dt: Optional[datetime]) -> str:
    if dt is None:
        return "-"
    return dt.astimezone(TH_TZ).strftime("%Y-%m-%d %H:%M:%S")


def _load_symbols(cfg=DEFAULT_CONFIG) -> list[str]:
    if SYMBOLS_FILE.exists():
        raw = SYMBOLS_FILE.read_text(encoding="utf-8")
        saved = normalize_symbols(parse_symbol_list(raw), default_exchange=cfg.default_exchange_prefix)
        if saved:
            return saved
    return normalize_symbols(cfg.default_symbols, default_exchange=cfg.default_exchange_prefix)


def _save_symbols(symbols: list[str]) -> None:
    SYMBOLS_FILE.parent.mkdir(parents=True, exist_ok=True)
    SYMBOLS_FILE.write_text("\n".join(symbols), encoding="utf-8")


def _avg_for_symbol(*, symbol: str, bars: int, cfg=DEFAULT_CONFIG) -> tuple[str, Optional[dict], Optional[str]]:
    try:
        ws = TradingViewWSClient(url=cfg.ws_url, timeout=cfg.ws_timeout_seconds)
        ohlcv = ws.get_ohlcv(symbol=symbol, resolution="D", bars=int(bars))
        av = compute_volume_averages(ohlcv)
        if av is None:
            return symbol, None, "not enough daily bars"
        return (
            symbol,
            {"avg5": av.avg5, "avg10": av.avg10, "avg20": av.avg20, "avg50": av.avg50},
            None,
        )
    except Exception as exc:  # noqa: BLE001
        return symbol, None, str(exc)


def _build_live_snapshot(*, symbols: list[str], avg_workers: int, cfg=DEFAULT_CONFIG) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not symbols:
        return pd.DataFrame(), pd.DataFrame(columns=["symbol", "error"])

    try:
        quotes = fetch_quotes(
            url=cfg.scanner_url,
            symbols=symbols,
            timeout=cfg.scanner_timeout_seconds,
            batch_size=cfg.scanner_batch_size,
        )
    except TradingViewScannerError as exc:
        err = pd.DataFrame([{"symbol": "*scanner*", "error": str(exc)}])
        return pd.DataFrame(), err

    avg_by_symbol: dict[str, dict] = {}
    errors: list[dict[str, str]] = []
    max_workers = max(1, int(avg_workers))
    bars = max(120, cfg.avg_history_bars)
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(_avg_for_symbol, symbol=sym, bars=bars, cfg=cfg): sym for sym in symbols}
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                _sym, data, err = fut.result()
                if err or data is None:
                    errors.append({"symbol": sym, "error": err or "avg unavailable"})
                    continue
                avg_by_symbol[sym] = data
            except Exception as exc:  # noqa: BLE001
                errors.append({"symbol": sym, "error": str(exc)})

    scanned_at = datetime.now(tz=TH_TZ)
    rows: list[dict] = []
    for sym in symbols:
        q = quotes.get(sym)
        avg = avg_by_symbol.get(sym) or {}
        vol_today = float(q.volume) if (q and q.volume is not None) else None
        avg5 = avg.get("avg5")
        avg10 = avg.get("avg10")
        avg20 = avg.get("avg20")
        avg50 = avg.get("avg50")
        ratio5 = (float(vol_today) / float(avg5)) if (vol_today is not None and avg5 is not None and avg5 > 0) else None
        break5 = bool(vol_today is not None and avg5 is not None and avg5 > 0 and float(vol_today) > float(avg5))
        rows.append(
            {
                "symbol": sym,
                "scanned_at": scanned_at,
                "vol_today": vol_today,
                "avg5": avg5,
                "avg10": avg10,
                "avg20": avg20,
                "avg50": avg50,
                "ratio5": ratio5,
                "close": float(q.close) if (q and q.close is not None) else None,
                "chg_pct": float(q.chg_pct) if (q and q.chg_pct is not None) else None,
                "break5": break5,
            }
        )
    df = pd.DataFrame(rows)
    err_df = pd.DataFrame(errors) if errors else pd.DataFrame(columns=["symbol", "error"])
    return df, err_df


def _backfill_for_symbol(*, symbol: str, bars: int, cfg=DEFAULT_CONFIG) -> tuple[str, Optional[pd.DataFrame], Optional[str]]:
    try:
        ws = TradingViewWSClient(url=cfg.ws_url, timeout=cfg.ws_timeout_seconds)
        ohlcv = ws.get_ohlcv(symbol=symbol, resolution="D", bars=int(bars))
        if ohlcv.empty or "volume" not in ohlcv.columns:
            return symbol, None, "no volume history"

        vol = pd.to_numeric(ohlcv["volume"], errors="coerce")
        avg5 = vol.shift(1).rolling(5).mean()
        dt_idx = pd.to_datetime(vol.index, utc=True, errors="coerce")
        dates = dt_idx.tz_convert(TH_TZ).date

        out = pd.DataFrame(
            {
                "date": dates,
                "symbol": symbol,
                "vol_today": vol.values,
                "avg5": avg5.values,
            }
        )
        out = out.dropna(subset=["vol_today", "avg5"])
        if out.empty:
            return symbol, None, "not enough bars for avg5"
        out["break5"] = (out["avg5"] > 0) & (out["vol_today"] > out["avg5"])
        out["ratio5"] = out["vol_today"] / out["avg5"]
        return symbol, out, None
    except Exception as exc:  # noqa: BLE001
        return symbol, None, str(exc)


def _build_backfill_daily_history(
    *,
    symbols: list[str],
    days: int,
    workers: int,
    cfg=DEFAULT_CONFIG,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if not symbols:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(columns=["symbol", "error"])

    bars = max(120, int(days) + 80)
    max_workers = max(1, int(workers))
    frames: list[pd.DataFrame] = []
    errors: list[dict[str, str]] = []

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(_backfill_for_symbol, symbol=sym, bars=bars, cfg=cfg): sym for sym in symbols}
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                _sym, frame, err = fut.result()
                if err or frame is None:
                    errors.append({"symbol": sym, "error": err or "unknown error"})
                    continue
                frames.append(frame)
            except Exception as exc:  # noqa: BLE001
                errors.append({"symbol": sym, "error": str(exc)})

    if not frames:
        err_df = pd.DataFrame(errors) if errors else pd.DataFrame(columns=["symbol", "error"])
        return pd.DataFrame(), pd.DataFrame(), err_df

    full_df = pd.concat(frames, ignore_index=True)
    cutoff_day = datetime.now(tz=TH_TZ).date() - timedelta(days=max(1, int(days)) - 1)
    full_df = full_df[full_df["date"] >= cutoff_day].copy()
    if full_df.empty:
        err_df = pd.DataFrame(errors) if errors else pd.DataFrame(columns=["symbol", "error"])
        return pd.DataFrame(), pd.DataFrame(), err_df

    daily_df = (
        full_df.groupby("date", as_index=False)
        .agg(
            n_total=("symbol", "nunique"),
            n_break=("break5", "sum"),
        )
        .sort_values("date")
    )
    daily_df["n_break"] = daily_df["n_break"].astype(int)
    daily_df["break_ratio_pct"] = (daily_df["n_break"] / daily_df["n_total"].clip(lower=1)) * 100.0
    daily_df["date"] = daily_df["date"].astype(str)

    break_df = full_df[full_df["break5"]].copy()
    break_df["date"] = break_df["date"].astype(str)
    break_df = break_df.sort_values(["date", "ratio5"], ascending=[False, False])

    err_df = pd.DataFrame(errors) if errors else pd.DataFrame(columns=["symbol", "error"])
    return daily_df, break_df, err_df


def render_volume_breakout(lang: str = "th") -> None:
    if _IMPORT_ERROR is not None:
        st.error(f"Volume Breakout modules not available: {_IMPORT_ERROR}")
        return
    if DEFAULT_CONFIG is None:
        st.error("Volume Breakout config is missing.")
        return

    labels = {
        "en": {
            "title": "Volume Breakout (AVG5)",
            "tab_symbols": "Symbols",
            "tab_live": "Live",
            "tab_chart": "Chart",
            "tab_backfill": "Backfill",
            "tab_errors": "Errors",
            "symbols_help": "1 symbol per line (e.g. ADVANC or SET:ADVANC)",
            "save_symbols": "Save symbols",
            "symbols_stored": "Symbols are stored in stock_volume_alert/data/symbols.txt",
            "live_title": "Live Snapshot",
            "polling": "Polling interval target: {sec}s | Timezone: Asia/Bangkok",
            "refresh": "Refresh now",
            "show_break_only": "Show only Break AVG5",
            "chart_title": "Chart (Candlestick + SMA5/10/20/50)",
            "backfill_title": "Backfill Daily (No SQL)",
            "backfill_caption": "This calculates history on demand from TradingView and keeps it only in session.",
            "backfill_days": "Backfill days",
            "backfill_workers": "Backfill workers",
            "run_backfill": "Run backfill now",
            "backfill_complete": "Backfill complete",
            "errors_title": "Errors",
            "no_live": "No live data yet. Click Refresh now.",
            "no_symbols": "No symbols enabled",
        },
        "th": {
            "title": "Volume Breakout (AVG5)",
            "tab_symbols": "รายชื่อหุ้น",
            "tab_live": "Live",
            "tab_chart": "Chart",
            "tab_backfill": "Backfill",
            "tab_errors": "ข้อผิดพลาด",
            "symbols_help": "1 หุ้นต่อบรรทัด (เช่น ADVANC หรือ SET:ADVANC)",
            "save_symbols": "บันทึกรายชื่อหุ้น",
            "symbols_stored": "เก็บรายชื่อไว้ที่ stock_volume_alert/data/symbols.txt",
            "live_title": "Live Snapshot",
            "polling": "ตั้งเป้ารอบดึงข้อมูล: {sec}s | เวลา: Asia/Bangkok",
            "refresh": "รีเฟรช",
            "show_break_only": "แสดงเฉพาะ Break AVG5",
            "chart_title": "Chart (Candlestick + SMA5/10/20/50)",
            "backfill_title": "Backfill รายวัน (No SQL)",
            "backfill_caption": "คำนวณย้อนหลังแบบ on-demand และเก็บใน session เท่านั้น",
            "backfill_days": "จำนวนวันย้อนหลัง",
            "backfill_workers": "จำนวน worker",
            "run_backfill": "เริ่ม Backfill",
            "backfill_complete": "Backfill เสร็จสิ้น",
            "errors_title": "ข้อผิดพลาด",
            "no_live": "ยังไม่มีข้อมูล Live กดรีเฟรชก่อน",
            "no_symbols": "ไม่มีรายชื่อหุ้น",
        },
    }
    lang_key = "th" if lang not in {"th", "en"} else lang
    t = labels[lang_key]

    st.subheader(t["title"])

    cfg = DEFAULT_CONFIG
    if "vb_symbols" not in st.session_state:
        st.session_state["vb_symbols"] = _load_symbols(cfg)

    tabs = st.tabs([t["tab_symbols"], t["tab_live"], t["tab_chart"], t["tab_backfill"], t["tab_errors"]])

    with tabs[0]:
        st.subheader(t["tab_symbols"])
        raw_default = "\n".join(st.session_state["vb_symbols"])
        raw = st.text_area(t["symbols_help"], value=raw_default, height=320)
        col1, col2 = st.columns([1, 3])
        with col1:
            if st.button(t["save_symbols"], type="primary", key="vb_save_symbols"):
                syms = normalize_symbols(parse_symbol_list(raw), default_exchange=cfg.default_exchange_prefix)
                st.session_state["vb_symbols"] = syms
                _save_symbols(syms)
                st.success(f"Saved {len(syms)} symbols")
        with col2:
            st.caption(t["symbols_stored"])

        sym_df = pd.DataFrame({"symbol": st.session_state["vb_symbols"]})
        st.dataframe(sym_df, use_container_width=True, hide_index=True)

    with tabs[1]:
        st.subheader(t["live_title"])
        st.caption(t["polling"].format(sec=cfg.scan_interval_seconds))
        avg_workers = st.slider(
            "AVG workers",
            min_value=1,
            max_value=8,
            value=max(1, min(4, int(cfg.avg_refresh_workers))),
            step=1,
            key="vb_avg_workers",
        )
        refresh = st.button(t["refresh"], type="primary", key="vb_refresh_live")
        if refresh:
            with st.spinner("Fetching live snapshot..."):
                live_df, err_df = _build_live_snapshot(
                    symbols=st.session_state["vb_symbols"],
                    avg_workers=avg_workers,
                    cfg=cfg,
                )
            st.session_state["vb_live_df"] = live_df
            st.session_state["vb_live_err_df"] = err_df
            st.session_state["vb_live_updated_at"] = datetime.now(tz=TH_TZ)

        live_df = st.session_state.get("vb_live_df")
        if isinstance(live_df, pd.DataFrame) and not live_df.empty:
            show_break_only = st.checkbox(t["show_break_only"], value=True, key="vb_show_break_only")
            df_view = live_df.copy()
            if show_break_only:
                df_view = df_view[df_view["break5"] == True]  # noqa: E712
            df_view = df_view.sort_values(["break5", "ratio5"], ascending=[False, False], na_position="last")

            imported_rows = int(live_df["vol_today"].notna().sum())
            break_rows = int(live_df["break5"].sum())
            updated = st.session_state.get("vb_live_updated_at")
            k1, k2, k3 = st.columns(3)
            with k1:
                st.metric("Universe", int(live_df.shape[0]))
            with k2:
                st.metric("Break AVG5", break_rows)
            with k3:
                st.metric("Last import (TH)", _fmt_dt(updated))

            st.dataframe(
                df_view[
                    [
                        "symbol",
                        "scanned_at",
                        "vol_today",
                        "avg5",
                        "avg10",
                        "avg20",
                        "avg50",
                        "ratio5",
                        "close",
                        "chg_pct",
                        "break5",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
            )
            st.caption(f"Imported rows: {imported_rows}")
        else:
            st.info(t["no_live"])

    with tabs[2]:
        st.subheader(t["chart_title"])
        symbols = st.session_state["vb_symbols"]
        if not symbols:
            st.info(t["no_symbols"])
        else:
            symbol = st.selectbox("Symbol", symbols, index=0, key="vb_chart_symbol")
            bars = st.slider("Bars", min_value=60, max_value=260, value=120, step=10, key="vb_chart_bars")
            ws = _get_ws()
            try:
                ohlcv = ws.get_ohlcv(symbol=symbol, resolution="D", bars=int(bars))
                ohlcv = ohlcv.sort_index()
                df = ohlcv.copy()
                for col in ("open", "high", "low", "close", "volume"):
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                df = df.dropna(subset=["open", "high", "low", "close"])
                if df.empty:
                    st.warning("No OHLC data for chart.")
                else:
                    x_idx = pd.to_datetime(df.index, errors="coerce")
                    if getattr(x_idx, "tz", None) is not None:
                        x_idx = x_idx.tz_convert(TH_TZ)
                    for w in (5, 10, 20, 50):
                        df[f"sma{w}"] = df["close"].rolling(w).mean()

                    fig_price = go.Figure()
                    fig_price.add_trace(
                        go.Candlestick(
                            x=x_idx,
                            open=df["open"],
                            high=df["high"],
                            low=df["low"],
                            close=df["close"],
                            name="OHLC",
                        )
                    )
                    for w, color in ((5, "#00E676"), (10, "#00B0FF"), (20, "#FFD54F"), (50, "#FF7043")):
                        fig_price.add_trace(
                            go.Scatter(
                                x=x_idx,
                                y=df[f"sma{w}"],
                                mode="lines",
                                line=dict(width=1.8, color=color),
                                name=f"SMA{w}",
                            )
                        )
                    fig_price.update_layout(
                        height=560,
                        legend=dict(orientation="h"),
                        xaxis_rangeslider_visible=False,
                        margin=dict(l=10, r=10, t=10, b=10),
                    )
                    st.plotly_chart(fig_price, use_container_width=True)

                    latest = df.iloc[-1]
                    sma_rows = []
                    close_last = float(latest["close"]) if pd.notna(latest["close"]) else None
                    for w in (5, 10, 20, 50):
                        sma_val = latest.get(f"sma{w}")
                        if pd.isna(sma_val):
                            continue
                        sma_val = float(sma_val)
                        diff = (close_last - sma_val) if close_last is not None else None
                        if diff is None:
                            status = "N/A"
                        elif diff > 0:
                            status = "Above"
                        elif diff < 0:
                            status = "Below"
                        else:
                            status = "At SMA"
                        sma_rows.append(
                            {
                                "metric": f"SMA{w}",
                                "value": sma_val,
                                "close_vs_sma": diff,
                                "status": status,
                            }
                        )
                    if sma_rows:
                        st.dataframe(pd.DataFrame(sma_rows), use_container_width=True, hide_index=True)

                    vol = pd.to_numeric(df["volume"], errors="coerce")
                    vol_df = pd.DataFrame({"volume": vol})
                    vol_df["avg5"] = vol_df["volume"].shift(1).rolling(5).mean()
                    vol_df["avg10"] = vol_df["volume"].shift(1).rolling(10).mean()
                    vol_df["avg20"] = vol_df["volume"].shift(1).rolling(20).mean()
                    vol_df["avg50"] = vol_df["volume"].shift(1).rolling(50).mean()
                    fig_vol = go.Figure()
                    fig_vol.add_trace(go.Bar(x=x_idx, y=vol_df["volume"], name="Volume"))
                    for w in (5, 10, 20, 50):
                        fig_vol.add_trace(go.Scatter(x=x_idx, y=vol_df[f"avg{w}"], mode="lines", name=f"AVG{w} (prev)"))
                    fig_vol.update_layout(height=300, legend=dict(orientation="h"), margin=dict(l=10, r=10, t=10, b=10))
                    st.plotly_chart(fig_vol, use_container_width=True)
            except TradingViewWSError as exc:
                st.error(f"TradingView error: {exc}")

    with tabs[3]:
        st.subheader(t["backfill_title"])
        st.caption(t["backfill_caption"])
        col_a, col_b = st.columns(2)
        with col_a:
            backfill_days = st.slider(
                t["backfill_days"],
                min_value=7,
                max_value=120,
                value=60,
                step=1,
                key="vb_backfill_days",
            )
        with col_b:
            backfill_workers = st.slider(
                t["backfill_workers"],
                min_value=1,
                max_value=8,
                value=max(1, min(4, int(cfg.avg_refresh_workers))),
                step=1,
                key="vb_backfill_workers",
            )

        if st.button(t["run_backfill"], type="primary", key="vb_run_backfill"):
            symbols = st.session_state["vb_symbols"]
            if not symbols:
                st.warning(t["no_symbols"])
            else:
                with st.spinner(f"Backfill {len(symbols)} symbols x {backfill_days} days..."):
                    daily_df, break_df, err_df = _build_backfill_daily_history(
                        symbols=symbols,
                        days=backfill_days,
                        workers=backfill_workers,
                        cfg=cfg,
                    )
                st.session_state["vb_backfill_daily_df"] = daily_df
                st.session_state["vb_backfill_break_df"] = break_df
                st.session_state["vb_backfill_err_df"] = err_df
                st.session_state["vb_backfill_updated_at"] = datetime.now(tz=TH_TZ)
                st.success(t["backfill_complete"])

        daily_df = st.session_state.get("vb_backfill_daily_df")
        break_df = st.session_state.get("vb_backfill_break_df")
        err_df = st.session_state.get("vb_backfill_err_df")
        updated = st.session_state.get("vb_backfill_updated_at")
        st.caption(f"Last backfill (TH): {_fmt_dt(updated)}")

        if isinstance(daily_df, pd.DataFrame) and not daily_df.empty:
            hist_df = daily_df.sort_values("date")
            fig_hist = go.Figure()
            fig_hist.add_trace(go.Bar(x=hist_df["date"], y=hist_df["n_break"], name="Break AVG5"))
            fig_hist.add_trace(
                go.Scatter(
                    x=hist_df["date"],
                    y=hist_df["break_ratio_pct"],
                    name="Break %",
                    mode="lines+markers",
                    yaxis="y2",
                )
            )
            fig_hist.update_layout(
                height=360,
                legend=dict(orientation="h"),
                yaxis=dict(title="Break count"),
                yaxis2=dict(title="Break %", overlaying="y", side="right"),
                margin=dict(l=10, r=10, t=10, b=10),
            )
            st.plotly_chart(fig_hist, use_container_width=True)

            show_df = daily_df[["date", "n_total", "n_break", "break_ratio_pct"]].copy()
            show_df = show_df.sort_values("date", ascending=False)
            show_df["break_ratio_pct"] = show_df["break_ratio_pct"].map(lambda x: f"{x:.2f}")
            st.dataframe(show_df, use_container_width=True, hide_index=True)

            if isinstance(break_df, pd.DataFrame) and not break_df.empty:
                dates = sorted(break_df["date"].unique(), reverse=True)
                selected_day = st.selectbox("Detail day", dates, index=0, key="vb_detail_day")
                detail_df = break_df[break_df["date"] == selected_day][["symbol", "vol_today", "avg5", "ratio5"]].copy()
                detail_df = detail_df.sort_values("ratio5", ascending=False)
                st.dataframe(detail_df, use_container_width=True, hide_index=True)
            else:
                st.info("No Break AVG5 days in selected range")
        else:
            st.info("Click Run backfill now to build daily history")

        if isinstance(err_df, pd.DataFrame) and not err_df.empty:
            st.markdown("#### Backfill errors")
            st.dataframe(err_df, use_container_width=True, hide_index=True)

    with tabs[4]:
        st.subheader(t["errors_title"])
        live_err_df = st.session_state.get("vb_live_err_df")
        backfill_err_df = st.session_state.get("vb_backfill_err_df")
        frames = []
        if isinstance(live_err_df, pd.DataFrame) and not live_err_df.empty:
            frames.append(live_err_df.assign(scope="live"))
        if isinstance(backfill_err_df, pd.DataFrame) and not backfill_err_df.empty:
            frames.append(backfill_err_df.assign(scope="backfill"))
        if frames:
            err_df = pd.concat(frames, ignore_index=True)
            st.dataframe(err_df[["scope", "symbol", "error"]], use_container_width=True, hide_index=True)
        else:
            st.info("No errors")
