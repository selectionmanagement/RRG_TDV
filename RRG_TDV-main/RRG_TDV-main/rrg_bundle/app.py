from __future__ import annotations

import concurrent.futures as futures
import time
from dataclasses import dataclass
import copy
import html
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
import streamlit as st

from rrg.cache import DiskCache
import plotly.graph_objects as go

from volume_breakout import render_volume_breakout

from rrg.plot import build_rrg_figure, LabelMode, TailMode, Theme
from rrg.rrg_calc import (
    compute_rrg_for_symbol,
    compute_rrg_for_symbol_fifty_two_week_high,
    compute_rrg_for_symbol_fifty_two_week_low,
    compute_rrg_for_symbol_three_month_high,
)
from rrg.symbols import format_set_symbol, parse_symbol_list, short_symbol
from rrg.tv import TradingViewClient, resolution_from_label


Lang = str

_I18N: Dict[Lang, Dict[str, str]] = {
    "en": {
        "app_title": "Market Strength (SET100)",
        "sidebar_lang": "Language / ภาษา",
        "lang_en": "English",
        "lang_th": "ไทย",
        "tab_data": "Data",
        "tab_visual": "Visual",
        "tab_guide": "Guide",
        "tab_donation": "Honesty Box",
        "symbols": "Symbols (one per line)",
        "symbols_help": "Type ADVANC and the app will use SET:ADVANC",
        "benchmark": "Benchmark",
        "timeframe": "Timeframe",
        "timeframe_help": "Daily shows one row per trading day, Weekly aggregates per week.",
        "bars": "Bars",
        "cache_ttl": "Cache time-to-live (hours)",
        "fetch_workers": "Parallel data fetch workers",
        "fetch_workers_help": "Lower this if you hit rate limits.",
        "force_refresh": "Force refresh (ignore cache)",
        "fetch_btn": "Fetch / Refresh Data",
        "last_fetch": "Last fetch: `{time}` | Timeframe: `{tf}` | Bars: `{bars}` | Symbols: `{n}`",
        "visual_caption": "Theme locked to Vivid (dark).",
        "auto_scale": "Auto scale (recommended)",
        "axis_span": "Axis span (+/-)",
        "axis_span_help": "Fix the quadrant size around 100 when Auto scale is off.",
        "metric_sort": "Metric to sort",
        "metric_sort_help": "Speed shows recent momentum, Distance highlights benchmark drift.",
        "top_n": "Top N symbols",
        "top_n_help": "Defines how many dots plus tails are visible.",
        "min_filter": "Minimum metric filter",
        "min_filter_help": "Hide entries whose chosen metric falls below this floor.",
        "show_tails_topn": "Show tails for Top N symbols",
        "show_tails_topn_help": "Only the Top N symbols will draw lines.",
        "animate_timeline": "Animate timeline (last year)",
        "animate_help": "Play or drag through the past 12 months of data.",
        "anim_window": "Animation window (days)",
        "anim_window_help": "How many days the animation includes.",
        "frame_step": "Frame step (days)",
        "frame_step_help": "Skip this many days between frames to reduce load.",
        "main_tab_rrg": "RRG",
        "main_tab_snapshot": "Snapshot",
        "main_tab_breadth": "Market Breadth",
        "main_tab_volume": "Volume Breakout",
        "main_tab_errors": "Errors",
        "hide_settings": "Hide settings",
        "settings": "Settings",
        "model": "Model",
        "model_ema": "EMA (classic RRG)",
        "model_3m": "3 Month - High",
        "model_52wh": "52 Week - High",
        "model_52wl": "52 Week - Low",
        "rrg": "RRG",
        "download_csv": "Download CSV",
        "no_data": "Open Sidebar -> Data, then click Fetch / Refresh Data.",
        "no_symbols": "Add at least 1 symbol.",
        "fetching": "Fetching data...",
        "fetching_bench": "Fetching benchmark: {bench}",
        "fetching_syms": "Fetching {n} symbols...",
        "done": "Done.",
        "benchmark_error": "Benchmark error: {err}",
        "no_symbols_computed": "No symbols could be computed (check Errors tab).",
        "breadth_title": "Market Breadth",
        "breadth_caption": "Breadth is computed from the currently loaded symbol list (not the whole exchange). Lookback matches Animation window: last {days} days.",
        "breadth_ema_title": "Above EMA 20 / 50 / 200",
        "breadth_hl_title": "New 52-week highs vs lows (window={bars} bars)",
        "not_enough_ema": "Not enough data to compute EMA breadth.",
        "not_enough_hl": "Not enough data to compute 52-week high/low breadth.",
        "snapshot_3m": "3 Month - High (Mark Points)",
        "snapshot_52wh": "52 Week - High (Mark Points)",
        "snapshot_52wl": "52 Week - Low (Mark Points)",
        "errors_count": "Errors: {n}",
        "no_errors": "No errors.",
        "donation_title": "All referenced data in this content is access-funded by prior users.",
        "donation_scan": "Scan to donate (BNB)",
        "donation_network": "Network: {net}",
        "donation_address": "Wallet address",
        "donation_thanks": "All referenced data in this content is access-funded by prior users.",
        "donation_more": "More ways to donate",
        "donation_btc": "Bitcoin (BTC)",
        "donation_sol": "Solana (SOL)",
        "donation_xrp": "Ripple (XRP)",
        "donation_xrp_memo": "Destination tag / memo",
        "donation_usdt": "USDT (BEP20)",
        "computing_rrg": "Computing RRG...",
        "rrg_settings_top": "RRG settings are shown at the top of the main RRG tab.",
        "rrg_summary_ema": "Model: `{model}` | RS Ratio EMA length: `{ratio}` | RS Momentum EMA length: `{mom}` | Tail length: `{tail}`",
        "rrg_summary_window": "Model: `{model}` | Momentum lookback (bars): `{lookback}` | Tail length: `{tail}`",
        "rs_ratio_ema": "RS Ratio EMA length",
        "rs_mom_ema": "RS Momentum EMA length",
        "mom_lookback": "Momentum lookback (bars)",
        "tail_len": "Tail length",
        "uses_prev_3m": "Uses previous 3 full calendar months (excluding current month).",
        "uses_prev_52w": "Uses previous 52 full weeks (excluding current week).",
        "metric_above_ema20": "Above EMA20 (%)",
        "metric_above_ema50": "Above EMA50 (%)",
        "metric_above_ema200": "Above EMA200 (%)",
        "metric_new_highs": "New highs (count)",
        "metric_new_lows": "New lows (count)",
        "metric_highs_minus_lows": "Highs - Lows",
        "visual_quick_guide": "### Quick visual guide\n- Keep **Auto scale** on for a balanced quadrant view.\n- Pick **Metric** for sorting: Speed tracks momentum, Distance highlights benchmark drift.\n- **Top N symbols** and **Minimum metric filter** control how many symbols stay on screen.\n",
        "tails_heading": "#### Tails",
        "animation_heading": "#### Animation",
        "rrg_caption": "Benchmark: `{bench}` | Timeframe: `{tf}` | As of: `{asof}` | Loaded: `{n}`",
        "no_symbols_filter": "No symbols matched the filter (Top N and min threshold).",
        "rrg_dr_caption": "DR (Depository Receipt - Foreign Securities) only | Benchmark: `{bench}` | Timeframe: `{tf}` | As of: `{asof}` | Loaded: `{n}`",
        "rrg_dr_title": "RRG - Depository Receipts (Foreign Securities)",
        "no_dr_symbols": "No DR symbols available in the current dataset.",
        "prerelease_heading": "### Pre-release link",
        "prerelease_request": "Request pre-release access",
        "prerelease_ready": "Ready — click to open release notes",
        "prerelease_modal_title": "Confirm pre-release access",
        "prerelease_modal_warning": "This link will be publicly visible. Please confirm before proceeding.",
        "prerelease_modal_body": "If you are ready, click \"Confirm\" and the app will show the public link.",
        "confirm": "Confirm",
        "cancel": "Cancel",
    },
    "th": {
        "app_title": "Market Strength (SET100)",
        "sidebar_lang": "Language / ภาษา",
        "lang_en": "English",
        "lang_th": "ไทย",
        "tab_data": "ข้อมูล",
        "tab_visual": "การแสดงผล",
        "tab_guide": "คู่มือ",
        "tab_donation": "สนับสนุน",
        "symbols": "รายชื่อหุ้น (บรรทัดละ 1 ตัว)",
        "symbols_help": "พิมพ์ ADVANC ระบบจะใช้ SET:ADVANC ให้อัตโนมัติ",
        "benchmark": "Benchmark",
        "timeframe": "กรอบเวลา",
        "timeframe_help": "Daily = รายวัน, Weekly = สรุปรายสัปดาห์",
        "bars": "จำนวนแท่ง (Bars)",
        "cache_ttl": "อายุแคช (ชั่วโมง)",
        "fetch_workers": "จำนวนงานดึงข้อมูลพร้อมกัน",
        "fetch_workers_help": "ลดค่านี้หากเจอ rate limit",
        "force_refresh": "บังคับดึงใหม่ (ไม่ใช้แคช)",
        "fetch_btn": "ดึง/รีเฟรชข้อมูล",
        "last_fetch": "ดึงล่าสุด: `{time}` | กรอบเวลา: `{tf}` | Bars: `{bars}` | จำนวนหุ้น: `{n}`",
        "visual_caption": "ธีมล็อกไว้ที่ Vivid (dark)",
        "auto_scale": "ปรับสเกลอัตโนมัติ (แนะนำ)",
        "axis_span": "ช่วงแกน (+/-)",
        "axis_span_help": "กำหนดขนาดกราฟรอบ 100 เมื่อปิด Auto scale",
        "metric_sort": "Metric สำหรับจัดอันดับ",
        "metric_sort_help": "Speed = ความเร็วการเปลี่ยนแปลง, Distance = ระยะห่างจาก benchmark",
        "top_n": "จำนวน Top N",
        "top_n_help": "กำหนดจำนวนจุด/หางที่แสดง",
        "min_filter": "ตัวกรองขั้นต่ำ (Metric)",
        "min_filter_help": "ซ่อนรายการที่ metric ต่ำกว่าค่านี้",
        "show_tails_topn": "แสดงหางเฉพาะ Top N",
        "show_tails_topn_help": "จะแสดงเส้นหางเฉพาะหุ้น Top N",
        "animate_timeline": "เล่นอนิเมชันย้อนหลัง (1 ปี)",
        "animate_help": "กดเล่นหรือเลื่อนดูย้อนหลัง 12 เดือน",
        "anim_window": "ช่วงอนิเมชัน (วัน)",
        "anim_window_help": "กำหนดจำนวนวันย้อนหลังในอนิเมชัน",
        "frame_step": "ข้ามเฟรม (วัน)",
        "frame_step_help": "ข้ามวันระหว่างเฟรมเพื่อลดภาระการคำนวณ",
        "main_tab_rrg": "RRG",
        "main_tab_snapshot": "สรุปตาราง",
        "main_tab_breadth": "Market Breadth",
        "main_tab_volume": "Volume Breakout",
        "main_tab_errors": "ข้อผิดพลาด",
        "hide_settings": "ซ่อนการตั้งค่า",
        "settings": "การตั้งค่า",
        "model": "โมเดล",
        "model_ema": "EMA (RRG แบบดั้งเดิม)",
        "model_3m": "3 เดือน - จุดสูงสุด",
        "model_52wh": "52 สัปดาห์ - จุดสูงสุด",
        "model_52wl": "52 สัปดาห์ - จุดต่ำสุด",
        "rrg": "RRG",
        "download_csv": "ดาวน์โหลด CSV",
        "no_data": "ไปที่ Sidebar -> ข้อมูล แล้วกด ดึง/รีเฟรชข้อมูล",
        "no_symbols": "กรุณาใส่หุ้นอย่างน้อย 1 ตัว",
        "fetching": "กำลังดึงข้อมูล...",
        "fetching_bench": "กำลังดึง benchmark: {bench}",
        "fetching_syms": "กำลังดึง {n} หุ้น...",
        "done": "เสร็จแล้ว",
        "benchmark_error": "เกิดข้อผิดพลาดที่ benchmark: {err}",
        "no_symbols_computed": "คำนวณไม่ได้ (ดูแท็บ Errors)",
        "breadth_title": "ความกว้างตลาด",
        "breadth_caption": "คำนวณจากรายชื่อหุ้นที่โหลดอยู่ (ไม่ใช่ทั้งตลาด) และใช้ช่วงย้อนหลังเท่ากับ Animation window: {days} วัน",
        "breadth_ema_title": "สัดส่วนหุ้นเหนือ EMA 20 / 50 / 200",
        "breadth_hl_title": "จำนวนทำ New 52-week high vs low (window={bars} แท่ง)",
        "not_enough_ema": "ข้อมูลไม่พอสำหรับคำนวณ EMA breadth",
        "not_enough_hl": "ข้อมูลไม่พอสำหรับคำนวณ 52-week high/low breadth",
        "snapshot_3m": "3 เดือน - จุดสูงสุด (Mark Points)",
        "snapshot_52wh": "52 สัปดาห์ - จุดสูงสุด (Mark Points)",
        "snapshot_52wl": "52 สัปดาห์ - จุดต่ำสุด (Mark Points)",
        "errors_count": "ข้อผิดพลาด: {n}",
        "no_errors": "ไม่มีข้อผิดพลาด",
        "donation_title": "ข้อมูลทั้งหมดที่ใช้อ้างอิงในเนื้อหานี้ เป็นข้อมูลที่ได้รับการสนับสนุนค่าใช้จ่ายในการเข้าถึงจากผู้ใช้ก่อนหน้า",
        "donation_scan": "สแกนเพื่อสนับสนุน (BNB)",
        "donation_network": "เครือข่าย: {net}",
        "donation_address": "ที่อยู่กระเป๋า",
        "donation_thanks": "ข้อมูลทั้งหมดที่ใช้อ้างอิงในเนื้อหานี้ เป็นข้อมูลที่ได้รับการสนับสนุนค่าใช้จ่ายในการเข้าถึงจากผู้ใช้ก่อนหน้า",
        "donation_more": "ช่องทางสนับสนุนเพิ่มเติม",
        "donation_btc": "บิตคอยน์ (BTC)",
        "donation_sol": "โซลานา (SOL)",
        "donation_xrp": "ริปเปิล (XRP)",
        "donation_xrp_memo": "Destination tag / memo",
        "donation_usdt": "USDT (BEP20)",
        "computing_rrg": "กำลังคำนวณ RRG...",
        "rrg_settings_top": "การตั้งค่า RRG อยู่ด้านบนสุดของแท็บ RRG",
        "rrg_summary_ema": "โมเดล: `{model}` | RS Ratio EMA: `{ratio}` | RS Momentum EMA: `{mom}` | ความยาวหาง: `{tail}`",
        "rrg_summary_window": "โมเดล: `{model}` | ช่วงดู Momentum (แท่ง): `{lookback}` | ความยาวหาง: `{tail}`",
        "rs_ratio_ema": "ความยาว EMA (RS Ratio)",
        "rs_mom_ema": "ความยาว EMA (RS Momentum)",
        "mom_lookback": "ช่วงดู Momentum (แท่ง)",
        "tail_len": "ความยาวหาง",
        "uses_prev_3m": "ใช้ 3 เดือนเต็มก่อนหน้า (ไม่นับเดือนปัจจุบัน)",
        "uses_prev_52w": "ใช้ 52 สัปดาห์เต็มก่อนหน้า (ไม่นับสัปดาห์ปัจจุบัน)",
        "metric_above_ema20": "เหนือ EMA20 (%)",
        "metric_above_ema50": "เหนือ EMA50 (%)",
        "metric_above_ema200": "เหนือ EMA200 (%)",
        "metric_new_highs": "ทำจุดสูงสุดใหม่ (ตัว)",
        "metric_new_lows": "ทำจุดต่ำสุดใหม่ (ตัว)",
        "metric_highs_minus_lows": "สูง-ต่ำ (สุทธิ)",
        "visual_quick_guide": "### คำแนะนำการแสดงผลแบบย่อ\n- เปิด **ปรับสเกลอัตโนมัติ** เพื่อให้มุมมอง 4 Quadrant สมดุล\n- เลือก **Metric สำหรับจัดอันดับ**: Speed = โมเมนตัมล่าสุด, Distance = ระยะห่างจาก benchmark\n- **Top N** และ **ตัวกรองขั้นต่ำ (Metric)** ช่วยควบคุมจำนวนหุ้นที่แสดง\n",
        "tails_heading": "#### หาง (Tails)",
        "animation_heading": "#### อนิเมชัน",
        "rrg_caption": "Benchmark: `{bench}` | กรอบเวลา: `{tf}` | ณ วันที่: `{asof}` | แสดง: `{n}`",
        "no_symbols_filter": "ไม่มีหุ้นที่ผ่านเงื่อนไขตัวกรอง (Top N และค่าขั้นต่ำ)",
        "rrg_dr_caption": "DR (Depository Receipt - หลักทรัพย์ต่างประเทศ) เท่านั้น | Benchmark: `{bench}` | กรอบเวลา: `{tf}` | ณ วันที่: `{asof}` | แสดง: `{n}`",
        "rrg_dr_title": "RRG - Depository Receipts (หลักทรัพย์ต่างประเทศ)",
        "no_dr_symbols": "ไม่มีหุ้น DR ในชุดข้อมูลปัจจุบัน",
        "prerelease_heading": "### ลิงก์ก่อนเผยแพร่",
        "prerelease_request": "ขอเข้าเว็บก่อนเผยแพร่",
        "prerelease_ready": "พร้อมเผยแพร่แล้ว — คลิกเพื่อไปยังหน้า release note",
        "prerelease_modal_title": "เตือนก่อนเผยแพร่",
        "prerelease_modal_warning": "ลิงก์นี้จะเผยแพร่ให้คนทั่วไปเห็น คุณต้องยืนยันแล้วเท่านั้นจึงจะเข้าได้",
        "prerelease_modal_body": "หากคุณพร้อมเผยแพร่ กด \"ยืนยัน\" แล้วระบบจะปล่อยลิงก์จริงให้",
        "confirm": "ยืนยัน",
        "cancel": "ยกเลิก",
    },
}


def _t(key: str, *, lang: str, **kwargs: object) -> str:
    table = _I18N.get(lang) or _I18N["en"]
    template = table.get(key) or _I18N["en"].get(key) or key
    try:
        return template.format(**kwargs)
    except Exception:
        return template


def _inject_dashboard_css() -> None:
    st.markdown(
        """
        <style>
        [data-testid="stApp"] {
            font-family: "IBM Plex Sans", "Segoe UI", "Tahoma", sans-serif;
        }
        [data-testid="stAppViewContainer"] {
            background: linear-gradient(180deg, #090c12 0%, #0d1118 40%, #111827 100%);
        }
        [data-testid="stSidebar"] {
            background: #0b1220;
            border-right: 1px solid #334155;
        }
        [data-testid="stSidebarContent"] {
            padding-top: 0.4rem;
        }
        [data-testid="stSidebar"] * {
            color: #e5e7eb !important;
        }
        [data-testid="stSidebar"] [data-baseweb="tab-list"] button {
            background: rgba(15, 23, 42, 0.86);
            border-radius: 10px 10px 0 0;
            border: 1px solid #334155;
            width: auto !important;
            min-width: max-content !important;
            flex: 0 0 auto !important;
            padding: 0.35rem 0.62rem !important;
            white-space: nowrap !important;
        }
        [data-testid="stSidebar"] [data-baseweb="tab-list"] {
            gap: 0.35rem;
            flex-wrap: wrap;
        }
        [data-testid="stSidebar"] [data-baseweb="tab-highlight"] {
            background: #64748b;
        }
        [data-baseweb="tab-list"] {
            gap: 0.32rem;
            flex-wrap: wrap;
        }
        [data-baseweb="tab-list"] button[role="tab"] {
            width: auto !important;
            min-width: max-content !important;
            flex: 0 0 auto !important;
            white-space: nowrap !important;
            padding: 0.35rem 0.66rem !important;
        }
        [data-testid="stAppViewContainer"] .stMarkdown,
        [data-testid="stAppViewContainer"] p,
        [data-testid="stAppViewContainer"] label,
        [data-testid="stAppViewContainer"] h1,
        [data-testid="stAppViewContainer"] h2,
        [data-testid="stAppViewContainer"] h3,
        [data-testid="stAppViewContainer"] h4,
        [data-testid="stAppViewContainer"] h5,
        [data-testid="stAppViewContainer"] h6 {
            color: #e5e7eb;
        }
        .block-container {
            max-width: 1440px;
            padding-top: 1rem;
            padding-bottom: 2rem;
        }
        .dash-hero {
            border: 1px solid #334155;
            border-radius: 14px;
            padding: 1rem 1.1rem;
            background: linear-gradient(135deg, #0f172a 0%, #111827 100%);
            margin-bottom: 0.9rem;
        }
        .dash-hero h1 {
            margin: 0;
            font-size: 1.55rem;
            font-weight: 700;
            color: #f8fafc;
            letter-spacing: 0.01em;
        }
        .dash-hero p {
            margin: 0.25rem 0 0 0;
            color: #cbd5e1;
            font-size: 0.92rem;
        }
        .status-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
            gap: 0.6rem;
            margin: 0.45rem 0 1rem 0;
        }
        .status-card {
            border: 1px solid #334155;
            border-radius: 12px;
            background: #0f172a;
            padding: 0.55rem 0.7rem;
        }
        .status-label {
            font-size: 0.72rem;
            letter-spacing: 0.03em;
            text-transform: uppercase;
            color: #94a3b8;
            margin-bottom: 0.15rem;
            font-weight: 600;
        }
        .status-value {
            font-size: 0.98rem;
            font-weight: 700;
            color: #e5e7eb;
            line-height: 1.15rem;
        }
        .section-head {
            border-left: 4px solid #64748b;
            padding-left: 0.6rem;
            margin: 0.1rem 0 0.7rem 0;
        }
        .section-head h3 {
            margin: 0;
            font-size: 1.05rem;
            color: #f3f4f6;
            font-weight: 700;
        }
        .section-head p {
            margin: 0.18rem 0 0 0;
            color: #cbd5e1;
            font-size: 0.86rem;
        }
        [data-testid="stMetric"] {
            border: 1px solid #334155;
            border-radius: 12px;
            background: #0f172a;
            padding: 0.35rem 0.5rem;
        }
        [data-testid="stMetricLabel"],
        [data-testid="stMetricValue"] {
            color: #e5e7eb !important;
        }
        [data-testid="stCaptionContainer"] {
            color: #94a3b8 !important;
        }
        @media (max-width: 980px) {
            .block-container {
                padding-top: 0.6rem;
                padding-bottom: 1.1rem;
            }
            .dash-hero h1 {
                font-size: 1.24rem;
            }
            .status-grid {
                grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_app_hero(*, title: str, subtitle: str) -> None:
    safe_title = html.escape(title)
    safe_subtitle = html.escape(subtitle)
    st.markdown(
        (
            '<div class="dash-hero">'
            f"<h1>{safe_title}</h1>"
            f"<p>{safe_subtitle}</p>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def _render_status_cards(items: List[Tuple[str, str]]) -> None:
    blocks = []
    for label, value in items:
        blocks.append(
            (
                '<div class="status-card">'
                f'<div class="status-label">{html.escape(str(label))}</div>'
                f'<div class="status-value">{html.escape(str(value))}</div>'
                "</div>"
            )
        )
    st.markdown(f'<div class="status-grid">{"".join(blocks)}</div>', unsafe_allow_html=True)


def _render_section_head(title: str, subtitle: Optional[str] = None) -> None:
    subtitle_html = ""
    if subtitle:
        subtitle_html = f"<p>{html.escape(subtitle)}</p>"
    st.markdown(
        f'<div class="section-head"><h3>{html.escape(title)}</h3>{subtitle_html}</div>',
        unsafe_allow_html=True,
    )


@dataclass(frozen=True)
class AppConfig:
    default_benchmark: str = "SET:SET100"
    default_bars: int = 90
    default_cache_ttl_hours: int = 6
    default_workers: int = 4
    default_symbols: str = "\n".join(
        [
            "DELTA",
            "ADVANC",
            "PTT",
            "AOT",
            "GULF",
            "KBANK",
            "SCB",
            "PTTEP",
            "KTB",
            "CPALL",
            "TRUE",
            "BDMS",
            "BBL",
            "CPN",
            "THAI",
            "SCC",
            "TTB",
            "BAY",
            "CPAXT",
            "OR",
            "CPF",
            "BH",
            "MINT",
            "CRC",
            "TLI",
            "GPSC",
            "PTTGC",
            "IVL",
            "TISCO",
            "HMPRO",
            "BEM",
            "TOP",
            "MTC",
            "KTC",
            "SCGP",
            "RATCH",
            "AWC",
            "TFMAMA",
            "BJC",
            "EGCO",
            "TIDLOR",
            "KKP",
            "TCAP",
            "MRDIYT",
            "COM7",
            "CCET",
            "TU",
            "BANPU",
            "WHA",
            "OSP",
            "SAWAD",
            "ITC",
            "LH",
            "SCCC",
            "CBG",
            "CENTEL",
            "BTS",
            "BPP",
            "BGRIM",
            "SPI",
            "BCP",
            "TTW",
            "GLOBAL",
            "BTG",
            "JTS",
            "BLA",
            "BKIH",
            "SPALI",
            "BA",
            "MEGA",
            "TOA",
            "TFG",
            "AP",
            "BCH",
            "AEONTS",
            "SPRC",
            "MBK",
            "KCE",
            "STGT",
            "BAM",
            "VGI",
            "SIRI",
            "RCL",
            "TASCO",
            "RAM",
            "BCPG",
            "PB",
            "IRPC",
            "CREDIT",
            "CKP",
            "EA",
            "CK",
            "PLANB",
            "TVO",
            "AURA",
            "SPC",
            "STA",
            "VIBHA",
            "LHFG",
            "AMATA",
            "MSFT80",
            "MSFT19",
            "MSFT01",
            "LLY80",
            "GOLDUS19",
            "GOLDUS80",
            "NOVOB80",
            "NOVO80",
            "TENCENT19",
            "TENCENT80",
            "STAR80",
            "NDX01",
            "NDX80",
            "CNTECH80",
        ]
    )

    cache_dir: str = ".cache"


def is_dr_symbol(symbol: str) -> bool:
    """
    Check if a symbol is a DR (Depository Receipt).
    DR symbols typically end with numbers (e.g., AAPL80, TSLA01, NVDA80).
    They represent foreign securities traded on SET.
    """
    s = short_symbol(symbol)  # Remove SET: prefix
    if not s:
        return False
    # DR symbols typically end with 2 digits (e.g., AAPL80, TSLA01)
    # and often contain letters followed by numbers
    import re
    # Pattern: letters followed by 2 digits at the end
    pattern = r'^[A-Z]+\d{2}$'
    return bool(re.match(pattern, s))


PUBLIC_RELEASE_URL = "https://example.com/release/rrg-dashboard"

_SYMBOL_FALLBACKS: Dict[str, List[str]] = {
    "SET:NOVO80": ["SET:NOVOB80"],
    "SET:GOLDUS80": ["SET:GOLDUS19"],
    "SET:NDX80": ["SET:NDX01"],
    "SET:STAR80": ["SET:ASML01"],
    "SET:CNTECH80": ["SET:CNTECH01"],
}
_UNAVAILABLE_TV_SYMBOLS: set[str] = set()

def _fetch_one(
    *,
    client: TradingViewClient,
    cache: DiskCache,
    symbol: str,
    resolution: str,
    bars: int,
    ttl_seconds: int,
    refresh: bool,
) -> Tuple[str, Optional[pd.DataFrame], Optional[str]]:
    symbol = (symbol or "").strip().upper()
    if symbol in _UNAVAILABLE_TV_SYMBOLS:
        return symbol, None, "Symbol is unavailable on TradingView feed"

    cache_key = f"{symbol}|{resolution}|{bars}"
    if not refresh:
        cached = cache.get_df(cache_key, ttl_seconds=ttl_seconds)
        if cached is not None:
            return symbol, cached, None

    retry_attempts = 3
    last_err: Optional[str] = None
    candidates = [symbol, *[s for s in _SYMBOL_FALLBACKS.get(symbol, []) if s and s != symbol]]
    for candidate_symbol in candidates:
        for attempt in range(retry_attempts):
            try:
                df = client.get_ohlcv(symbol=candidate_symbol, resolution=resolution, bars=bars)
                cache.set_df(cache_key, df)
                return symbol, df, None
            except Exception as e:  # noqa: BLE001
                last_err = str(e)
                err_text = last_err.lower()
                retryable = any(k in err_text for k in ["timeout", "timed out", "tempor", "connection"])
                if attempt < (retry_attempts - 1) and retryable:
                    time.sleep(0.7 * (attempt + 1))
                    continue
                break
    return symbol, None, last_err or "Unknown error"


def _align_close(df: pd.DataFrame) -> pd.Series:
    if "close" not in df.columns:
        raise ValueError("Missing 'close' column")
    close = df["close"].copy()
    idx = pd.to_datetime(close.index, errors="coerce")
    valid = ~idx.isna()
    close = close[valid]
    idx = idx[valid]
    if getattr(idx, "tz", None) is not None:
        idx = idx.tz_convert("UTC").tz_localize(None)
    idx = idx.normalize()
    close = pd.Series(close.to_numpy(), index=idx, name="close")
    close = close.groupby(level=0).last().sort_index()
    return close


def _compute_rrg_bundle(
    *,
    closes: Dict[str, pd.Series],
    bench_close: pd.Series,
    ohlcv: Dict[str, pd.DataFrame],
    bench_ohlcv: Optional[pd.DataFrame],
    model_id: str,
    ratio_len: int,
    mom_len: int,
    tail_len: int,
) -> Tuple[
    pd.DataFrame,
    Dict[str, pd.DataFrame],
    Dict[str, str],
    Dict[str, pd.DataFrame],
]:
    rows: List[dict] = []
    tails: Dict[str, pd.DataFrame] = {}
    errors: Dict[str, str] = {}
    symbol_rrgs: Dict[str, pd.DataFrame] = {}

    for sym, close in closes.items():
        try:
            merged = pd.concat({"sym": close, "bench": bench_close}, axis=1, join="inner").dropna()
            if merged.empty:
                errors[sym] = "Not enough aligned bars"
                continue

            if model_id in {"3m_high", "52w_high", "52w_low"}:
                df_sym = ohlcv.get(sym)
                need_col = "low" if model_id == "52w_low" else "high"
                if df_sym is None or df_sym.empty or need_col not in df_sym.columns:
                    errors[sym] = f"Missing OHLCV {need_col} data"
                    continue
                if bench_ohlcv is None or bench_ohlcv.empty or need_col not in bench_ohlcv.columns:
                    errors[sym] = f"Missing benchmark OHLCV {need_col} data"
                    continue
                sym_series = df_sym[need_col]
                bench_series = bench_ohlcv[need_col]
                if model_id == "3m_high":
                    rrg_df = compute_rrg_for_symbol_three_month_high(
                        close_symbol=merged["sym"],
                        high_symbol=sym_series,
                        close_benchmark=merged["bench"],
                        high_benchmark=bench_series,
                        mom_lookback=int(mom_len),
                    )
                elif model_id == "52w_high":
                    rrg_df = compute_rrg_for_symbol_fifty_two_week_high(
                        close_symbol=merged["sym"],
                        high_symbol=sym_series,
                        close_benchmark=merged["bench"],
                        high_benchmark=bench_series,
                        mom_lookback=int(mom_len),
                    )
                else:
                    rrg_df = compute_rrg_for_symbol_fifty_two_week_low(
                        close_symbol=merged["sym"],
                        low_symbol=sym_series,
                        close_benchmark=merged["bench"],
                        low_benchmark=bench_series,
                        mom_lookback=int(mom_len),
                    )
                if rrg_df.empty:
                    errors[sym] = "Not enough high-window data"
                    continue
            else:
                aligned_bars = int(len(merged))
                if aligned_bars < 25:
                    errors[sym] = "Not enough aligned bars"
                    continue
                max_span = max(5, aligned_bars - 5)
                ratio_eff = min(max(5, int(ratio_len)), max_span)
                mom_eff = min(max(5, int(mom_len)), max_span)
                rrg_df = compute_rrg_for_symbol(
                    close_symbol=merged["sym"],
                    close_benchmark=merged["bench"],
                    ratio_len=int(ratio_eff),
                    mom_len=int(mom_eff),
                )
                if rrg_df.empty:
                    errors[sym] = "Not enough aligned bars"
                    continue
            latest = rrg_df.iloc[-1]
            rows.append(
                {
                    "symbol": sym,
                    "label": short_symbol(sym),
                    "rs_ratio": float(latest["rs_ratio"]),
                    "rs_mom": float(latest["rs_mom"]),
                    "quadrant": str(latest["quadrant"]),
                    "distance": float(latest["distance"]),
                    "speed": float(latest.get("speed", float("nan"))),
                    "angle_deg": float(latest["angle_deg"]),
                    "date": rrg_df.index[-1].date().isoformat(),
                }
            )
            tails[sym] = rrg_df[["rs_ratio", "rs_mom"]].tail(int(tail_len))
            symbol_rrgs[sym] = rrg_df
        except Exception as e:  # noqa: BLE001
            errors[sym] = str(e)

    if not rows:
        return pd.DataFrame(), {}, errors, {}

    table = pd.DataFrame(rows).set_index("symbol")
    table = table.sort_values(["quadrant", "distance"], ascending=[True, False])

    return table, tails, errors, symbol_rrgs


def _format_time(ts: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


def _safe_list(x: Optional[Iterable[str]]) -> List[str]:
    return [str(s) for s in (x or [])]

def _to_naive_timestamp(ts: pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(ts)
    if ts.tz is not None:
        try:
            ts = ts.tz_convert("UTC").tz_localize(None)
        except TypeError:
            ts = ts.tz_localize(None)
    return ts


def _three_month_high_mark(
    df: pd.DataFrame,
    *,
    ref_date: pd.Timestamp,
) -> Tuple[Optional[float], Optional[pd.Timestamp]]:
    """
    3 Month - High model:
    - Use the previous 3 *full* calendar months (excluding the current month of ref_date).
    - Find the highest OHLCV `high` within that window.
    - Return (high_value, date_of_high). If multiple, pick the latest date.
    """
    if df is None or df.empty or "high" not in df.columns:
        return None, None

    idx = pd.to_datetime(df.index)
    idx_naive = idx.tz_localize(None) if getattr(idx, "tz", None) is not None else idx
    ref = _to_naive_timestamp(pd.Timestamp(ref_date))
    start_current_month = ref.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    window_start = start_current_month - pd.DateOffset(months=3)
    window_end = start_current_month

    high_series = pd.Series(df["high"].to_numpy(), index=idx_naive).astype(float).dropna()
    high_series = high_series[(high_series.index >= window_start) & (high_series.index < window_end)]
    if high_series.empty:
        return None, None

    max_high = float(high_series.max())
    max_date = high_series[high_series == max_high].index.max()
    return max_high, pd.Timestamp(max_date)


def _week_start_naive(ts: pd.Timestamp) -> pd.Timestamp:
    ts = _to_naive_timestamp(pd.Timestamp(ts))
    ts = ts.replace(hour=0, minute=0, second=0, microsecond=0)
    return ts - pd.Timedelta(days=int(ts.weekday()))


def _fifty_two_week_high_mark(
    df: pd.DataFrame,
    *,
    ref_date: pd.Timestamp,
) -> Tuple[Optional[float], Optional[pd.Timestamp]]:
    """
    52 Week - High model:
    - Use the previous 52 *full* weeks (excluding the current week of ref_date).
    - Find the highest OHLCV `high` within that window.
    - Return (high_value, date_of_high). If multiple, pick the latest date.
    """
    if df is None or df.empty or "high" not in df.columns:
        return None, None

    idx = pd.to_datetime(df.index)
    idx_naive = idx.tz_localize(None) if getattr(idx, "tz", None) is not None else idx
    ref = _to_naive_timestamp(pd.Timestamp(ref_date))
    end_week = _week_start_naive(ref)
    window_start = end_week - pd.Timedelta(weeks=52)
    window_end = end_week

    high_series = pd.Series(df["high"].to_numpy(), index=idx_naive).astype(float).dropna()
    high_series = high_series[(high_series.index >= window_start) & (high_series.index < window_end)]
    if high_series.empty:
        return None, None

    max_high = float(high_series.max())
    max_date = high_series[high_series == max_high].index.max()
    return max_high, pd.Timestamp(max_date)


def _fifty_two_week_low_mark(
    df: pd.DataFrame,
    *,
    ref_date: pd.Timestamp,
) -> Tuple[Optional[float], Optional[pd.Timestamp]]:
    """
    52 Week - Low model:
    - Use the previous 52 *full* weeks (excluding the current week of ref_date).
    - Find the lowest OHLCV `low` within that window.
    - Return (low_value, date_of_low). If multiple, pick the latest date.
    """
    if df is None or df.empty or "low" not in df.columns:
        return None, None

    idx = pd.to_datetime(df.index)
    idx_naive = idx.tz_localize(None) if getattr(idx, "tz", None) is not None else idx
    ref = _to_naive_timestamp(pd.Timestamp(ref_date))
    end_week = _week_start_naive(ref)
    window_start = end_week - pd.Timedelta(weeks=52)
    window_end = end_week

    low_series = pd.Series(df["low"].to_numpy(), index=idx_naive).astype(float).dropna()
    low_series = low_series[(low_series.index >= window_start) & (low_series.index < window_end)]
    if low_series.empty:
        return None, None

    min_low = float(low_series.min())
    min_date = low_series[low_series == min_low].index.max()
    return min_low, pd.Timestamp(min_date)


def _breadth_window_bars(tf_label: str) -> int:
    # Approximate 52 weeks: weekly bars = 52, daily bars ~ 252 trading days
    return 52 if (tf_label or "").strip().lower() == "weekly" else 252


def _compute_breadth_above_ema(
    *,
    closes: Dict[str, pd.Series],
    spans: Iterable[int],
) -> pd.DataFrame:
    spans = [int(s) for s in spans]
    if not spans or not closes:
        return pd.DataFrame()

    out = {}
    for span in spans:
        bool_frames = []
        for sym, close in closes.items():
            if close is None or close.empty:
                continue
            s = close.astype(float).sort_index()
            ema = s.ewm(span=int(span), adjust=False).mean()
            above = (s > ema).astype(float)  # 1.0/0.0
            above.name = sym
            bool_frames.append(above)
        if not bool_frames:
            continue
        df = pd.concat(bool_frames, axis=1)
        denom = df.notna().sum(axis=1).astype(float)
        numer = df.sum(axis=1, skipna=True).astype(float)
        pct = (numer / denom) * 100.0
        out[f"ema_{span}"] = pct

    return pd.DataFrame(out).sort_index()


def _compute_breadth_new_high_low(
    *,
    ohlcv: Dict[str, pd.DataFrame],
    window_bars: int,
) -> pd.DataFrame:
    window_bars = max(2, int(window_bars))
    highs = []
    lows = []
    for sym, df in ohlcv.items():
        if df is None or df.empty or "high" not in df.columns or "low" not in df.columns:
            continue
        idx = pd.to_datetime(df.index)
        idx = idx.tz_localize(None) if getattr(idx, "tz", None) is not None else idx
        high = pd.Series(df["high"].to_numpy(), index=idx).astype(float).sort_index()
        low = pd.Series(df["low"].to_numpy(), index=idx).astype(float).sort_index()

        prev_max = high.rolling(window_bars, min_periods=window_bars).max().shift(1)
        prev_min = low.rolling(window_bars, min_periods=window_bars).min().shift(1)
        new_high = (high > prev_max).where(prev_max.notna()).astype(float)
        new_low = (low < prev_min).where(prev_min.notna()).astype(float)
        new_high.name = sym
        new_low.name = sym
        highs.append(new_high)
        lows.append(new_low)

    if not highs or not lows:
        return pd.DataFrame()

    df_high = pd.concat(highs, axis=1)
    df_low = pd.concat(lows, axis=1)
    denom = df_high.notna().sum(axis=1).astype(float)
    new_high_count = df_high.sum(axis=1, skipna=True).astype(float)
    new_low_count = df_low.sum(axis=1, skipna=True).astype(float)

    return (
        pd.DataFrame(
            {
                "new_high_count": new_high_count,
                "new_low_count": new_low_count,
                "new_high_minus_low": new_high_count - new_low_count,
                "new_high_pct": (new_high_count / denom) * 100.0,
                "new_low_pct": (new_low_count / denom) * 100.0,
            }
        )
        .sort_index()
        .replace([float("inf"), float("-inf")], pd.NA)
    )


def _compute_breadth_symbol_flags(
    *,
    closes: Dict[str, pd.Series],
    ohlcv: Dict[str, pd.DataFrame],
    asof: pd.Timestamp,
    window_bars: int,
) -> pd.DataFrame:
    asof_ts = pd.Timestamp(asof)
    window_bars = max(2, int(window_bars))

    rows: List[dict] = []
    for sym, close in closes.items():
        if close is None or close.empty:
            continue

        s_close = close.astype(float).copy()
        s_close.index = pd.to_datetime(s_close.index, errors="coerce")
        s_close = s_close[~s_close.index.isna()]
        s_close = s_close[~s_close.index.duplicated(keep="last")].sort_index()
        if s_close.empty:
            continue

        try:
            cutoff = _cutoff_for_index(asof_ts, s_close.index)
            close_up_to = s_close.loc[:cutoff]
        except Exception:
            close_up_to = s_close
        if close_up_to.empty:
            close_up_to = s_close

        last_idx = close_up_to.index[-1]
        last_close = float(close_up_to.iloc[-1])
        last_dt = _to_naive_timestamp(pd.Timestamp(last_idx))

        ema20 = s_close.ewm(span=20, adjust=False).mean()
        ema50 = s_close.ewm(span=50, adjust=False).mean()
        ema200 = s_close.ewm(span=200, adjust=False).mean()
        ema_20 = float(ema20.loc[last_idx])
        ema_50 = float(ema50.loc[last_idx])
        ema_200 = float(ema200.loc[last_idx])
        ema_200w: object = pd.NA
        try:
            weekly_close = close_up_to.resample("W-FRI").last().dropna()
            if not weekly_close.empty:
                ema200w = weekly_close.ewm(span=200, adjust=False).mean()
                ema_200w = float(ema200w.iloc[-1])
        except Exception:
            ema_200w = pd.NA

        above_ema20 = bool(last_close > ema_20) if pd.notna(ema_20) else False
        above_ema50 = bool(last_close > ema_50) if pd.notna(ema_50) else False
        above_ema200 = bool(last_close > ema_200) if pd.notna(ema_200) else False

        df = ohlcv.get(sym)
        new_high_flag: object = pd.NA
        new_low_flag: object = pd.NA
        high_last: object = pd.NA
        low_last: object = pd.NA
        high_date: object = pd.NA
        low_date: object = pd.NA
        prev_high_max: object = pd.NA
        prev_low_min: object = pd.NA
        if df is not None and not df.empty and "high" in df.columns and "low" in df.columns:
            idx = pd.to_datetime(df.index)
            idx = idx.tz_localize(None) if getattr(idx, "tz", None) is not None else idx
            high = pd.Series(df["high"].to_numpy(), index=idx).astype(float).sort_index()
            low = pd.Series(df["low"].to_numpy(), index=idx).astype(float).sort_index()

            high = high.loc[:_to_naive_timestamp(asof_ts)]
            low = low.loc[:_to_naive_timestamp(asof_ts)]
            if not high.empty and not low.empty:
                prev_max = high.rolling(window_bars, min_periods=window_bars).max().shift(1)
                prev_min = low.rolling(window_bars, min_periods=window_bars).min().shift(1)
                h_last = float(high.iloc[-1])
                l_last = float(low.iloc[-1])
                pmax = prev_max.iloc[-1]
                pmin = prev_min.iloc[-1]

                high_last = h_last
                low_last = l_last
                high_date = _to_naive_timestamp(pd.Timestamp(high.index[-1])).date().isoformat()
                low_date = _to_naive_timestamp(pd.Timestamp(low.index[-1])).date().isoformat()
                prev_high_max = pd.NA if pd.isna(pmax) else float(pmax)
                prev_low_min = pd.NA if pd.isna(pmin) else float(pmin)

                new_high_flag = pd.NA if pd.isna(pmax) else bool(h_last > float(pmax))
                new_low_flag = pd.NA if pd.isna(pmin) else bool(l_last < float(pmin))

        def _pct_over(ref: object) -> object:
            try:
                r = float(ref)  # type: ignore[arg-type]
            except Exception:
                return pd.NA
            if not pd.notna(r) or r == 0.0:
                return pd.NA
            return (last_close / r - 1.0) * 100.0

        def _pct_break(v: object, ref: object) -> object:
            try:
                vv = float(v)  # type: ignore[arg-type]
                rr = float(ref)  # type: ignore[arg-type]
            except Exception:
                return pd.NA
            if not pd.notna(vv) or not pd.notna(rr) or rr == 0.0:
                return pd.NA
            return (vv / rr - 1.0) * 100.0

        def _between(low: object, price: object, high: object) -> object:
            try:
                lo = float(low)  # type: ignore[arg-type]
                px = float(price)  # type: ignore[arg-type]
                hi = float(high)  # type: ignore[arg-type]
            except Exception:
                return pd.NA
            if not pd.notna(lo) or not pd.notna(px) or not pd.notna(hi):
                return pd.NA
            return bool(lo < px < hi)

        between_ema200d_price_ema200w = _between(ema_200, last_close, ema_200w)
        between_ema50d_price_ema200d = _between(ema_50, last_close, ema_200)
        recovering = between_ema200d_price_ema200w
        early_recovery = between_ema50d_price_ema200d

        rows.append(
            {
                "symbol": sym,
                "ticker": short_symbol(sym),
                "date": last_dt.date().isoformat(),
                "last_close": last_close,
                "above_ema20": above_ema20,
                "above_ema50": above_ema50,
                "above_ema200": above_ema200,
                "ema20_value": ema_20,
                "ema50_value": ema_50,
                "ema200_value": ema_200,
                "ema200w_value": ema_200w,
                "close_vs_ema20_pct": _pct_over(ema_20),
                "close_vs_ema50_pct": _pct_over(ema_50),
                "close_vs_ema200_pct": _pct_over(ema_200),
                "close_vs_ema200w_pct": _pct_over(ema_200w),
                "between_ema200d_price_ema200w": between_ema200d_price_ema200w,
                "recovering": recovering,
                "between_ema50d_price_ema200d": between_ema50d_price_ema200d,
                "early_recovery": early_recovery,
                "new_high": new_high_flag,
                "new_low": new_low_flag,
                "high_date": high_date,
                "high_last": high_last,
                "prev_high_max": prev_high_max,
                "high_breakout_pct": _pct_break(high_last, prev_high_max),
                "low_date": low_date,
                "low_last": low_last,
                "prev_low_min": prev_low_min,
                "low_breakout_pct": _pct_break(low_last, prev_low_min),
            }
        )

    if not rows:
        return pd.DataFrame()

    out = pd.DataFrame(rows).set_index("symbol")
    for c in [
        "new_high",
        "new_low",
        "between_ema200d_price_ema200w",
        "recovering",
        "between_ema50d_price_ema200d",
        "early_recovery",
    ]:
        if c in out.columns:
            out[c] = out[c].astype("boolean")
    return out


def _cutoff_for_index(cutoff: pd.Timestamp, idx: pd.Index) -> pd.Timestamp:
    cutoff = pd.Timestamp(cutoff)
    tz = getattr(idx, "tz", None)
    if tz is None:
        return cutoff.tz_localize(None) if getattr(cutoff, "tzinfo", None) is not None else cutoff
    if getattr(cutoff, "tzinfo", None) is None:
        return cutoff.tz_localize(tz)
    return cutoff.tz_convert(tz)


def _clamp_int(v: object, lo: int, hi: int, default: int) -> int:
    try:
        n = int(v)  # type: ignore[arg-type]
    except Exception:
        n = int(default)
    return max(int(lo), min(int(hi), int(n)))


def _calc_fixed_span_for_frames(all_points: pd.DataFrame) -> float:
    if all_points.empty:
        return 6.0
    span_x = max(abs(all_points["rs_ratio"].min() - 100.0), abs(all_points["rs_ratio"].max() - 100.0))
    span_y = max(abs(all_points["rs_mom"].min() - 100.0), abs(all_points["rs_mom"].max() - 100.0))
    base_span = max(span_x, span_y, 2.0)
    pad = max(0.8, base_span * 0.15)
    return base_span + pad


def _build_frame_tails(
    *,
    symbols: Iterable[str],
    symbol_histories: Dict[str, pd.DataFrame],
    frame_date: pd.Timestamp,
    tail_len: int,
) -> Dict[str, pd.DataFrame]:
    tails: Dict[str, pd.DataFrame] = {}
    for sym in symbols:
        history = symbol_histories.get(sym)
        if history is None or history.empty:
            continue
        history_up_to = history.loc[:frame_date]
        if history_up_to.empty:
            continue
        tail_points = history_up_to[["rs_ratio", "rs_mom"]].tail(tail_len)
        if tail_points.empty:
            continue
        tails[sym] = tail_points.copy()
    return tails


def _generate_rrg_frames(
    *,
    symbol_histories: Dict[str, pd.DataFrame],
    bench_dates: Iterable[pd.Timestamp],
    lookback_days: int,
    frame_step: int,
    metric_col: str,
    min_x: float,
    top_n: int,
) -> List[Tuple[pd.Timestamp, pd.DataFrame]]:
    ordered_dates = sorted(pd.to_datetime(list(bench_dates)))
    if not ordered_dates:
        return []
    lookback_days = max(1, int(lookback_days))
    frame_step = max(1, int(frame_step))
    cutoff = ordered_dates[-1] - pd.Timedelta(days=lookback_days)
    frames: List[Tuple[pd.Timestamp, pd.DataFrame]] = []
    for dt in ordered_dates:
        if dt < cutoff:
            continue
        rows_frame: List[dict] = []
        for sym, rrg_df in symbol_histories.items():
            if dt not in rrg_df.index:
                continue
            row = rrg_df.loc[dt]
            rows_frame.append(
                {
                    "symbol": sym,
                    "label": short_symbol(sym),
                    "rs_ratio": float(row["rs_ratio"]),
                    "rs_mom": float(row["rs_mom"]),
                    "quadrant": str(row["quadrant"]),
                    "distance": float(row["distance"]),
                    "speed": float(row.get("speed", float("nan"))),
                }
            )
        if not rows_frame:
            continue
        df_frame = pd.DataFrame(rows_frame).set_index("symbol")
        df_frame = df_frame[df_frame[metric_col].astype(float) >= float(min_x)]
        if df_frame.empty:
            continue
        df_frame = df_frame.sort_values(metric_col, ascending=False).head(int(top_n))
        if df_frame.empty:
            continue
        frames.append((dt, df_frame))
    if frame_step > 1:
        frames = frames[::frame_step]
    return frames


def _build_animation_figure(
    *,
    frames: List[Tuple[pd.Timestamp, pd.DataFrame]],
    symbol_histories: Dict[str, pd.DataFrame],
    tail_len: int,
    metric_col: str,
    min_x: float,
    top_n: int,
    label_mode: LabelMode,
    tail_mode: TailMode,
    theme: Theme,
    fixed_span: Optional[float],
    data_bundle: Dict[str, object],
    base_title: str,
    title_prefix: Optional[str] = None,
) -> Tuple[Optional[go.Figure], List[Tuple[pd.Timestamp, pd.DataFrame]], List[Dict[str, pd.DataFrame]]]:
    if not frames:
        return None, [], []

    frame_figures: List[Tuple[pd.Timestamp, go.Figure]] = []
    frame_tails_list: List[Dict[str, pd.DataFrame]] = []
    full_title = f"{title_prefix} - {base_title}" if title_prefix else base_title
    for frame_date, frame_filtered in frames:
        highlight_symbols = frame_filtered.index.astype(str).tolist()
        frame_tails: Dict[str, pd.DataFrame] = {}
        if tail_mode != "none":
            frame_tails = _build_frame_tails(
                symbols=highlight_symbols,
                symbol_histories=symbol_histories,
                frame_date=frame_date,
                tail_len=tail_len,
            )
        fig_frame = build_rrg_figure(
            points=frame_filtered,
            tails=frame_tails,
            highlighted_symbols=highlight_symbols,
            label_mode=label_mode,
            tail_mode=tail_mode,
            theme=theme,
            fixed_span=fixed_span,
            title=full_title,
        )
        frame_figures.append((frame_date, fig_frame))
        frame_tails_list.append(frame_tails)

    base_fig = frame_figures[0][1]
    animation_fig = go.Figure(
        data=copy.deepcopy(base_fig.data),
        layout=copy.deepcopy(base_fig.layout),
    )
    frame_traces: List[go.Frame] = []

    slider_steps = []
    frame_duration = 400
    for idx, (frame_date, frame_fig) in enumerate(frame_figures):
        frame_name = str(idx)
        frame_title = f"{frame_fig.layout.title.text} | {frame_date.date().isoformat()}"
        frame_layout = go.Layout(title=go.layout.Title(text=frame_title, x=0.5))
        frame_traces.append(
            go.Frame(
                data=copy.deepcopy(frame_fig.data),
                layout=frame_layout,
                name=frame_name,
            )
        )
        slider_steps.append(
            {
                "args": [
                    [frame_name],
                    {
                        "frame": {"duration": frame_duration, "redraw": True},
                        "mode": "immediate",
                    },
                ],
                "label": frame_date.date().isoformat(),
                "method": "animate",
            }
        )

    animation_fig.update_layout(
        updatemenus=[
            {
                "type": "buttons",
                "showactive": False,
                "x": 0.05,
                "y": 0.02,
                "xanchor": "left",
                "yanchor": "bottom",
                "direction": "left",
                "pad": {"r": 10, "t": 10},
                "buttons": [
                    {
                        "args": [
                            None,
                            {
                                "frame": {"duration": frame_duration, "redraw": True},
                                "fromcurrent": True,
                                "transition": {"duration": 0},
                            },
                        ],
                        "label": "Play",
                        "method": "animate",
                    },
                    {
                        "args": [
                            [[None]],
                            {
                                "frame": {"duration": 0, "redraw": True},
                                "mode": "immediate",
                                "transition": {"duration": 0},
                            },
                        ],
                        "label": "Pause",
                        "method": "animate",
                    },
                ],
            }
        ],
        sliders=[
            {
                "active": 0,
                "pad": {"t": 60, "l": 10, "r": 10},
                "steps": slider_steps,
                "currentvalue": {
                    "visible": True,
                    "prefix": "Frame: ",
                    "font": {"size": 12},
                    "xanchor": "right",
                },
            }
        ],
    )

    animation_fig.frames = frame_traces

    return animation_fig, frames, frame_tails_list


def main() -> None:
    cfg = AppConfig()
    st.session_state.setdefault("ui_lang", "th")
    lang = str(st.session_state.get("ui_lang") or "th")
    if lang not in {"th", "en"}:
        lang = "th"
        st.session_state["ui_lang"] = lang

    st.set_page_config(page_title=_t("app_title", lang=lang), layout="wide")
    _inject_dashboard_css()
    _render_app_hero(
        title=_t("app_title", lang=lang),
        subtitle=(
            "Professional Relative Rotation dashboard for SET100 with structured screening and risk-aware visuals."
            if lang == "en"
            else "แดชบอร์ดวิเคราะห์ Relative Rotation สำหรับ SET100 แบบเป็นระบบ ใช้งานง่าย และเน้นความน่าเชื่อถือ"
        ),
    )
    animate_rrg = False
    if "public_link_modal" not in st.session_state:
        st.session_state["public_link_modal"] = False
    if "public_link_allowed" not in st.session_state:
        st.session_state["public_link_allowed"] = False

    cache = DiskCache(cfg.cache_dir)
    client = TradingViewClient(timeout=35)

    with st.sidebar:
        st.markdown("### Control Center" if lang == "en" else "### แผงควบคุม")
        lang_label = _t("sidebar_lang", lang=lang)
        lang_choice = st.selectbox(
            lang_label,
            options=["th", "en"],
            index=0 if lang == "th" else 1,
            format_func=lambda x: _t("lang_th", lang=lang) if x == "th" else _t("lang_en", lang=lang),
        )
        st.session_state["ui_lang"] = str(lang_choice)
        lang = str(st.session_state.get("ui_lang") or "th")

        tab_data, tab_visual, tab_help, tab_donation = st.tabs(
            [_t("tab_data", lang=lang), _t("tab_visual", lang=lang), _t("tab_guide", lang=lang), _t("tab_donation", lang=lang)]
        )

        with tab_data:
            st.markdown("#### Data Setup")
            st.caption(
                "Change settings, then click Fetch to avoid request limits."
                if lang == "en"
                else "เปลี่ยนค่าตามต้องการ แล้วกดดึงข้อมูลเพื่อลดการชน rate limit"
            )
            st.caption("Universe and benchmark are managed centrally to keep screening consistent.")
            with st.form("data_form", clear_on_submit=False):
                # ใช้ชุดสัญลักษณ์เริ่มต้นจากระบบ ไม่ให้แก้ไขใน UI
                st.caption(
                    "Universe: ใช้ชุดหุ้นค่าเริ่มต้นที่กำหนดในระบบ"
                    if lang == "th"
                    else "Universe: using the default symbol list defined in the app."
                )

                benchmark_raw = st.text_input(
                    _t("benchmark", lang=lang),
                    value=cfg.default_benchmark,
                )
                tf_label = st.selectbox(
                    _t("timeframe", lang=lang),
                    options=["Daily", "Weekly"],
                    index=0,
                    help=_t("timeframe_help", lang=lang),
                )
                bars = st.number_input(
                    _t("bars", lang=lang),
                    min_value=90,
                    max_value=3000,
                    value=cfg.default_bars,
                    step=10,
                )

                # TTL / workers ใช้ค่าคงที่จาก AppConfig ไม่ต้องให้ user ตั้ง
                force_refresh = st.toggle(_t("force_refresh", lang=lang), value=False)

                fetch_clicked = st.form_submit_button(
                    _t("fetch_btn", lang=lang),
                    type="primary",
                )

            last_bundle = st.session_state.get("data_bundle")
            if last_bundle:
                st.caption(
                    _t(
                        "last_fetch",
                        lang=lang,
                        time=_format_time(float(last_bundle["fetched_at"])),
                        tf=last_bundle["tf_label"],
                        bars=last_bundle["bars"],
                        n=len(last_bundle["symbols"]),
                    )
                )

        with tab_visual:
            st.markdown("#### Chart Controls")
            st.caption(
                "Theme preset: Institutional Dark (graphite)." if lang == "en" else "Theme preset: Institutional Dark (graphite)."
            )
            theme = "dark"
            st.markdown(_t("visual_quick_guide", lang=lang))
            st.caption("Performance mode defaults: tails OFF, animation OFF.")

            fixed_span: Optional[float] = None
            col_scale, col_rank, col_anim = st.columns(3)

            # ซ้าย: Scale
            with col_scale:
                st.subheader("Scale" if lang == "en" else "สเกลกราฟ")
                auto_range = st.toggle(_t("auto_scale", lang=lang), value=True)
                if not auto_range:
                    fixed_span = float(
                        st.slider(
                            _t("axis_span", lang=lang),
                            min_value=2.0,
                            max_value=20.0,
                            value=6.0,
                            step=0.5,
                            help=_t("axis_span_help", lang=lang),
                        )
                    )

            # กลาง: การจัดลำดับจุด
            with col_rank:
                st.subheader("Ranking" if lang == "en" else "การจัดลำดับ")
                metric_ui = st.selectbox(
                    _t("metric_sort", lang=lang),
                    options=["Speed", "Distance"],
                    index=0,
                    help=_t("metric_sort_help", lang=lang),
                )
                top_n = st.slider(
                    _t("top_n", lang=lang),
                    min_value=1,
                    max_value=100,
                    value=20,
                    step=1,
                    help=_t("top_n_help", lang=lang),
                )
                min_x = float(
                    st.number_input(
                        _t("min_filter", lang=lang),
                        min_value=0.0,
                        value=0.0,
                        step=0.1,
                        help=_t("min_filter_help", lang=lang),
                    )
                )

            # ขวา: หาง + อนิเมชัน
            animation_window = 90
            animation_step = 1
            with col_anim:
                st.subheader("Tails & Animation" if lang == "en" else "หาง & อนิเมชัน")
                st.markdown(_t("tails_heading", lang=lang))
                show_tails_top = st.toggle(
                    _t("show_tails_topn", lang=lang),
                    value=False,
                    help=_t("show_tails_topn_help", lang=lang),
                )

                st.markdown(_t("animation_heading", lang=lang))
                animate_rrg = st.checkbox(
                    _t("animate_timeline", lang=lang),
                    value=False,
                    help=_t("animate_help", lang=lang),
                )
                animation_window = st.slider(
                    _t("anim_window", lang=lang),
                    min_value=90,
                    max_value=365,
                    value=90,
                    step=15,
                    help=_t("anim_window_help", lang=lang),
                )
                animation_step = st.slider(
                    _t("frame_step", lang=lang),
                    min_value=1,
                    max_value=10,
                    value=1,
                    step=1,
                    help=_t("frame_step_help", lang=lang),
                )

            label_mode_ui = "Highlighted"
            tail_mode_ui = "Highlighted" if show_tails_top else "None"

        with tab_help:
            if lang == "th":
                st.markdown("## คู่มือภาพรวมแดชบอร์ด")
                st.markdown(
                    "แดชบอร์ดนี้ใช้วิเคราะห์ **Relative Rotation Graph (RRG)** ของหุ้นในกลุ่มที่คุณเลือก "
                    "เพื่อดูว่าหุ้นไหนกำลัง **แข็งแรงขึ้น / อ่อนแรงลง** เมื่อเทียบกับ Benchmark ในมุมมองเวลาเดียวกัน\n\n"
                    "- เหมาะสำหรับดู **ภาพรวมตลาด** และหา **ผู้นำ (leaders)** / **ผู้ตาม (laggards)**\n"
                    "- ใช้ได้ทั้งมุมมอง **ระยะสั้น** (โมเมนตัม) และ **ระยะกลาง–ยาว** (ตำแหน่งเทียบ High/Low)\n"
                )
                st.markdown("### ขั้นตอนการใช้งานแบบย่อ")
                st.markdown(
                    "1. ไปที่แท็บ **ข้อมูล (Data)** ทาง Sidebar\n"
                    "   - ใส่รายชื่อหุ้นบรรทัดละ 1 ตัว (ไม่ต้องพิมพ์ `SET:`)\n"
                    "   - เลือก **Benchmark**, **Timeframe (Daily/Weekly)** และจำนวน Bars\n"
                    "   - กดปุ่ม **ดึง/รีเฟรชข้อมูล** เพื่อโหลดข้อมูลล่าสุด\n"
                    "2. กลับมาที่หน้า RRG (แท็บหลักด้านบนกราฟ)\n"
                    "   - เลือก **โมเดล** (EMA / 3 เดือน / 52 สัปดาห์ High/Low)\n"
                    "   - ปรับความยาว EMA หรือช่วงดู Momentum และความยาวหาง (Tail)\n"
                    "3. ใช้แท็บ **สรุปตาราง (Snapshot)** เพื่อดูตัวเลขละเอียดและจุด 3 เดือน / 52 สัปดาห์\n"
                    "4. ใช้แท็บ **Market Breadth** เพื่อดูสุขภาพรวมของกลุ่มหุ้นที่เลือก\n"
                )
                st.markdown("### คำอธิบายแต่ละแท็บหลัก")
                st.markdown(
                    "- **RRG**: แสดงกราฟ 4 Quadrant (Leading / Weakening / Lagging / Improving) ของหุ้นเทียบ Benchmark\n"
                    "- **สรุปตาราง (Snapshot)**: รวมตาราง RS Ratio / RS Momentum / Distance + ระยะห่างจาก High/Low\n"
                    "- **Market Breadth**: ดูเปอร์เซ็นต์หุ้นที่อยู่เหนือ EMA 20 / 50 / 200 และจำนวนทำ New High / New Low\n"
                    "- **Errors**: แสดงสัญลักษณ์ที่คำนวณไม่ได้ พร้อมข้อความอธิบายสาเหตุ\n"
                )
                st.markdown("### การตั้งค่าที่ควรรู้")
                st.markdown(
                    "- **Auto scale (ปรับสเกลอัตโนมัติ)**: แนะนำให้เปิด เพื่อให้กราฟโฟกัสใกล้จุด 100/100\n"
                    "- **Metric สำหรับจัดอันดับ (Speed / Distance)**:\n"
                    "  - *Speed*: เน้นหุ้นที่โมเมนตัมเปลี่ยนเร็ว เหมาะกับคนมองเทรนด์กำลังเปลี่ยน\n"
                    "  - *Distance*: เน้นหุ้นที่อยู่ไกลจาก Benchmark มาก เหมาะกับการหาตัวที่ “โดดออกมา” ชัดเจน\n"
                    "- **Top N / ตัวกรองขั้นต่ำ (Metric)**: ใช้จำกัดจำนวนสัญลักษณ์บนกราฟ และซ่อนตัวที่สัญญาณอ่อนมาก\n"
                    "- **หาง (Tails)**: เปิดเพื่อดูเส้นทางการเคลื่อนที่ของหุ้นในช่วงย้อนหลังตามความยาว Tail\n"
                    "- **อนิเมชัน (Animation)**: เล่นย้อน timeline เพื่อดูการหมุนของกลุ่มผู้นำ–ผู้ตามตลอดช่วงที่เลือก\n"
                )
                st.markdown("### วิธีอ่าน RS Ratio / RS Momentum และ Quadrant")
                st.markdown(
                    "- **RS Ratio ~ 100**: เคลื่อนไหวใกล้เคียง Benchmark, มากกว่า 100 = แข็งแกร่งกว่า, น้อยกว่า 100 = อ่อนกว่า\n"
                    "- **RS Momentum ~ 100**: วัดความเร็วของการเปลี่ยนแปลง RS Ratio, มากกว่า 100 = แนวโน้มกำลังดีขึ้น, น้อยกว่า 100 = แนวโน้มกำลังแย่ลง\n"
                    "- การตีความ 4 Quadrant (โดยทั่วไป):\n"
                    "  - **Leading (ขวาบน)**: แข็งแกร่งและยังดีขึ้นต่อเนื่อง มักเป็นผู้นำรอบตลาด\n"
                    "  - **Weakening (ขวาล่าง)**: ยังแข็งกว่าตลาด แต่โมเมนตัมเริ่มอ่อนลง ระวังการพักตัวหรือกลับทิศ\n"
                    "  - **Lagging (ซ้ายล่าง)**: อ่อนกว่าตลาดและยังแย่ต่อเนื่อง มักใช้เป็นกลุ่มหลีกเลี่ยง\n"
                    "  - **Improving (ซ้ายบน)**: เคยอ่อน แต่กำลังฟื้นตัว โมเมนตัมดีขึ้น อาจกลายเป็นผู้นำรอบถัดไป\n"
                )
                st.markdown("### โน้ตเกี่ยวกับโมเดล (Models)")
                st.markdown(
                    "- **EMA model**: ใช้ค่าเฉลี่ยแบบ EMA ของความแข็งแกร่งสัมพัทธ์ในการคำนวณ RS Ratio และ RS Momentum\n"
                    "- **3 เดือน / 52 สัปดาห์ High/Low**:\n"
                    "  - RS Ratio มาจากตำแหน่งราคาปิดเทียบกับ High หรือ Low ของช่วงเวลาที่กำหนดของหุ้นแต่ละตัว\n"
                    "  - เปรียบเทียบกับ Benchmark ในช่วงเวลาเดียวกัน เพื่อดูว่าใครฟื้นตัว/ย่อตัวได้ดีกว่า\n"
                    "  - RS Momentum วัดการเปลี่ยนแปลงของตำแหน่งนี้ตามค่า **Momentum lookback (แท่ง)**\n"
                )
            else:
                st.markdown("## Dashboard overview")
                st.markdown(
                    "This dashboard helps you analyze **Relative Rotation Graphs (RRG)** for the symbols you choose, "
                    "so you can see which names are **improving or weakening** versus a chosen benchmark over time.\n\n"
                    "- Use it to get a **market-wide overview** and to spot **leaders vs laggards**.\n"
                    "- Works for both **short-term momentum** and **medium/long-term position vs High/Low windows**.\n"
                )
                st.markdown("### Quick workflow")
                st.markdown(
                    "1. Go to the **Data** tab in the sidebar\n"
                    "   - Enter one symbol per line (you don’t need to type `SET:`)\n"
                    "   - Pick **Benchmark**, **Timeframe (Daily/Weekly)**, and number of bars\n"
                    "   - Click **Fetch / Refresh Data** to load the latest data\n"
                    "2. Switch to the main **RRG** tab\n"
                    "   - Choose a **Model** (EMA / 3‑month / 52‑week High/Low)\n"
                    "   - Adjust EMA lengths or Momentum lookback and Tail length as needed\n"
                    "3. Use the **Snapshot** tab to inspect detailed values and 3‑month / 52‑week mark points\n"
                    "4. Use the **Market Breadth** tab to understand the overall health of the symbol universe you loaded\n"
                )
                st.markdown("### What each main tab shows")
                st.markdown(
                    "- **RRG**: core 4‑quadrant chart (Leading / Weakening / Lagging / Improving) versus the benchmark\n"
                    "- **Snapshot**: table with RS Ratio / RS Momentum / Distance plus distance from 3‑month / 52‑week High/Low\n"
                    "- **Market Breadth**: % of symbols above EMA 20 / 50 / 200 and counts of new 52‑week highs vs lows\n"
                    "- **Errors**: list of symbols that failed to compute, with explanations\n"
                )
                st.markdown("### Key settings to pay attention to")
                st.markdown(
                    "- **Auto scale (recommended)**: keeps the cloud of points centered around 100/100; turn it off to lock a fixed span\n"
                    "- **Metric to sort (Speed / Distance)**:\n"
                    "  - *Speed*: focuses on names with fast momentum changes, great for spotting turning points\n"
                    "  - *Distance*: focuses on how far symbols sit from the benchmark, great for highlighting strong outliers\n"
                    "- **Top N / Minimum metric filter**: limit clutter and hide very weak signals by keeping only the strongest names\n"
                    "- **Tails**: show historical paths so you can see the direction of travel, not just the latest point\n"
                    "- **Animation**: plays through the selected window so you can watch rotations between quadrants over time\n"
                )
                st.markdown("### Reading RS Ratio, RS Momentum, and quadrants")
                st.markdown(
                    "- **RS Ratio ≈ 100**: moving roughly in line with the benchmark; above 100 = outperforming, below 100 = underperforming\n"
                    "- **RS Momentum ≈ 100**: measures the pace of change in RS Ratio; above 100 = improving, below 100 = deteriorating\n"
                    "- Typical quadrant interpretation:\n"
                    "  - **Leading (top right)**: strong and still improving, often current market leaders\n"
                    "  - **Weakening (bottom right)**: still strong vs benchmark but momentum is rolling over, watch for consolidations/reversals\n"
                    "  - **Lagging (bottom left)**: weak and still deteriorating, often candidates to avoid\n"
                    "  - **Improving (top left)**: recovering from weakness with rising momentum, potential future leaders\n"
                )
                st.markdown("### Model notes")
                st.markdown(
                    "- **EMA model**: RS Ratio and RS Momentum are derived from EMAs of relative strength versus the benchmark.\n"
                    "- **3‑month / 52‑week High/Low models**:\n"
                    "  - RS Ratio is based on where the close sits inside its own High/Low window, compared to the benchmark in the same window.\n"
                    "  - RS Momentum tracks how that position changes over the chosen **Momentum lookback (bars)**.\n"
                )

            st.markdown(_t("prerelease_heading", lang=lang))
            if st.button(_t("prerelease_request", lang=lang), key="public_link_request"):
                st.session_state["public_link_modal"] = True
            if st.session_state["public_link_allowed"]:
                st.success(_t("prerelease_ready", lang=lang))
                st.markdown(f"[🔗 ลิงก์เผยแพร่]({PUBLIC_RELEASE_URL})")

        with tab_donation:
            btc_addr = "18JVHjNfkd7F3hCDT3bgi58pbBy4dnt3LY"
            sol_addr = "J74BbKp7Xh69nt27ejYu8tvyK3DtQb85fgcWakit37ZU"
            xrp_addr = "rJn2zAPdFA193sixJwuFixRkYDUtx3apQh"
            xrp_memo = "500363648"
            usdt_addr = "0x4c6a2b66f293bb84e63b2fc6444bd1e8f7a34d4b"
            usdt_net = "BNB"
            st.markdown(f"### {_t('donation_title', lang=lang)}")
            thanks_text = _t("donation_thanks", lang=lang)
            if thanks_text.strip() != _t("donation_title", lang=lang).strip():
                st.caption(thanks_text)

            st.markdown(f"**{_t('donation_btc', lang=lang)}**")
            st.text_input("BTC address", value=btc_addr, key="donation_btc_addr", disabled=True)

            st.markdown(f"**{_t('donation_sol', lang=lang)}**")
            st.text_input("SOL address", value=sol_addr, key="donation_sol_addr", disabled=True)

            st.markdown(f"**{_t('donation_xrp', lang=lang)}**")
            st.text_input("XRP address", value=xrp_addr, key="donation_xrp_addr", disabled=True)
            st.text_input(_t("donation_xrp_memo", lang=lang), value=xrp_memo, key="donation_xrp_memo", disabled=True)

            st.markdown(f"**{_t('donation_usdt', lang=lang)}**")
            st.write(_t("donation_network", lang=lang, net=usdt_net))
            st.text_input("USDT (BEP20) address", value=usdt_addr, key="donation_usdt_bep20_addr", disabled=True)

    if st.session_state.get("public_link_modal"):
        st.warning("ลิงก์นี้จะเผยแพร่ให้คนทั่วไปเห็น คุณต้องยืนยันแล้วเท่านั้นจึงจะเข้าได้")
        st.markdown("หากคุณพร้อมเผยแพร่ กด \"ยืนยัน\" แล้วระบบจะปล่อยลิงก์จริงให้")
        col_confirm, col_cancel = st.columns(2)
        if col_confirm.button("ยืนยันเผยแพร่", key="public_link_confirm"):
            st.session_state["public_link_allowed"] = True
            st.session_state["public_link_modal"] = False
        if col_cancel.button("ยกเลิก", key="public_link_cancel"):
            st.session_state["public_link_modal"] = False

    st.session_state.setdefault("auto_fetch_attempted", False)
    auto_fetch = (
        (not bool(fetch_clicked))
        and (not bool(st.session_state.get("auto_fetch_attempted")))
        and (st.session_state.get("data_bundle") is None)
    )

    if fetch_clicked or auto_fetch:
        if auto_fetch:
            st.session_state["auto_fetch_attempted"] = True
        # ใช้ default_symbols จาก config แทน input user
        symbols_raw = parse_symbol_list(cfg.default_symbols)
        symbols = [format_set_symbol(s) for s in symbols_raw]
        benchmark = (
            format_set_symbol(benchmark_raw)
            if ":" not in benchmark_raw
            else benchmark_raw.strip().upper()
        )
        resolution = resolution_from_label(tf_label)
        # TTL fix ตามค่า default ใน AppConfig
        ttl_seconds = int(cfg.default_cache_ttl_hours) * 3600

        if not symbols:
            st.error(_t("no_symbols", lang=lang))
            return

        with st.status(_t("fetching", lang=lang), expanded=False) as status:
            status.update(label=_t("fetching_bench", lang=lang, bench=benchmark))
            _, bench_df, bench_err = _fetch_one(
                client=client,
                cache=cache,
                symbol=benchmark,
                resolution=resolution,
                bars=int(bars),
                ttl_seconds=ttl_seconds,
                refresh=bool(force_refresh),
            )
            if bench_err or bench_df is None:
                st.error(
                    _t(
                        "benchmark_error",
                        lang=lang,
                        err=(bench_err or "Unknown error"),
                    )
                )
                return

            bench_close = _align_close(bench_df)
            asof = bench_close.index[-1].date().isoformat() if not bench_close.empty else "n/a"
            bench_ohlcv = bench_df.copy()

            closes: Dict[str, pd.Series] = {}
            ohlcv: Dict[str, pd.DataFrame] = {}
            errors: Dict[str, str] = {}

            status.update(label=_t("fetching_syms", lang=lang, n=len(symbols)))
            # จำนวน workers คงที่ เพื่อลดโอกาสชน rate limit
            safe_workers = 3
            with futures.ThreadPoolExecutor(max_workers=safe_workers) as ex:
                jobs = [
                    ex.submit(
                        _fetch_one,
                        client=client,
                        cache=cache,
                        symbol=s,
                        resolution=resolution,
                        bars=int(bars),
                        ttl_seconds=ttl_seconds,
                        refresh=bool(force_refresh),
                    )
                    for s in symbols
                ]
                done = 0
                prog = st.progress(0.0)
                for job in futures.as_completed(jobs):
                    sym, df, err = job.result()
                    done += 1
                    prog.progress(done / len(jobs))
                    if df is not None:
                        try:
                            ohlcv[sym] = df.copy()
                            closes[sym] = _align_close(df)
                        except Exception as e:  # noqa: BLE001
                            errors[sym] = str(e)
                    else:
                        errors[sym] = err or "Unknown error"

            timeout_symbols = [
                sym for sym, err_msg in errors.items() if "timeout while fetching" in str(err_msg).lower()
            ]
            if timeout_symbols:
                status.update(label=f"Retrying timed-out symbols sequentially ({len(timeout_symbols)})...")
                for sym in timeout_symbols:
                    _, df_retry, err_retry = _fetch_one(
                        client=client,
                        cache=cache,
                        symbol=sym,
                        resolution=resolution,
                        bars=int(bars),
                        ttl_seconds=ttl_seconds,
                        refresh=True,
                    )
                    if df_retry is not None:
                        try:
                            ohlcv[sym] = df_retry.copy()
                            closes[sym] = _align_close(df_retry)
                            errors.pop(sym, None)
                        except Exception as e:  # noqa: BLE001
                            errors[sym] = str(e)
                    else:
                        errors[sym] = err_retry or errors.get(sym, "Unknown error")

            status.update(label=_t("done", lang=lang))

        st.session_state["data_bundle"] = {
            "symbols": symbols,
            "benchmark": benchmark,
            "tf_label": tf_label,
            "resolution": resolution,
            "bars": int(bars),
            "asof": asof,
            "ref_ts": bench_close.index[-1] if not bench_close.empty else pd.Timestamp.utcnow(),
            "bench_ohlcv": bench_ohlcv,
            "bench_close": bench_close,
            "closes": closes,
            "ohlcv": ohlcv,
            "errors": errors,
            "fetched_at": time.time(),
            "ttl_hours": int(cfg.default_cache_ttl_hours),
        }
        st.session_state["data_gen"] = int(st.session_state.get("data_gen", 0)) + 1
        st.session_state.pop("rrg_key", None)

    data_bundle = st.session_state.get("data_bundle")
    if not data_bundle:
        st.info(_t("no_data", lang=lang))
        return

    symbols = data_bundle.get("symbols") or []
    data_tf = str(data_bundle.get("tf_label") or "Daily")
    status_labels = (
        {
            "last_fetch": "Last Fetch",
            "benchmark": "Benchmark",
            "timeframe": "Timeframe",
            "bars": "Bars",
            "universe": "Universe",
            "ttl": "Cache TTL (h)",
        }
        if lang == "en"
        else {
            "last_fetch": "Last Fetch",
            "benchmark": "Benchmark",
            "timeframe": "Timeframe",
            "bars": "Bars",
            "universe": "Universe",
            "ttl": "Cache TTL (h)",
        }
    )
    _render_status_cards(
        [
            (status_labels["last_fetch"], _format_time(float(data_bundle.get("fetched_at") or time.time()))),
            (status_labels["benchmark"], str(data_bundle.get("benchmark") or "-")),
            (status_labels["timeframe"], data_tf),
            (status_labels["bars"], str(data_bundle.get("bars") or "-")),
            (status_labels["universe"], str(len(symbols))),
            (status_labels["ttl"], str(data_bundle.get("ttl_hours") or cfg.default_cache_ttl_hours)),
        ]
    )

    # RRG settings (shown in main RRG tab)
    st.session_state.setdefault("rrg_model_id", "ema")
    st.session_state.setdefault("rrg_ratio_len", 70)
    st.session_state.setdefault("rrg_mom_len", 50)
    st.session_state.setdefault("rrg_mom_lookback", 20)
    st.session_state.setdefault("rrg_tail_len", 20)

    model_id = str(st.session_state.get("rrg_model_id") or "ema")
    if model_id not in {"ema", "3m_high", "52w_high", "52w_low"}:
        model_id = "ema"
        st.session_state["rrg_model_id"] = model_id

    if model_id == "ema":
        if data_tf == "Weekly":
            ratio_len = _clamp_int(st.session_state.get("rrg_ratio_len"), 5, 40, 14)
            mom_len = _clamp_int(st.session_state.get("rrg_mom_len"), 5, 40, 10)
            tail_len = _clamp_int(st.session_state.get("rrg_tail_len"), 5, 30, 12)
        else:
            ratio_len = _clamp_int(st.session_state.get("rrg_ratio_len"), 20, 120, 70)
            mom_len = _clamp_int(st.session_state.get("rrg_mom_len"), 10, 120, 50)
            tail_len = _clamp_int(st.session_state.get("rrg_tail_len"), 5, 60, 20)
        st.session_state["rrg_ratio_len"] = int(ratio_len)
        st.session_state["rrg_mom_len"] = int(mom_len)
        st.session_state["rrg_tail_len"] = int(tail_len)
    else:
        ratio_len = 0
        if model_id == "3m_high":
            if data_tf == "Weekly":
                mom_len = _clamp_int(st.session_state.get("rrg_mom_lookback"), 1, 12, 4)
                tail_len = _clamp_int(st.session_state.get("rrg_tail_len"), 5, 30, 12)
            else:
                mom_len = _clamp_int(st.session_state.get("rrg_mom_lookback"), 1, 60, 20)
                tail_len = _clamp_int(st.session_state.get("rrg_tail_len"), 5, 60, 20)
        else:
            if data_tf == "Weekly":
                mom_len = _clamp_int(st.session_state.get("rrg_mom_lookback"), 1, 52, 12)
                tail_len = _clamp_int(st.session_state.get("rrg_tail_len"), 5, 30, 12)
            else:
                mom_len = _clamp_int(st.session_state.get("rrg_mom_lookback"), 1, 252, 60)
                tail_len = _clamp_int(st.session_state.get("rrg_tail_len"), 5, 60, 20)
        st.session_state["rrg_mom_lookback"] = int(mom_len)
        st.session_state["rrg_tail_len"] = int(tail_len)

    data_errors: Dict[str, str] = dict(data_bundle.get("errors") or {})
    closes = data_bundle.get("closes") or {}
    ohlcv = data_bundle.get("ohlcv") or {}
    bench_ohlcv = data_bundle.get("bench_ohlcv")
    bench_close = data_bundle["bench_close"]
    ref_ts = data_bundle.get("ref_ts") or (bench_close.index[-1] if not bench_close.empty else pd.Timestamp.utcnow())

    rrg_cache = st.session_state.setdefault("rrg_cache", {})
    data_gen = int(st.session_state.get("data_gen", 0))
    cache_key = (data_gen, tuple(symbols), str(model_id), int(ratio_len), int(mom_len), int(tail_len))
    cached = rrg_cache.get(cache_key)
    if cached:
        table, tails, compute_errors, symbol_histories = cached
    else:
        with st.status(_t("computing_rrg", lang=lang), expanded=False) as status:
            table, tails, compute_errors, symbol_histories = _compute_rrg_bundle(
                closes=closes,
                bench_close=bench_close,
                ohlcv=ohlcv,
                bench_ohlcv=bench_ohlcv,
                model_id=str(model_id),
                ratio_len=int(ratio_len),
                mom_len=int(mom_len),
                tail_len=int(tail_len),
            )
            status.update(label="Done.")
        rrg_cache[cache_key] = (table, tails, compute_errors, symbol_histories)

    st.session_state["rrg_table"] = table
    st.session_state["rrg_tails"] = tails
    st.session_state["rrg_errors"] = compute_errors
    st.session_state["rrg_histories"] = symbol_histories

    table: pd.DataFrame = st.session_state.get("rrg_table")  # type: ignore[assignment]
    tails: Dict[str, pd.DataFrame] = st.session_state.get("rrg_tails") or {}
    compute_errors: Dict[str, str] = st.session_state.get("rrg_errors") or {}

    table_is_empty = table is None or table.empty
    metric_col = "speed" if metric_ui == "Speed" else "distance"
    if table_is_empty:
        filtered = pd.DataFrame()
    else:
        filtered = table[table[metric_col].astype(float) >= float(min_x)].sort_values(metric_col, ascending=False)
        filtered = filtered.head(int(top_n))

    if filtered.empty:
        display_table = pd.DataFrame(
            columns=["symbol", "label", "rs_ratio", "rs_mom", "quadrant", "distance", "speed", "angle_deg", "date"]
        ).set_index("symbol")
        display_tails = {}
        highlight_symbols = []
    else:
        display_table = filtered
        display_tails = {s: tails[s] for s in filtered.index if s in tails}
        highlight_symbols = filtered.index.astype(str).tolist()

    label_mode = "highlighted"
    tail_mode = "highlighted" if show_tails_top else "none"

    frames_list = (
        _generate_rrg_frames(
            symbol_histories=symbol_histories,
            bench_dates=bench_close.index,
            lookback_days=animation_window,
            frame_step=animation_step,
            metric_col=metric_col,
            min_x=min_x,
            top_n=top_n,
        )
        if not table_is_empty
        else []
    )
    union_frame_df = pd.concat([df for _, df in frames_list]) if frames_list else pd.DataFrame()
    global_span = _calc_fixed_span_for_frames(union_frame_df)
    fixed_span_for_display = fixed_span
    frame_caption = None
    animation_fig: Optional[go.Figure] = None
    animation_records: List[Tuple[pd.Timestamp, pd.DataFrame]] = []
    animation_tails: List[Dict[str, pd.DataFrame]] = []
    chart_title = f"RRG - {short_symbol(data_bundle['benchmark'])} - {data_bundle['tf_label']} - {data_bundle['asof']}"
    if animate_rrg and frames_list:
        symbol_histories = st.session_state.get("rrg_histories") or {}
        animation_fig, animation_records, animation_tails = _build_animation_figure(
            frames=frames_list,
            symbol_histories=symbol_histories,
            tail_len=int(tail_len),
            metric_col=metric_col,
            min_x=min_x,
            top_n=top_n,
            label_mode=label_mode,  # type: ignore[arg-type]
            tail_mode=tail_mode,  # type: ignore[arg-type]
            theme=theme,  # type: ignore[arg-type]
            fixed_span=global_span,
            data_bundle=data_bundle,
            base_title=chart_title,
            title_prefix="Animate timeline",
        )
        if animation_fig and animation_records:
            frame_caption = (
                f"Animation range: {animation_records[0][0].date().isoformat()} -> "
                f"{animation_records[-1][0].date().isoformat()}"
            )
            last_frame_df = animation_records[-1][1]
            display_table = last_frame_df
            highlight_symbols = display_table.index.astype(str).tolist()
            display_tails = animation_tails[-1] if animation_tails else {}
            fixed_span_for_display = global_span

    if animation_fig is None:
        if frames_list:
            max_idx = len(frames_list) - 1
            default_idx = int(st.session_state.get("rrg_frame_idx", max_idx))
            default_idx = max(0, min(max_idx, default_idx))
            frame_idx = st.slider(
                "Timeline frame",
                min_value=0,
                max_value=max_idx,
                value=default_idx,
                step=1,
                key="rrg_frame_slider",
            )
            st.session_state["rrg_frame_idx"] = frame_idx
            frame_date, frame_df = frames_list[frame_idx]
            frame_filtered = frame_df[frame_df[metric_col].astype(float) >= float(min_x)]
            frame_filtered = frame_filtered.sort_values(metric_col, ascending=False).head(int(top_n))
            display_table = frame_filtered
            highlight_symbols = display_table.index.astype(str).tolist()
            display_tails = {s: tails[s] for s in display_table.index if s in tails}
            frame_caption = f"Frame date: {frame_date.date().isoformat()}"
            fixed_span_for_display = global_span
        else:
            fixed_span_for_display = fixed_span

    if frame_caption:
        st.caption(frame_caption)

    fig = None
    if not display_table.empty:
        fig = animation_fig or build_rrg_figure(
            points=display_table,
            tails=display_tails,
            highlighted_symbols=highlight_symbols,
            label_mode=label_mode,  # type: ignore[arg-type]
            tail_mode=tail_mode,  # type: ignore[arg-type]
            theme=theme,  # type: ignore[arg-type]
            fixed_span=fixed_span_for_display,
            title=chart_title,
        )

    # High/low window mark points for reference tables (based on OHLCV high/low)
    three_cache = st.session_state.setdefault("three_month_high_cache", {})
    fifty_two_cache = st.session_state.setdefault("fifty_two_week_high_cache", {})
    fifty_two_low_cache = st.session_state.setdefault("fifty_two_week_low_cache", {})
    ref_ts_naive = _to_naive_timestamp(pd.Timestamp(ref_ts))
    start_current_month = ref_ts_naive.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    three_key = (data_gen, str(start_current_month.date()))
    three_month_high_df = three_cache.get(three_key)
    if three_month_high_df is None:
        rows_3m: List[dict] = []
        for sym in symbols:
            df_sym = ohlcv.get(sym)
            high_val, high_dt = _three_month_high_mark(df_sym, ref_date=ref_ts_naive)
            if high_val is None or high_dt is None:
                continue
            close_series = closes.get(sym)
            last_close = float("nan")
            if close_series is not None and not close_series.empty:
                try:
                    cutoff = _cutoff_for_index(ref_ts_naive, close_series.index)
                    close_up_to = close_series.loc[:cutoff]
                except Exception:
                    close_up_to = close_series
                if not close_up_to.empty:
                    last_close = float(close_up_to.iloc[-1])
            pct_from_high = (last_close / high_val - 1.0) * 100.0 if pd.notna(last_close) else float("nan")
            rows_3m.append(
                {
                    "symbol": sym,
                    "ticker": short_symbol(sym),
                    "three_month_high": float(high_val),
                    "three_month_high_date": high_dt.date().isoformat(),
                    "last_close": last_close,
                    "pct_from_high": float(pct_from_high),
                }
            )
        three_month_high_df = pd.DataFrame(rows_3m).set_index("symbol") if rows_3m else pd.DataFrame()
        three_cache[three_key] = three_month_high_df

    start_current_week = _week_start_naive(ref_ts_naive)
    fifty_two_key = (data_gen, str(start_current_week.date()))
    fifty_two_week_high_df = fifty_two_cache.get(fifty_two_key)
    if fifty_two_week_high_df is None:
        rows_52w: List[dict] = []
        for sym in symbols:
            df_sym = ohlcv.get(sym)
            high_val, high_dt = _fifty_two_week_high_mark(df_sym, ref_date=ref_ts_naive)
            if high_val is None or high_dt is None:
                continue
            close_series = closes.get(sym)
            last_close = float("nan")
            if close_series is not None and not close_series.empty:
                try:
                    cutoff = _cutoff_for_index(ref_ts_naive, close_series.index)
                    close_up_to = close_series.loc[:cutoff]
                except Exception:
                    close_up_to = close_series
                if not close_up_to.empty:
                    last_close = float(close_up_to.iloc[-1])
            pct_from_high = (last_close / high_val - 1.0) * 100.0 if pd.notna(last_close) else float("nan")
            rows_52w.append(
                {
                    "symbol": sym,
                    "ticker": short_symbol(sym),
                    "fifty_two_week_high": float(high_val),
                    "fifty_two_week_high_date": high_dt.date().isoformat(),
                    "last_close": last_close,
                    "pct_from_high_52w": float(pct_from_high),
                }
            )
        fifty_two_week_high_df = pd.DataFrame(rows_52w).set_index("symbol") if rows_52w else pd.DataFrame()
        fifty_two_cache[fifty_two_key] = fifty_two_week_high_df

    fifty_two_low_key = (data_gen, str(start_current_week.date()))
    fifty_two_week_low_df = fifty_two_low_cache.get(fifty_two_low_key)
    if fifty_two_week_low_df is None:
        rows_52w_low: List[dict] = []
        for sym in symbols:
            df_sym = ohlcv.get(sym)
            low_val, low_dt = _fifty_two_week_low_mark(df_sym, ref_date=ref_ts_naive)
            if low_val is None or low_dt is None:
                continue
            close_series = closes.get(sym)
            last_close = float("nan")
            if close_series is not None and not close_series.empty:
                try:
                    cutoff = _cutoff_for_index(ref_ts_naive, close_series.index)
                    close_up_to = close_series.loc[:cutoff]
                except Exception:
                    close_up_to = close_series
                if not close_up_to.empty:
                    last_close = float(close_up_to.iloc[-1])
            pct_from_low = (last_close / low_val - 1.0) * 100.0 if pd.notna(last_close) else float("nan")
            rows_52w_low.append(
                {
                    "symbol": sym,
                    "ticker": short_symbol(sym),
                    "fifty_two_week_low": float(low_val),
                    "fifty_two_week_low_date": low_dt.date().isoformat(),
                    "last_close": last_close,
                    "pct_from_low_52w": float(pct_from_low),
                }
            )
        fifty_two_week_low_df = pd.DataFrame(rows_52w_low).set_index("symbol") if rows_52w_low else pd.DataFrame()
        fifty_two_low_cache[fifty_two_low_key] = fifty_two_week_low_df

    # Per-symbol breadth flags (for Snapshot)
    breadth_symbols_cache = st.session_state.setdefault("breadth_symbols_cache", {})
    window_bars = _breadth_window_bars(str(data_bundle.get("tf_label") or "Daily"))
    breadth_symbols_key = (data_gen, tuple(symbols), str(ref_ts_naive.date()), int(window_bars))
    breadth_symbols_df = breadth_symbols_cache.get(breadth_symbols_key)
    if (
        breadth_symbols_df is None
        or (isinstance(breadth_symbols_df, pd.DataFrame) and breadth_symbols_df.empty)
    ):
        breadth_symbols_df = _compute_breadth_symbol_flags(
            closes=closes,
            ohlcv=ohlcv,
            asof=pd.Timestamp(ref_ts),
            window_bars=window_bars,
        )
        breadth_symbols_cache[breadth_symbols_key] = breadth_symbols_df

    tab_overview, tab_chart, tab_table, tab_breadth, tab_volume, tab_errors = st.tabs(
        [
            "Overview" if lang == "en" else "ภาพรวม",
            _t("main_tab_rrg", lang=lang),
            _t("main_tab_snapshot", lang=lang),
            _t("main_tab_breadth", lang=lang),
            _t("main_tab_volume", lang=lang),
            _t("main_tab_errors", lang=lang),
        ]
    )

    with tab_overview:
        _render_section_head(
            "Dashboard Overview" if lang == "en" else "ภาพรวมแดชบอร์ด",
            "Quick operational summary and data readiness." if lang == "en" else "สรุปสถานะข้อมูลและความพร้อมของระบบแบบรวดเร็ว",
        )
        merged_errors = {**data_errors, **compute_errors}
        computed_count = 0 if table_is_empty else int(len(table))
        dr_universe = sum(1 for s in symbols if is_dr_symbol(str(s)))
        dr_computed = 0 if table_is_empty else sum(1 for s in table.index if is_dr_symbol(str(s)))
        overview_labels = (
            {
                "universe": "Universe",
                "loaded": "Loaded",
                "ready": "RRG Ready",
                "dr_coverage": "DR Coverage",
                "errors": "Errors",
            }
            if lang == "en"
            else {
                "universe": "Universe",
                "loaded": "Loaded",
                "ready": "RRG Ready",
                "dr_coverage": "DR Coverage",
                "errors": "Errors",
            }
        )

        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric(overview_labels["universe"], len(symbols))
        m2.metric(overview_labels["loaded"], len(closes))
        m3.metric(overview_labels["ready"], computed_count)
        m4.metric(overview_labels["dr_coverage"], f"{dr_computed}/{dr_universe}")
        m5.metric(overview_labels["errors"], len(merged_errors))

        left, right = st.columns([2.4, 1.6])
        with left:
            _render_section_head(
                "Top RRG Signals" if lang == "en" else "สัญญาณ RRG เด่น",
                "Ordered by selected metric and active filters." if lang == "en" else "เรียงตาม metric และตัวกรองที่เลือกในปัจจุบัน",
            )
            if display_table is None or display_table.empty:
                st.info(_t("no_symbols_filter", lang=lang))
            else:
                signal_cols = ["symbol", "label", "quadrant", "rs_ratio", "rs_mom", "distance", "speed", "date"]
                df_signal = display_table.reset_index()
                for c in signal_cols:
                    if c not in df_signal.columns:
                        df_signal[c] = pd.NA
                st.dataframe(
                    df_signal[signal_cols].head(15),
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "rs_ratio": st.column_config.NumberColumn(format="%.2f"),
                        "rs_mom": st.column_config.NumberColumn(format="%.2f"),
                        "distance": st.column_config.NumberColumn(format="%.2f"),
                        "speed": st.column_config.NumberColumn(format="%.2f"),
                    },
                )

        with right:
            _render_section_head(
                "Run Health" if lang == "en" else "สุขภาพระบบ",
                "Data and compute checkpoints for this session." if lang == "en" else "จุดตรวจสอบสถานะข้อมูลและการคำนวณในรอบนี้",
            )
            health_rows = pd.DataFrame(
                [
                    {"check": "Data source" if lang == "en" else "แหล่งข้อมูล", "value": "TradingView"},
                    {"check": "As of" if lang == "en" else "อ้างอิงถึง", "value": str(data_bundle.get("asof") or "-")},
                    {"check": "Benchmark", "value": str(data_bundle.get("benchmark") or "-")},
                    {"check": "Timeframe", "value": str(data_bundle.get("tf_label") or "-")},
                    {"check": "Bars", "value": str(data_bundle.get("bars") or "-")},
                    {"check": "Fetch errors", "value": str(len(data_errors))},
                    {"check": "Compute errors", "value": str(len(compute_errors))},
                ]
            )
            st.dataframe(health_rows, use_container_width=True, hide_index=True)
            if merged_errors:
                st.caption(
                    "Open Errors tab for full details." if lang == "en" else "ดูรายละเอียดทั้งหมดได้ที่แท็บ Errors"
                )

    with tab_chart:
        _render_section_head(
            "RRG Analysis" if lang == "en" else "วิเคราะห์ RRG",
            "Quadrant rotation view with configurable model and ranking controls."
            if lang == "en"
            else "กราฟ Quadrant rotation พร้อมตั้งค่าโมเดลและตัวกรองการจัดอันดับ",
        )
        st.session_state.setdefault("rrg_settings_hidden", False)
        hide_settings = bool(st.session_state.get("rrg_settings_hidden"))

        header_left, header_right = st.columns([3, 1])
        with header_left:
            st.markdown("### RRG")
        with header_right:
            st.toggle(_t("hide_settings", lang=lang), key="rrg_settings_hidden")

        model_labels = {
            "ema": _t("model_ema", lang=lang),
            "3m_high": _t("model_3m", lang=lang),
            "52w_high": _t("model_52wh", lang=lang),
            "52w_low": _t("model_52wl", lang=lang),
        }

        if hide_settings:
            if model_id == "ema":
                st.caption(
                    _t(
                        "rrg_summary_ema",
                        lang=lang,
                        model=model_labels.get(model_id, model_id),
                        ratio=ratio_len,
                        mom=mom_len,
                        tail=tail_len,
                    )
                )
            else:
                st.caption(
                    _t(
                        "rrg_summary_window",
                        lang=lang,
                        model=model_labels.get(model_id, model_id),
                        lookback=mom_len,
                        tail=tail_len,
                    )
                )
        else:
            st.markdown(f"#### {_t('settings', lang=lang)}")
            model_choice = st.selectbox(
                _t("model", lang=lang),
                options=["ema", "3m_high", "52w_high", "52w_low"],
                key="rrg_model_id",
                format_func=lambda x: model_labels.get(str(x), str(x)),
            )
            if model_choice == "ema":
                if data_tf == "Weekly":
                    st.slider(_t("rs_ratio_ema", lang=lang), min_value=5, max_value=40, step=1, key="rrg_ratio_len")
                    st.slider(_t("rs_mom_ema", lang=lang), min_value=5, max_value=40, step=1, key="rrg_mom_len")
                    st.slider(_t("tail_len", lang=lang), min_value=5, max_value=30, step=1, key="rrg_tail_len")
                else:
                    st.slider(_t("rs_ratio_ema", lang=lang), min_value=20, max_value=120, step=1, key="rrg_ratio_len")
                    st.slider(_t("rs_mom_ema", lang=lang), min_value=10, max_value=120, step=1, key="rrg_mom_len")
                    st.slider(_t("tail_len", lang=lang), min_value=5, max_value=60, step=1, key="rrg_tail_len")
            elif model_choice == "3m_high":
                st.caption(_t("uses_prev_3m", lang=lang))
                if data_tf == "Weekly":
                    st.slider(_t("mom_lookback", lang=lang), min_value=1, max_value=12, step=1, key="rrg_mom_lookback")
                    st.slider(_t("tail_len", lang=lang), min_value=5, max_value=30, step=1, key="rrg_tail_len")
                else:
                    st.slider(_t("mom_lookback", lang=lang), min_value=1, max_value=60, step=1, key="rrg_mom_lookback")
                    st.slider(_t("tail_len", lang=lang), min_value=5, max_value=60, step=1, key="rrg_tail_len")
            else:
                st.caption(_t("uses_prev_52w", lang=lang))
                if data_tf == "Weekly":
                    st.slider(_t("mom_lookback", lang=lang), min_value=1, max_value=52, step=1, key="rrg_mom_lookback")
                    st.slider(_t("tail_len", lang=lang), min_value=5, max_value=30, step=1, key="rrg_tail_len")
                else:
                    st.slider(_t("mom_lookback", lang=lang), min_value=1, max_value=252, step=1, key="rrg_mom_lookback")
                    st.slider(_t("tail_len", lang=lang), min_value=5, max_value=60, step=1, key="rrg_tail_len")

        caption_placeholder = st.empty()
        fig_placeholder = st.empty()
        if display_table.empty or fig is None:
            msg_key = "no_symbols_computed" if table_is_empty else "no_symbols_filter"
            fig_placeholder.warning(_t(msg_key, lang=lang))
        else:
            fig_placeholder.plotly_chart(fig, use_container_width=True)
        caption_placeholder.caption(
            _t(
                "rrg_caption",
                lang=lang,
                bench=data_bundle["benchmark"],
                tf=data_bundle["tf_label"],
                asof=data_bundle["asof"],
                n=len(display_table),
            )
        )

    with tab_table:
        _render_section_head(
            "Snapshot & Screeners" if lang == "en" else "สรุปตารางและหน้าสแกน",
            "Sortable table with downloadable output and scan-ready symbol lists."
            if lang == "en"
            else "ตารางสรุปพร้อมดาวน์โหลด และรายการหุ้นที่เข้าเงื่อนไขสแกน",
        )
        view = display_table.reset_index().rename(columns={"label": "ticker"})
        if isinstance(three_month_high_df, pd.DataFrame) and not three_month_high_df.empty:
            extra_cols = three_month_high_df[
                ["three_month_high", "three_month_high_date", "last_close", "pct_from_high"]
            ].reset_index()
            view = view.merge(extra_cols, on="symbol", how="left")
        if isinstance(fifty_two_week_high_df, pd.DataFrame) and not fifty_two_week_high_df.empty:
            extra_cols_52w = fifty_two_week_high_df[
                ["fifty_two_week_high", "fifty_two_week_high_date", "pct_from_high_52w"]
            ].reset_index()
            view = view.merge(extra_cols_52w, on="symbol", how="left")
        if isinstance(fifty_two_week_low_df, pd.DataFrame) and not fifty_two_week_low_df.empty:
            extra_cols_52w_low = fifty_two_week_low_df[
                ["fifty_two_week_low", "fifty_two_week_low_date", "pct_from_low_52w"]
            ].reset_index()
            view = view.merge(extra_cols_52w_low, on="symbol", how="left")
        st.caption(
            "Operational snapshot for all computed symbols in the current run."
            if lang == "en"
            else "Operational snapshot for all computed symbols in the current run."
        )
        quad_series = view["quadrant"] if "quadrant" in view.columns else pd.Series(dtype=object)
        leading_count = int((quad_series == "Leading").sum()) if not quad_series.empty else 0
        improving_count = int((quad_series == "Improving").sum()) if not quad_series.empty else 0
        dr_row_count = (
            int(view["symbol"].apply(lambda s: is_dr_symbol(str(s))).sum())
            if "symbol" in view.columns and not view.empty
            else 0
        )
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Rows", int(len(view)))
        k2.metric("Leading", leading_count)
        k3.metric("Improving", improving_count)
        k4.metric("DR Rows", dr_row_count)
        st.dataframe(
            view,
            use_container_width=True,
            column_config={
                "rs_ratio": st.column_config.NumberColumn(format="%.2f"),
                "rs_mom": st.column_config.NumberColumn(format="%.2f"),
                "distance": st.column_config.NumberColumn(format="%.2f"),
                "speed": st.column_config.NumberColumn(format="%.2f"),
                "angle_deg": st.column_config.NumberColumn(format="%.1f"),
                "three_month_high": st.column_config.NumberColumn(format="%.2f"),
                "pct_from_high": st.column_config.NumberColumn(
                    format="%.2f",
                    help="Percent: (last_close / 3M_high - 1) * 100"
                    if lang == "en"
                    else "เปอร์เซ็นต์: (last_close / 3M_high - 1) * 100",
                ),
                "fifty_two_week_high": st.column_config.NumberColumn(format="%.2f"),
                "pct_from_high_52w": st.column_config.NumberColumn(
                    format="%.2f",
                    help="Percent: (last_close / 52W_high - 1) * 100"
                    if lang == "en"
                    else "เปอร์เซ็นต์: (last_close / 52W_high - 1) * 100",
                ),
                "fifty_two_week_low": st.column_config.NumberColumn(format="%.2f"),
                "pct_from_low_52w": st.column_config.NumberColumn(
                    format="%.2f",
                    help="Percent: (last_close / 52W_low - 1) * 100"
                    if lang == "en"
                    else "เปอร์เซ็นต์: (last_close / 52W_low - 1) * 100",
                ),
            },
            hide_index=True,
        )
        st.download_button(
            _t("download_csv", lang=lang),
            data=view.to_csv(index=False).encode("utf-8"),
            file_name=f"rrg_{str(data_bundle['tf_label']).lower()}.csv",
            mime="text/csv",
        )

        with st.expander("Market Breadth - Passing symbols" if lang == "en" else "Market Breadth - รายชื่อหุ้นที่เข้าเงื่อนไข", expanded=False):
            if breadth_symbols_df is None or not isinstance(breadth_symbols_df, pd.DataFrame) or breadth_symbols_df.empty:
                st.info("No breadth symbol flags available." if lang == "en" else "ไม่มีข้อมูลรายชื่อหุ้นจาก Market Breadth")
            else:
                scan_summary_specs = [
                    ("Recovering", "recovering"),
                    ("Early recovery", "early_recovery"),
                ]
                summary_rows = []
                for label, col in scan_summary_specs:
                    if col in breadth_symbols_df.columns:
                        matched = breadth_symbols_df[breadth_symbols_df[col].fillna(False) == True]  # noqa: E712
                    else:
                        matched = breadth_symbols_df.iloc[0:0]
                    tickers = sorted({str(t) for t in matched.get("ticker", pd.Series(dtype=str)).tolist() if str(t)})
                    summary_rows.append(
                        {
                            "scan": label,
                            "count": len(tickers),
                            "symbols": ", ".join(tickers) if tickers else "-",
                        }
                    )
                st.markdown("#### Scan summary" if lang == "en" else "#### สรุปผลสแกน")
                summary_df = pd.DataFrame(summary_rows)
                if not summary_df.empty:
                    summary_metrics = {row["scan"]: int(row["count"]) for row in summary_rows}
                    s1, s2 = st.columns(2)
                    s1.metric("Recovering", summary_metrics.get("Recovering", 0))
                    s2.metric("Early recovery", summary_metrics.get("Early recovery", 0))
                st.dataframe(summary_df, use_container_width=True, hide_index=True)

                tab_labels = [
                    "Recovering",
                    "Early recovery",
                ]
                modes = [
                    "recovering",
                    "early_recovery",
                ]
                tabs_b = st.tabs(tab_labels)

                for t, mode in zip(tabs_b, modes):
                    with t:
                        df_show = breadth_symbols_df.copy()
                        if mode == "ema20":
                            df_show = df_show[df_show["above_ema20"] == True]  # noqa: E712
                        elif mode == "ema50":
                            df_show = df_show[df_show["above_ema50"] == True]  # noqa: E712
                        elif mode == "ema200":
                            df_show = df_show[df_show["above_ema200"] == True]  # noqa: E712
                        elif mode == "between_ema200d_price_ema200w":
                            if "between_ema200d_price_ema200w" in df_show.columns:
                                df_show = df_show[df_show["between_ema200d_price_ema200w"].fillna(False) == True]  # noqa: E712
                            else:
                                df_show = df_show.iloc[0:0]
                        elif mode == "recovering":
                            if "recovering" in df_show.columns:
                                df_show = df_show[df_show["recovering"].fillna(False) == True]  # noqa: E712
                            else:
                                df_show = df_show.iloc[0:0]
                        elif mode == "between_ema50d_price_ema200d":
                            if "between_ema50d_price_ema200d" in df_show.columns:
                                df_show = df_show[df_show["between_ema50d_price_ema200d"].fillna(False) == True]  # noqa: E712
                            else:
                                df_show = df_show.iloc[0:0]
                        elif mode == "early_recovery":
                            if "early_recovery" in df_show.columns:
                                df_show = df_show[df_show["early_recovery"].fillna(False) == True]  # noqa: E712
                            else:
                                df_show = df_show.iloc[0:0]
                        elif mode == "new_high":
                            df_show = df_show[df_show["new_high"].fillna(False) == True]  # noqa: E712
                        elif mode == "new_low":
                            df_show = df_show[df_show["new_low"].fillna(False) == True]  # noqa: E712

                        if lang == "en":
                            st.caption(f"Symbols: {len(df_show)} | As of: {str(ref_ts_naive.date())}")
                        else:
                            st.caption(f"จำนวนหุ้น: {len(df_show)} | อ้างอิงถึง: {str(ref_ts_naive.date())}")
                        if lang == "en":
                            if mode == "ema20":
                                st.caption("Reference: pass if last_close > EMA20 (compare `last_close` vs `ema20_value`).")
                            elif mode == "ema50":
                                st.caption("Reference: pass if last_close > EMA50 (compare `last_close` vs `ema50_value`).")
                            elif mode == "ema200":
                                st.caption("Reference: pass if last_close > EMA200 (compare `last_close` vs `ema200_value`).")
                            elif mode == "between_ema200d_price_ema200w":
                                st.caption("Reference: pass if EMA200D < price < EMA200W.")
                            elif mode == "recovering":
                                st.caption("Reference: Recovering = EMA200D < price < EMA200W.")
                            elif mode == "between_ema50d_price_ema200d":
                                st.caption("Reference: pass if EMA50D < price < EMA200D.")
                            elif mode == "early_recovery":
                                st.caption("Reference: Early recovery = EMA50D < price < EMA200D.")
                            elif mode == "new_high":
                                st.caption(
                                    f"Reference: pass if `high_last` breaks above `prev_high_max` "
                                    f"(previous {window_bars}-bar max)."
                                )
                            elif mode == "new_low":
                                st.caption(
                                    f"Reference: pass if `low_last` breaks below `prev_low_min` "
                                    f"(previous {window_bars}-bar min)."
                                )
                        else:
                            if mode == "ema20":
                                st.caption("จุดอ้างอิง: ผ่านเกณฑ์เมื่อ last_close > EMA20 (เทียบ `last_close` กับ `ema20_value`)")
                            elif mode == "ema50":
                                st.caption("จุดอ้างอิง: ผ่านเกณฑ์เมื่อ last_close > EMA50 (เทียบ `last_close` กับ `ema50_value`)")
                            elif mode == "ema200":
                                st.caption("จุดอ้างอิง: ผ่านเกณฑ์เมื่อ last_close > EMA200 (เทียบ `last_close` กับ `ema200_value`)")
                            elif mode == "new_high":
                                st.caption(
                                    f"จุดอ้างอิง: ผ่านเกณฑ์เมื่อ `high_last` ทะลุ `prev_high_max` "
                                    f"(ค่าสูงสุด {window_bars} แท่งก่อนหน้า)"
                                )
                            elif mode == "new_low":
                                st.caption(
                                    f"จุดอ้างอิง: ผ่านเกณฑ์เมื่อ `low_last` หลุด `prev_low_min` "
                                    f"(ค่าต่ำสุด {window_bars} แท่งก่อนหน้า)"
                                )

                        df_show = df_show.reset_index().sort_values(["date", "ticker"], ascending=[False, True])

                        base_number_cols = {
                            "last_close": st.column_config.NumberColumn(format="%.2f"),
                            "ema20_value": st.column_config.NumberColumn(format="%.2f"),
                            "ema50_value": st.column_config.NumberColumn(format="%.2f"),
                            "ema200_value": st.column_config.NumberColumn(format="%.2f"),
                            "ema200w_value": st.column_config.NumberColumn(format="%.2f"),
                            "close_vs_ema20_pct": st.column_config.NumberColumn(format="%.2f"),
                            "close_vs_ema50_pct": st.column_config.NumberColumn(format="%.2f"),
                            "close_vs_ema200_pct": st.column_config.NumberColumn(format="%.2f"),
                            "close_vs_ema200w_pct": st.column_config.NumberColumn(format="%.2f"),
                            "high_last": st.column_config.NumberColumn(format="%.2f"),
                            "prev_high_max": st.column_config.NumberColumn(format="%.2f"),
                            "high_breakout_pct": st.column_config.NumberColumn(format="%.2f"),
                            "low_last": st.column_config.NumberColumn(format="%.2f"),
                            "prev_low_min": st.column_config.NumberColumn(format="%.2f"),
                            "low_breakout_pct": st.column_config.NumberColumn(format="%.2f"),
                        }

                        if mode == "ema20":
                            show_cols = [
                                "symbol",
                                "ticker",
                                "date",
                                "last_close",
                                "ema20_value",
                                "close_vs_ema20_pct",
                            ]
                        elif mode == "ema50":
                            show_cols = [
                                "symbol",
                                "ticker",
                                "date",
                                "last_close",
                                "ema50_value",
                                "close_vs_ema50_pct",
                            ]
                        elif mode == "ema200":
                            show_cols = [
                                "symbol",
                                "ticker",
                                "date",
                                "last_close",
                                "ema200_value",
                                "close_vs_ema200_pct",
                            ]
                        elif mode == "between_ema200d_price_ema200w":
                            show_cols = [
                                "symbol",
                                "ticker",
                                "date",
                                "last_close",
                                "ema200_value",
                                "ema200w_value",
                                "close_vs_ema200_pct",
                                "close_vs_ema200w_pct",
                                "between_ema200d_price_ema200w",
                            ]
                        elif mode == "recovering":
                            show_cols = [
                                "symbol",
                                "ticker",
                                "date",
                                "last_close",
                                "ema200_value",
                                "ema200w_value",
                                "close_vs_ema200_pct",
                                "close_vs_ema200w_pct",
                                "recovering",
                            ]
                        elif mode == "between_ema50d_price_ema200d":
                            show_cols = [
                                "symbol",
                                "ticker",
                                "date",
                                "last_close",
                                "ema50_value",
                                "ema200_value",
                                "close_vs_ema50_pct",
                                "close_vs_ema200_pct",
                                "between_ema50d_price_ema200d",
                            ]
                        elif mode == "early_recovery":
                            show_cols = [
                                "symbol",
                                "ticker",
                                "date",
                                "last_close",
                                "ema50_value",
                                "ema200_value",
                                "close_vs_ema50_pct",
                                "close_vs_ema200_pct",
                                "early_recovery",
                            ]
                        elif mode == "new_high":
                            show_cols = [
                                "symbol",
                                "ticker",
                                "high_date",
                                "high_last",
                                "prev_high_max",
                                "high_breakout_pct",
                                "last_close",
                                "date",
                            ]
                        elif mode == "new_low":
                            show_cols = [
                                "symbol",
                                "ticker",
                                "low_date",
                                "low_last",
                                "prev_low_min",
                                "low_breakout_pct",
                                "last_close",
                                "date",
                            ]
                        else:
                            show_cols = [
                                "symbol",
                                "ticker",
                                "date",
                                "last_close",
                                "above_ema20",
                                "ema20_value",
                                "close_vs_ema20_pct",
                                "above_ema50",
                                "ema50_value",
                                "close_vs_ema50_pct",
                                "above_ema200",
                                "ema200_value",
                                "close_vs_ema200_pct",
                                "ema200w_value",
                                "close_vs_ema200w_pct",
                                "between_ema200d_price_ema200w",
                                "recovering",
                                "between_ema50d_price_ema200d",
                                "early_recovery",
                                "new_high",
                                "high_date",
                                "high_last",
                                "prev_high_max",
                                "high_breakout_pct",
                                "new_low",
                                "low_date",
                                "low_last",
                                "prev_low_min",
                                "low_breakout_pct",
                            ]

                        for c in show_cols:
                            if c not in df_show.columns:
                                df_show[c] = pd.NA
                        df_show = df_show[show_cols]
                        st.dataframe(
                            df_show,
                            use_container_width=True,
                            column_config={
                                **base_number_cols,
                            },
                            hide_index=True,
                        )

        with st.expander(_t("snapshot_3m", lang=lang), expanded=False):
            if three_month_high_df is None or not isinstance(three_month_high_df, pd.DataFrame) or three_month_high_df.empty:
                st.info(
                    "No 3-month high points available (need OHLCV high data in the previous 3 full months)."
                    if lang == "en"
                    else "ไม่มีข้อมูล 3 เดือน - จุดสูงสุด (ต้องมีข้อมูล OHLCV high ใน 3 เดือนเต็มก่อนหน้า)"
                )
            else:
                st.dataframe(
                    three_month_high_df.reset_index().sort_values("pct_from_high", ascending=False),
                    use_container_width=True,
                    column_config={
                        "three_month_high": st.column_config.NumberColumn(format="%.2f"),
                        "pct_from_high": st.column_config.NumberColumn(
                            format="%.2f",
                            help="Percent: (last_close / 3M_high - 1) * 100"
                            if lang == "en"
                            else "เปอร์เซ็นต์: (last_close / 3M_high - 1) * 100",
                        ),
                    },
                    hide_index=True,
                )
        with st.expander(_t("snapshot_52wh", lang=lang), expanded=False):
            if (
                fifty_two_week_high_df is None
                or not isinstance(fifty_two_week_high_df, pd.DataFrame)
                or fifty_two_week_high_df.empty
            ):
                st.info(
                    "No 52-week high points available (need OHLCV high data in the previous 52 full weeks)."
                    if lang == "en"
                    else "ไม่มีข้อมูล 52 สัปดาห์ - จุดสูงสุด (ต้องมีข้อมูล OHLCV high ใน 52 สัปดาห์เต็มก่อนหน้า)"
                )
            else:
                st.dataframe(
                    fifty_two_week_high_df.reset_index().sort_values("pct_from_high_52w", ascending=False),
                    use_container_width=True,
                    column_config={
                        "fifty_two_week_high": st.column_config.NumberColumn(format="%.2f"),
                        "pct_from_high_52w": st.column_config.NumberColumn(
                            format="%.2f",
                            help="Percent: (last_close / 52W_high - 1) * 100"
                            if lang == "en"
                            else "เปอร์เซ็นต์: (last_close / 52W_high - 1) * 100",
                        ),
                    },
                    hide_index=True,
                )
        with st.expander(_t("snapshot_52wl", lang=lang), expanded=False):
            if fifty_two_week_low_df is None or not isinstance(fifty_two_week_low_df, pd.DataFrame) or fifty_two_week_low_df.empty:
                st.info(
                    "No 52-week low points available (need OHLCV low data in the previous 52 full weeks)."
                    if lang == "en"
                    else "ไม่มีข้อมูล 52 สัปดาห์ - จุดต่ำสุด (ต้องมีข้อมูล OHLCV low ใน 52 สัปดาห์เต็มก่อนหน้า)"
                )
            else:
                st.dataframe(
                    fifty_two_week_low_df.reset_index().sort_values("pct_from_low_52w", ascending=False),
                    use_container_width=True,
                    column_config={
                        "fifty_two_week_low": st.column_config.NumberColumn(format="%.2f"),
                        "pct_from_low_52w": st.column_config.NumberColumn(
                            format="%.2f",
                            help="Percent: (last_close / 52W_low - 1) * 100"
                            if lang == "en"
                            else "เปอร์เซ็นต์: (last_close / 52W_low - 1) * 100",
                        ),
                    },
                    hide_index=True,
                )

    with tab_breadth:
        _render_section_head(
            "Market Breadth Overview" if lang == "en" else "ภาพรวม Market Breadth",
            "Cross-sectional health metrics from the active symbol universe."
            if lang == "en"
            else "วัดสุขภาพเชิงกว้างของกลุ่มหุ้นที่เลือกในรอบปัจจุบัน",
        )
        st.markdown(f"### {_t('breadth_title', lang=lang)}")
        lookback_days = int(animation_window) if animation_window else 90
        st.caption(_t("breadth_caption", lang=lang, days=lookback_days))

        breadth_cache = st.session_state.setdefault("breadth_cache", {})
        window_bars = _breadth_window_bars(str(data_bundle.get("tf_label") or "Daily"))
        breadth_key = (data_gen, tuple(symbols), str(data_bundle.get("tf_label")), int(window_bars))
        cached_breadth = breadth_cache.get(breadth_key)
        if cached_breadth:
            ema_breadth, hl_breadth = cached_breadth
        else:
            ema_breadth = _compute_breadth_above_ema(closes=closes, spans=[20, 50, 200])
            hl_breadth = _compute_breadth_new_high_low(ohlcv=ohlcv, window_bars=window_bars)
            breadth_cache[breadth_key] = (ema_breadth, hl_breadth)

        ref_breadth = pd.Timestamp(ref_ts)
        cutoff = ref_breadth - pd.Timedelta(days=lookback_days)
        ema_view = None
        hl_view = None
        if ema_breadth is not None and not ema_breadth.empty:
            cutoff_ema = _cutoff_for_index(cutoff, ema_breadth.index)
            ema_view = ema_breadth.loc[ema_breadth.index >= cutoff_ema]
        if hl_breadth is not None and not hl_breadth.empty:
            cutoff_hl = _cutoff_for_index(cutoff, hl_breadth.index)
            hl_view = hl_breadth.loc[hl_breadth.index >= cutoff_hl]

        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown(f"#### {_t('breadth_ema_title', lang=lang)}")
            if ema_view is None or ema_view.empty:
                st.info(_t("not_enough_ema", lang=lang))
            else:
                st.caption(f"Observations: {len(ema_view)}")
                ema_last_rows = ema_view.dropna(how="all")
                last = ema_last_rows.iloc[-1] if not ema_last_rows.empty else pd.Series(dtype=float)
                st.metric(_t("metric_above_ema20", lang=lang), f"{last.get('ema_20', float('nan')):.1f}")
                st.metric(_t("metric_above_ema50", lang=lang), f"{last.get('ema_50', float('nan')):.1f}")
                st.metric(_t("metric_above_ema200", lang=lang), f"{last.get('ema_200', float('nan')):.1f}")
                fig_ema = go.Figure()
                fig_ema.add_trace(go.Scatter(x=ema_view.index, y=ema_view["ema_20"], name="EMA20", mode="lines"))
                fig_ema.add_trace(go.Scatter(x=ema_view.index, y=ema_view["ema_50"], name="EMA50", mode="lines"))
                fig_ema.add_trace(go.Scatter(x=ema_view.index, y=ema_view["ema_200"], name="EMA200", mode="lines"))
                fig_ema.update_layout(
                    height=420,
                    margin=dict(l=10, r=10, t=30, b=10),
                    yaxis_title="Percent of symbols above EMA" if lang == "en" else "เปอร์เซ็นต์หุ้นที่อยู่เหนือ EMA",
                    xaxis_title="Date" if lang == "en" else "วันที่",
                    template="plotly_dark" if theme in {"vivid", "dark"} else "plotly_white",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
                )
                st.plotly_chart(fig_ema, use_container_width=True)

        with col_b:
            st.markdown(_t("breadth_hl_title", lang=lang, bars=window_bars))
            if hl_view is None or hl_view.empty:
                st.info(_t("not_enough_hl", lang=lang))
            else:
                st.caption(f"Observations: {len(hl_view)}")
                hl_last_rows = hl_view.dropna(how="all")
                if hl_last_rows.empty:
                    last_hl = pd.Series({"new_high_count": 0.0, "new_low_count": 0.0, "new_high_minus_low": 0.0})
                else:
                    last_hl = hl_last_rows.iloc[-1]
                st.metric(_t("metric_new_highs", lang=lang), f"{int(last_hl.get('new_high_count', 0))}")
                st.metric(_t("metric_new_lows", lang=lang), f"{int(last_hl.get('new_low_count', 0))}")
                st.metric(_t("metric_highs_minus_lows", lang=lang), f"{int(last_hl.get('new_high_minus_low', 0))}")
                fig_hl = go.Figure()
                fig_hl.add_trace(
                    go.Bar(
                        x=hl_view.index,
                        y=hl_view["new_high_count"],
                        name="New highs" if lang == "en" else "ทำจุดสูงสุดใหม่",
                    )
                )
                fig_hl.add_trace(
                    go.Bar(
                        x=hl_view.index,
                        y=(-hl_view["new_low_count"].astype(float)),
                        name="New lows (negative)" if lang == "en" else "ทำจุดต่ำสุดใหม่ (ติดลบ)",
                    )
                )
                fig_hl.add_trace(
                    go.Scatter(
                        x=hl_view.index,
                        y=hl_view["new_high_minus_low"],
                        name="Highs - lows (net)" if lang == "en" else "สูง-ต่ำ (สุทธิ)",
                        mode="lines",
                        line=dict(width=2, dash="dot"),
                    )
                )
                fig_hl.update_layout(
                    height=420,
                    margin=dict(l=10, r=10, t=30, b=10),
                    yaxis_title="Count" if lang == "en" else "จำนวน",
                    xaxis_title="Date" if lang == "en" else "วันที่",
                    template="plotly_dark" if theme in {"vivid", "dark"} else "plotly_white",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
                    barmode="relative",
                )
                st.plotly_chart(fig_hl, use_container_width=True)

        st.markdown("---")
        _render_section_head(
            "Recovery Scan Dashboard" if lang == "en" else "Recovery Scan Dashboard",
            "Visual focus on Recovering and Early recovery setups."
            if lang == "en"
            else "Visual focus on Recovering and Early recovery setups.",
        )
        if breadth_symbols_df is None or not isinstance(breadth_symbols_df, pd.DataFrame) or breadth_symbols_df.empty:
            st.info("No recovery scan data available.")
        else:
            rec_df = (
                breadth_symbols_df[breadth_symbols_df["recovering"].fillna(False) == True]  # noqa: E712
                if "recovering" in breadth_symbols_df.columns
                else breadth_symbols_df.iloc[0:0]
            )
            early_df = (
                breadth_symbols_df[breadth_symbols_df["early_recovery"].fillna(False) == True]  # noqa: E712
                if "early_recovery" in breadth_symbols_df.columns
                else breadth_symbols_df.iloc[0:0]
            )
            rec_symbols = sorted({str(v) for v in rec_df.get("ticker", pd.Series(dtype=str)).tolist() if str(v)})
            early_symbols = sorted({str(v) for v in early_df.get("ticker", pd.Series(dtype=str)).tolist() if str(v)})
            overlap = len(set(rec_symbols) & set(early_symbols))

            c1, c2, c3 = st.columns(3)
            c1.metric("Recovering", len(rec_symbols))
            c2.metric("Early recovery", len(early_symbols))
            c3.metric("Overlap", overlap)

            fig_recovery = go.Figure(
                data=[
                    go.Bar(
                        x=["Recovering", "Early recovery"],
                        y=[len(rec_symbols), len(early_symbols)],
                        marker_color=["#38bdf8", "#22c55e"],
                    )
                ]
            )
            fig_recovery.update_layout(
                height=320,
                margin=dict(l=10, r=10, t=25, b=10),
                yaxis_title="Count",
                template="plotly_dark" if theme in {"vivid", "dark"} else "plotly_white",
                showlegend=False,
            )
            st.plotly_chart(fig_recovery, use_container_width=True)

            left_scan, right_scan = st.columns(2)
            with left_scan:
                st.markdown("#### Recovering")
                if rec_df.empty:
                    st.info("No symbols")
                else:
                    show_cols = ["symbol", "ticker", "date", "last_close", "ema200_value", "ema200w_value"]
                    for col in show_cols:
                        if col not in rec_df.columns:
                            rec_df[col] = pd.NA
                    st.dataframe(
                        rec_df.reset_index(drop=True)[show_cols].sort_values(["date", "ticker"], ascending=[False, True]),
                        use_container_width=True,
                        hide_index=True,
                    )
            with right_scan:
                st.markdown("#### Early recovery")
                if early_df.empty:
                    st.info("No symbols")
                else:
                    show_cols = ["symbol", "ticker", "date", "last_close", "ema50_value", "ema200_value"]
                    for col in show_cols:
                        if col not in early_df.columns:
                            early_df[col] = pd.NA
                    st.dataframe(
                        early_df.reset_index(drop=True)[show_cols].sort_values(["date", "ticker"], ascending=[False, True]),
                        use_container_width=True,
                        hide_index=True,
                    )

    with tab_volume:
        _render_section_head(
            "Volume Breakout Monitor" if lang == "en" else "ติดตาม Volume Breakout",
            "Live and backfill volume-break conditions in one workspace."
            if lang == "en"
            else "ติดตามสัญญาณปริมาณซื้อขายแบบสดและย้อนหลังในหน้าจอเดียว",
        )
        render_volume_breakout(lang=lang)


    with tab_errors:
        _render_section_head(
            "Operational Errors" if lang == "en" else "ข้อผิดพลาดของระบบ",
            "Data fetch and compute exceptions for quick diagnostics."
            if lang == "en"
            else "แสดงปัญหาการดึงข้อมูลและคำนวณเพื่อช่วยวิเคราะห์ได้รวดเร็ว",
        )
        merged_errors = {**data_errors, **compute_errors}
        if not merged_errors:
            st.success(_t("no_errors", lang=lang))
        else:
            st.warning(_t("errors_count", lang=lang, n=len(merged_errors)))
            st.write(merged_errors)


if __name__ == "__main__":
    main()

