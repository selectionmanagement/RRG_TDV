from __future__ import annotations

from dataclasses import dataclass, field
from datetime import time
from typing import List


_RRG_FALLBACK_SYMBOLS: tuple[str, ...] = tuple(
    line.strip()
    for line in """
        DELTA
        ADVANC
        PTT
        AOT
        GULF
        KBANK
        SCB
        PTTEP
        KTB
        CPALL
        TRUE
        BDMS
        BBL
        CPN
        THAI
        SCC
        TTB
        BAY
        CPAXT
        OR
        CPF
        BH
        MINT
        CRC
        TLI
        GPSC
        PTTGC
        IVL
        TISCO
        HMPRO
        BEM
        TOP
        MTC
        KTC
        SCGP
        RATCH
        AWC
        TFMAMA
        BJC
        EGCO
        TIDLOR
        KKP
        TCAP
        MRDIYT
        COM7
        CCET
        TU
        BANPU
        WHA
        OSP
        SAWAD
        ITC
        LH
        SCCC
        CBG
        CENTEL
        BTS
        BPP
        BGRIM
        SPI
        BCP
        TTW
        GLOBAL
        BTG
        JTS
        BLA
        BKIH
        SPALI
        BA
        MEGA
        TOA
        TFG
        AP
        BCH
        AEONTS
        SPRC
        MBK
        KCE
        STGT
        BAM
        VGI
        SIRI
        RCL
        TASCO
        RAM
        BCPG
        PB
        IRPC
        CREDIT
        CKP
        EA
        CK
        PLANB
        TVO
        AURA
        SPC
        STA
        VIBHA
        LHFG
        AMATA
    """.splitlines()
    if line.strip()
)


def _rrg_default_symbols() -> tuple[str, ...]:
    try:
        from rrg_bundle.app import AppConfig as RRGAppConfig
        from rrg_bundle.rrg.symbols import parse_symbol_list
    except Exception:  # pragma: no cover
        return _RRG_FALLBACK_SYMBOLS
    symbols = parse_symbol_list(RRGAppConfig.default_symbols)
    return tuple(symbols)


@dataclass(frozen=True)
class MarketSession:
    start: time
    end: time


@dataclass(frozen=True)
class AppConfig:
    timezone: str = "Asia/Bangkok"
    default_exchange_prefix: str = "SET"
    scan_interval_seconds: int = 60

    market_sessions: List[MarketSession] = field(
        default_factory=lambda: [
            MarketSession(start=time(10, 0), end=time(12, 30)),
            MarketSession(start=time(14, 30), end=time(16, 30)),
        ]
    )
    daily_close_report_time: time = time(16, 30)

    scanner_url: str = "https://scanner.tradingview.com/thailand/scan"
    scanner_timeout_seconds: int = 20
    scanner_batch_size: int = 200

    ws_url: str = "wss://data.tradingview.com/socket.io/websocket"
    ws_timeout_seconds: int = 20
    avg_history_bars: int = 120
    avg_windows: tuple[int, int, int, int] = (5, 10, 20, 50)

    # Worker concurrency for historical average refresh.
    avg_refresh_workers: int = 6

    # Default symbols shared with the RRG dashboard; this pulls the list from
    # rrg_bundle.app.AppConfig.default_symbols so it automatically stays in sync.
    default_symbols: tuple[str, ...] = field(default_factory=_rrg_default_symbols)


DEFAULT_CONFIG = AppConfig()
