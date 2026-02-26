from __future__ import annotations

from volume_alert.config import AppConfig
from volume_alert.db import Database
from volume_alert.symbols import normalize_symbol, normalize_symbols


def bootstrap_symbols(*, db: Database, cfg: AppConfig) -> None:
    """Make sure the configured default symbols exist in the database."""
    rows = db.get_all_symbols()

    # Migrate malformed symbols (e.g. SET:"ADVANC") to canonical form.
    migrations: list[tuple[str, str, bool]] = []
    for row in rows:
        raw = str(row["symbol"] or "")
        enabled = bool(int(row["enabled"] or 0))
        normalized = normalize_symbol(raw, default_exchange=cfg.default_exchange_prefix)
        if not normalized:
            if enabled:
                db.set_symbol_enabled(raw, False)
            continue
        if normalized != raw:
            migrations.append((raw, normalized, enabled))

    if migrations:
        db.upsert_symbols([new for _, new, _ in migrations])
        for old, new, was_enabled in migrations:
            if was_enabled:
                db.set_symbol_enabled(new, True)
            db.set_symbol_enabled(old, False)
        rows = db.get_all_symbols()

    defaults = normalize_symbols(cfg.default_symbols, default_exchange=cfg.default_exchange_prefix)
    if not defaults:
        return

    existing = {str(row["symbol"]) for row in rows}
    missing = [sym for sym in defaults if sym not in existing]
    if missing:
        db.upsert_symbols(missing)
