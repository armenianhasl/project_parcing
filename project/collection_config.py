"""Per-symbol switches shared by the launcher and both collectors."""
from __future__ import annotations

import os

SYMBOLS = ("BTC", "ETH", "SOL", "XRP")


def enabled_symbols(service):
    if service not in {"orderbooks", "prices"}:
        raise ValueError("Unknown collector service")
    books = service == "orderbooks"
    legacy_key = "POLY_SYMBOLS" if books else "SPOT_SYMBOLS"
    suffix = "ORDERBOOK" if books else "SPOT"
    defaults = SYMBOLS if books else tuple(f"{s}/usd" for s in SYMBOLS)
    legacy = {s.strip().upper() for s in os.getenv(legacy_key, ",".join(defaults)).split(",") if s.strip()}
    if legacy - {s.upper() for s in defaults}:
        raise ValueError(f"{legacy_key}: supported symbols are BTC, ETH, SOL, XRP")
    selected = []
    for symbol, legacy_symbol in zip(SYMBOLS, defaults):
        key = f"{symbol}_{suffix}_ENABLED"
        value = os.getenv(key)
        if value is None:
            enabled = legacy_symbol.upper() in legacy
        else:
            value = value.strip().lower()
            if value not in {"true", "false"}:
                raise ValueError(f"{key}: expected true or false")
            enabled = value == "true"
        if enabled:
            selected.append(symbol.lower() if books else f"{symbol.lower()}/usd")
    if books and selected and os.getenv("POLY_TIMEFRAMES", "15m").strip().lower() != "15m":
        raise ValueError("Only 15m orderbooks are supported")
    return selected
