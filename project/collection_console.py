"""Report each acknowledged database batch without printing market values."""
from __future__ import annotations

import os
import json
import sys
import threading
from collections import Counter
from datetime import datetime

from collection_config import SYMBOLS

CONSOLE_SYMBOL_KEY = "_console_symbol"


def market_symbol(value):
    """Resolve a configured ticker, spot pair or market slug, never a price."""
    if not isinstance(value, str):
        return None
    symbol = value.removeprefix("metadata:").split("/", 1)[0].split("-", 1)[0].upper()
    return symbol if symbol in SYMBOLS else None


class ConsoleReporter:
    def __init__(self, service, *, emit=None, enabled=None):
        self.service = {"orderbooks": "СТАКАН", "prices": "СПОТ"}.get(service, service)
        self.emit = emit or self.write_line
        self.enabled = enabled if enabled is not None else os.getenv("COLLECTOR_LOG_ENABLED", "true").lower() not in {"0", "false", "no"}
        self.total = Counter()
        self.market_symbols = {}
        self.connection_symbols = {}
        self.errors = set()
        self.lock = threading.Lock()

    @staticmethod
    def write_line(line):
        sys.stdout.write(line + "\n")
        sys.stdout.flush()

    def line(self, text, *, symbol=None):
        if not self.enabled:
            return
        try:
            label = f"{self.service} {symbol}" if symbol else self.service
            self.emit(f"[{datetime.now().astimezone().strftime('%H:%M:%S')}] [{label}] {text}")
        except (OSError, UnicodeError):
            self.enabled = False

    def error(self, key, active=True):
        symbol = market_symbol(key)
        with self.lock:
            had_errors = any(market_symbol(cause) == symbol for cause in self.errors)
            if active:
                self.errors.add(key)
            else:
                self.errors.discard(key)
            has_errors = any(market_symbol(cause) == symbol for cause in self.errors)
        if has_errors and not had_errors:
            self.line("Есть ошибки.", symbol=symbol)
        elif had_errors and not has_errors:
            self.line("Работа восстановлена.", symbol=symbol)

    def row_symbol(self, row):
        symbol = next((symbol for key in (CONSOLE_SYMBOL_KEY, "symbol", "stream", "slug", "market")
                       if (symbol := market_symbol(row.get(key)))), None)
        # Read older outbox records too: their market/connection can be learned
        # from metadata and raw events already acknowledged in this process.
        if symbol is None and row.get("service"):
            try:
                details = json.loads(row.get("details", "{}"))
            except (TypeError, ValueError):
                details = {}
            if isinstance(details, dict):
                symbol = market_symbol(details.get("stream")) or market_symbol(details.get("slug"))
        connection = (row.get("run_id"), row.get("connection_id"))
        market = row.get("market")
        if symbol:
            if market:
                self.market_symbols[market] = symbol
            if connection[1] not in (None, "log", "metadata"):
                self.connection_symbols[connection] = symbol
            # Console attribution must not grow indefinitely on long runs.
            for cache in (self.market_symbols, self.connection_symbols):
                if len(cache) > 4096:
                    cache.pop(next(iter(cache)))
        return symbol or self.market_symbols.get(market) or self.connection_symbols.get(connection)

    def confirmed(self, destination, rows):
        if not self.enabled or not rows:
            return
        # Called by the uploader only after ClickHouse acknowledges the insert
        # and the corresponding outbox jobs are removed. A batch can mix coins.
        table = destination.rsplit(".", 1)[-1]
        with self.lock:
            counts = Counter(self.row_symbol(row) for row in rows)
            updates = []
            for symbol, count in counts.items():
                key = (destination, symbol)
                self.total[key] += count
                updates.append((symbol, count, self.total[key]))
        for symbol, count, total in updates:
            if symbol is None and not table.endswith("collector_log_v2"):
                symbol = "РЫНОК НЕ ОПРЕДЕЛЁН"
            self.line(f"{table} | записано строк: {count} | всего за запуск: {total}", symbol=symbol)
