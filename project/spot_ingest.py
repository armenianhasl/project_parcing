"""spot_ingest.py

Ingest Polymarket "Current price" feed from RTDS WebSocket into ClickHouse.

Runs independently from market orderbook ingest, with one connection per symbol.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

import websockets
from ingest_common import (
    CH_DB, ch_http_command, ch_http_insert_json_each_row,
    normalize_messages, now_msk_naive, to_float, to_int, websocket_messages,
)


SPOT_WS_URL = os.getenv("SPOT_WS_URL", "wss://ws-live-data.polymarket.com")
SPOT_TOPIC = os.getenv("SPOT_TOPIC", "crypto_prices_chainlink").strip()
SPOT_TABLE = os.getenv("SPOT_TABLE", "crypto_spot").strip()
SPOT_FLUSH_EVERY_N = int(os.getenv("SPOT_FLUSH_EVERY_N", "200"))
SPOT_FLUSH_EVERY_SEC = float(os.getenv("SPOT_FLUSH_EVERY_SEC", "2"))
SPOT_DEBUG_RAW = os.getenv("SPOT_DEBUG_RAW", "false").lower() in ("1", "true", "yes")


def _parse_spot_symbols() -> List[str]:
    symbols = [
        s.strip().lower()
        for s in os.getenv("SPOT_SYMBOLS", "").split(",")
        if s.strip()
    ]
    if not symbols:
        legacy_symbol = os.getenv("SPOT_SYMBOL", "btc/usd").strip().lower()
        if legacy_symbol:
            symbols = [legacy_symbol]
    return list(dict.fromkeys(symbols))


SPOT_SYMBOLS = _parse_spot_symbols()
SPOT_SYMBOL_SET = set(SPOT_SYMBOLS)

if not SPOT_SYMBOLS:
    raise RuntimeError("Set SPOT_SYMBOLS or SPOT_SYMBOL in .env")


def ensure_table() -> None:
    msk_expr = "toDateTime(intDiv(event_ts_ms, 1000) + 10800)"
    q_spot = f"""
CREATE TABLE IF NOT EXISTS {CH_DB}.{SPOT_TABLE}
(
  ingest_time DateTime,
  symbol String,
  source_topic String,
  event_ts_ms UInt64,
  event_time_msk DateTime MATERIALIZED {msk_expr},
  ws_ts_ms UInt64,
  value Float64,
  full_accuracy_value String
)
ENGINE = MergeTree
ORDER BY (symbol, event_ts_ms)
"""
    ch_http_command(q_spot)


def _subscription_for_symbol(symbol: str) -> Dict[str, Any]:
    if SPOT_TOPIC == "crypto_prices_chainlink":
        return {
            "topic": SPOT_TOPIC,
            "type": "*",
            "filters": json.dumps({"symbol": symbol}, separators=(",", ":")),
        }
    return {
        "topic": SPOT_TOPIC,
        "type": "update",
        "filters": symbol,
    }


def build_subscribe_message_for_symbol(symbol: str) -> Dict[str, Any]:
    return {
        "action": "subscribe",
        "subscriptions": [_subscription_for_symbol(symbol)],
    }


def parse_spot_row(msg: Dict[str, Any]) -> Optional[Tuple]:
    if not isinstance(msg, dict):
        return None

    topic = str(msg.get("topic", "")).strip()
    if SPOT_TOPIC and topic and topic != SPOT_TOPIC:
        return None

    msg_type = str(msg.get("type", "")).strip().lower()
    if msg_type and msg_type != "update":
        return None

    payload = msg.get("payload")
    if not isinstance(payload, dict):
        return None

    symbol = str(payload.get("symbol", "")).strip().lower()
    if SPOT_SYMBOL_SET and symbol not in SPOT_SYMBOL_SET:
        return None

    value = to_float(payload.get("value"), default=float("nan"))
    if not math.isfinite(value):
        return None

    event_ts_ms = to_int(payload.get("timestamp"), 0)
    if event_ts_ms <= 0:
        event_ts_ms = to_int(msg.get("timestamp"), 0)
    if event_ts_ms <= 0:
        return None

    ws_ts_ms = max(0, to_int(msg.get("timestamp"), 0))
    full_accuracy_value = str(payload.get("full_accuracy_value") or "")

    return (
        now_msk_naive(),
        symbol,
        topic or SPOT_TOPIC,
        int(event_ts_ms),
        int(ws_ts_ms),
        float(value),
        full_accuracy_value,
    )


async def flusher_loop(buffer: List[Tuple], lock: asyncio.Lock, flush_event: asyncio.Event) -> None:
    while True:
        try:
            await asyncio.wait_for(flush_event.wait(), timeout=SPOT_FLUSH_EVERY_SEC)
        except asyncio.TimeoutError:
            pass

        async with lock:
            if not buffer:
                flush_event.clear()
                continue
            rows = buffer[:]
            buffer.clear()
            flush_event.clear()

        try:
            cols = [
                "ingest_time",
                "symbol",
                "source_topic",
                "event_ts_ms",
                "ws_ts_ms",
                "value",
                "full_accuracy_value",
            ]
            await asyncio.to_thread(ch_http_insert_json_each_row, f"{CH_DB}.{SPOT_TABLE}", cols, rows)
            counts = Counter(str(row[1]) for row in rows)
            counts_s = ", ".join(f"{sym}={cnt}" for sym, cnt in sorted(counts.items()))
            print(f"CH inserted {SPOT_TABLE}: {len(rows)} rows ({counts_s})")
        except Exception as e:
            print(f"CH INSERT ERROR {SPOT_TABLE}:", repr(e))
            async with lock:
                buffer[:0] = rows


async def ingest_symbol_loop(
    symbol: str,
    buffer: List[Tuple],
    lock: asyncio.Lock,
    flush_event: asyncio.Event,
) -> None:
    backoff = 1.0

    while True:
        try:
            async with websockets.connect(
                SPOT_WS_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                open_timeout=15,
                max_size=10 * 1024 * 1024,
            ) as ws:
                print(f"RTDS connected: {symbol}")
                await ws.send(
                    json.dumps(
                        build_subscribe_message_for_symbol(symbol),
                        separators=(",", ":"),
                    )
                )
                print(f"RTDS subscribed: {symbol}")

                async for message in websocket_messages(ws, ping_every=5.0):
                    if SPOT_DEBUG_RAW:
                        print(f"RAW {symbol}:", message[:400])
                    try:
                        raw = json.loads(message)
                    except Exception:
                        continue

                    for msg in normalize_messages(raw):
                        row = parse_spot_row(msg)
                        if row is None:
                            continue
                        async with lock:
                            buffer.append(row)
                            if len(buffer) >= SPOT_FLUSH_EVERY_N:
                                flush_event.set()

                backoff = 1.0
        except Exception as e:
            print(f"RTDS error {symbol}:", repr(e))
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)


async def ingest_forever() -> None:
    await asyncio.to_thread(ensure_table)
    print(f"[INIT] spot table ready: {CH_DB}.{SPOT_TABLE}")
    print(f"[INIT] ws={SPOT_WS_URL} topic={SPOT_TOPIC} symbols={','.join(SPOT_SYMBOLS)}")

    buffer: List[Tuple] = []
    lock = asyncio.Lock()
    flush_event = asyncio.Event()
    asyncio.create_task(flusher_loop(buffer, lock, flush_event))

    tasks = [
        asyncio.create_task(ingest_symbol_loop(symbol, buffer, lock, flush_event))
        for symbol in SPOT_SYMBOLS
    ]
    print(f"[INIT] started {len(tasks)} independent symbol websocket loops")
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    from collector_v2 import main
    raise SystemExit(main("prices"))
