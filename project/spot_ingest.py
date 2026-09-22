"""spot_ingest.py

Ingest Polymarket "Current price" feed from RTDS WebSocket into ClickHouse.

This runs independently from market orderbook ingest and keeps crypto spot
ticks available for price_to_beat capture and model calculations.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import quote, urlencode

import certifi
import urllib3
import websockets
from dotenv import load_dotenv


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, "password.env"), override=False)
load_dotenv(os.path.join(BASE_DIR, ".env"), override=False)


SPOT_WS_URL = os.getenv("SPOT_WS_URL", "wss://ws-live-data.polymarket.com")
SPOT_TOPIC = os.getenv("SPOT_TOPIC", "crypto_prices_chainlink").strip()
SPOT_TABLE = os.getenv("SPOT_TABLE", "crypto_spot").strip()
SPOT_FLUSH_EVERY_N = int(os.getenv("SPOT_FLUSH_EVERY_N", "200"))
SPOT_FLUSH_EVERY_SEC = float(os.getenv("SPOT_FLUSH_EVERY_SEC", "2"))
SPOT_DEBUG_RAW = os.getenv("SPOT_DEBUG_RAW", "false").lower() in ("1", "true", "yes")

CH_HOST = os.getenv("CLICKHOUSE_HOST", "89.169.153.71")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "remote_ingest")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD", "")
CH_DB = os.getenv("CLICKHOUSE_DB", "polyk")
CH_HTTP_INTERFACE = os.getenv("CLICKHOUSE_HTTP_INTERFACE", "http")
CH_HTTP_HOST = os.getenv("CLICKHOUSE_HTTP_HOST", CH_HOST)
CH_HTTP_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", str(CH_PORT)))
CH_INSERT_RETRIES = int(os.getenv("CH_INSERT_RETRIES", "3"))
CH_INSERT_RETRY_BACKOFF_SEC = float(os.getenv("CH_INSERT_RETRY_BACKOFF_SEC", "0.5"))

if CH_USER == "remote_ingest" and not CH_PASS:
    raise RuntimeError("CLICKHOUSE_PASSWORD is empty. Set it in password.env or .env")

POLY_INSECURE_SSL = os.getenv("POLY_INSECURE_SSL", "false").lower() in ("1", "true", "yes")
_http = urllib3.PoolManager(
    cert_reqs="CERT_NONE" if POLY_INSECURE_SSL else "CERT_REQUIRED",
    ca_certs=None if POLY_INSECURE_SSL else certifi.where(),
)


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


def now_msk_naive() -> datetime:
    return (datetime.now(timezone.utc) + timedelta(hours=3)).replace(tzinfo=None)


def to_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return int(x)
        if isinstance(x, str) and x.strip():
            return int(float(x.strip()))
    except Exception:
        pass
    return default


def to_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        if isinstance(x, str) and x.strip():
            return float(x.strip())
    except Exception:
        pass
    return default


def normalize_messages(raw: Union[Dict[str, Any], List[Any]]) -> List[Dict[str, Any]]:
    if isinstance(raw, dict):
        return [raw]
    if isinstance(raw, list):
        out: List[Dict[str, Any]] = []
        for x in raw:
            if isinstance(x, dict):
                out.append(x)
            elif isinstance(x, list):
                out.extend([y for y in x if isinstance(y, dict)])
        return out
    return []


def ch_http_command(sql: str) -> None:
    params = {
        "database": CH_DB,
        "query": sql,
        "user": CH_USER,
        "password": CH_PASS,
    }
    url = f"{CH_HTTP_INTERFACE}://{CH_HTTP_HOST}:{CH_HTTP_PORT}/?" + urlencode(params, quote_via=quote)
    resp = _http.request(
        "POST",
        url,
        body=b"",
        headers={"Content-Type": "text/plain"},
        timeout=30.0,
        retries=False,
    )
    if resp.status >= 300:
        raise RuntimeError(f"HTTP COMMAND failed {resp.status}: {resp.data.decode('utf-8', errors='replace')}")


def ch_http_insert_json_each_row(table: str, columns: List[str], rows: List[Tuple]) -> None:
    if not rows:
        return

    dict_rows: List[Dict[str, Any]] = []
    for r in rows:
        d = {columns[i]: r[i] for i in range(len(columns))}
        for k, v in list(d.items()):
            if isinstance(v, datetime):
                d[k] = v.strftime("%Y-%m-%d %H:%M:%S")
        dict_rows.append(d)

    body = "\n".join(json.dumps(d, ensure_ascii=False, separators=(",", ":")) for d in dict_rows) + "\n"
    insert_sql = f"INSERT INTO {table} ({', '.join(columns)}) FORMAT JSONEachRow"

    params = {
        "database": CH_DB,
        "query": insert_sql,
        "user": CH_USER,
        "password": CH_PASS,
    }
    url = f"{CH_HTTP_INTERFACE}://{CH_HTTP_HOST}:{CH_HTTP_PORT}/?" + urlencode(params, quote_via=quote)
    attempts = max(1, CH_INSERT_RETRIES)
    last_exc: Optional[Exception] = None

    for attempt in range(1, attempts + 1):
        try:
            resp = _http.request(
                "POST",
                url,
                body=body.encode("utf-8"),
                headers={"Content-Type": "application/json"},
                timeout=30.0,
                retries=False,
            )
            if resp.status >= 300:
                raise RuntimeError(
                    f"HTTP INSERT failed {resp.status}: {resp.data.decode('utf-8', errors='replace')}"
                )
            return
        except Exception as e:
            last_exc = e
            if attempt >= attempts:
                break
            sleep_sec = max(0.0, CH_INSERT_RETRY_BACKOFF_SEC) * attempt
            if sleep_sec > 0:
                time.sleep(sleep_sec)

    if last_exc is not None:
        raise last_exc


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
    try:
        ch_http_command(f"ALTER TABLE {CH_DB}.{SPOT_TABLE} DROP COLUMN event_time_msk")
    except Exception:
        pass
    try:
        ch_http_command(
            f"ALTER TABLE {CH_DB}.{SPOT_TABLE} "
            f"ADD COLUMN event_time_msk DateTime MATERIALIZED {msk_expr}"
        )
    except Exception:
        pass


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


def build_subscribe_messages() -> List[Dict[str, Any]]:
    return [
        {
            "action": "subscribe",
            "subscriptions": [_subscription_for_symbol(symbol)],
        }
        for symbol in SPOT_SYMBOLS
    ]


def build_subscribe_message() -> Dict[str, Any]:
    """Legacy batch subscribe payload, kept for quick local inspection."""
    return {
        "action": "subscribe",
        "subscriptions": [_subscription_for_symbol(symbol) for symbol in SPOT_SYMBOLS],
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
            ch_http_insert_json_each_row(f"{CH_DB}.{SPOT_TABLE}", cols, rows)
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

                async for message in ws:
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
    ensure_table()
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
    asyncio.run(ingest_forever())
