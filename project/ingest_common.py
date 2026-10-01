"""Shared configuration, message parsing and ClickHouse HTTP writes."""

from __future__ import annotations

import asyncio
import gzip
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode

import certifi
import urllib3
from dotenv import load_dotenv
from websockets.exceptions import ConnectionClosedOK


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / "password.env", override=False)
load_dotenv(BASE_DIR / ".env", override=False)

CH_DB = os.getenv("CLICKHOUSE_DB", "polyk")
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD", "")
CH_HOST = os.getenv("CLICKHOUSE_HTTP_HOST", os.getenv("CLICKHOUSE_HOST", "localhost"))
CH_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", os.getenv("CLICKHOUSE_PORT", "8123")))
CH_INTERFACE = os.getenv("CLICKHOUSE_HTTP_INTERFACE", "http")
CH_INSERT_RETRIES = max(1, int(os.getenv("CH_INSERT_RETRIES", "3")))
CH_INSERT_RETRY_BACKOFF_SEC = max(0.0, float(os.getenv("CH_INSERT_RETRY_BACKOFF_SEC", "0.5")))

POLY_INSECURE_SSL = os.getenv("POLY_INSECURE_SSL", "false").lower() in ("1", "true", "yes")
http = urllib3.PoolManager(
    maxsize=8,
    cert_reqs="CERT_NONE" if POLY_INSECURE_SSL else "CERT_REQUIRED",
    ca_certs=None if POLY_INSECURE_SSL else certifi.where(),
)


def now_msk_naive() -> datetime:
    return (datetime.now(timezone.utc) + timedelta(hours=3)).replace(tzinfo=None)


def to_int(value, default: int = 0) -> int:
    try:
        return int(float(value)) if isinstance(value, str) else int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def to_float(value, default: float = 0.0) -> float:
    try:
        if isinstance(value, str):
            value = value.strip().replace(" ", "")
            value = value.replace(",", "" if "." in value else ".")
        return float(value)
    except (TypeError, ValueError, OverflowError):
        return default


def normalize_messages(raw) -> list[dict]:
    if isinstance(raw, dict):
        return [raw]
    if isinstance(raw, list):
        return [
            message
            for item in raw
            for message in (item if isinstance(item, list) else [item])
            if isinstance(message, dict)
        ]
    return []


async def websocket_messages(ws, *, ping_every: float, stop_event=None, idle_timeout=60):
    """Independent text heartbeats, responsive shutdown and a dead-peer watchdog."""
    async def heartbeat():
        while True:
            await asyncio.sleep(ping_every)
            await asyncio.wait_for(ws.send("PING"), timeout=10)

    ping_task = asyncio.create_task(heartbeat())
    stop_task = asyncio.create_task(stop_event.wait()) if stop_event is not None else None
    receiving = None
    try:
        while stop_event is None or not stop_event.is_set():
            receiving = asyncio.create_task(ws.recv())
            waiting = {receiving, ping_task}
            if stop_task is not None:
                waiting.add(stop_task)
            done, _ = await asyncio.wait(waiting, timeout=idle_timeout, return_when=asyncio.FIRST_COMPLETED)
            if ping_task in done:
                ping_task.result()
            if receiving not in done:
                if stop_task in done:
                    return
                raise TimeoutError("WebSocket peer is unresponsive")
            try:
                message = receiving.result()
            except ConnectionClosedOK:
                return
            control = message.strip().upper() if isinstance(message, (str, bytes)) else message
            if control in ("PONG", b"PONG"):
                continue
            if control in ("PING", b"PING"):
                await asyncio.wait_for(ws.send("PONG"), timeout=10)
                continue
            yield message
    finally:
        tasks = [task for task in (receiving, ping_task, stop_task) if task is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def _ch_request(sql: str, body: bytes = b"") -> None:
    if CH_USER == "remote_ingest" and not CH_PASS:
        raise RuntimeError("CLICKHOUSE_PASSWORD is empty. Set it in password.env or .env")
    params = urlencode({"database": CH_DB, "query": sql})
    headers = {
        "Content-Type": "text/plain; charset=utf-8",
        "X-ClickHouse-User": CH_USER,
        "X-ClickHouse-Key": CH_PASS,
        "Connection": "keep-alive",
    }
    if len(body) >= 1024 and os.getenv("CH_HTTP_COMPRESSION", "true").lower() not in {"0", "false", "no"}:
        body = gzip.compress(body, compresslevel=1, mtime=0)
        headers["Content-Encoding"] = "gzip"
    response = http.request(
        "POST",
        f"{CH_INTERFACE}://{CH_HOST}:{CH_PORT}/?{params}",
        body=body,
        headers=headers,
        timeout=float(os.getenv("CLICKHOUSE_TIMEOUT_SEC", "10")),
        retries=False,
    )
    if response.status >= 300:
        raise RuntimeError(
            f"ClickHouse HTTP {response.status}: {response.data.decode('utf-8', errors='replace')}"
        )


def ch_http_command(sql: str) -> None:
    _ch_request(sql)


def _json_default(value):
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    raise TypeError(f"Unsupported ClickHouse value: {type(value).__name__}")


def ch_http_insert_json_each_row(table: str, columns: list[str], rows: list[tuple]) -> None:
    if not rows:
        return
    body = (
        "\n".join(
            json.dumps(dict(zip(columns, row, strict=True)), ensure_ascii=False,
                       separators=(",", ":"), default=_json_default, allow_nan=False)
            for row in rows
        ) + "\n"
    ).encode("utf-8")
    sql = f"INSERT INTO {table} ({', '.join(columns)}) FORMAT JSONEachRow"
    for attempt in range(1, CH_INSERT_RETRIES + 1):
        try:
            _ch_request(sql, body)
            return
        except Exception:
            if attempt == CH_INSERT_RETRIES:
                raise
            time.sleep(CH_INSERT_RETRY_BACKOFF_SEC * attempt)
