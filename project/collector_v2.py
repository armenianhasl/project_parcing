"""UP collectors with exact prices, arrival order, pre-subscription and an outbox."""
from __future__ import annotations

import asyncio
import json
import os
import queue
import random
import sys
import signal
import time
import uuid
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlencode

import websockets
from websockets.exceptions import WebSocketException
from urllib3.util import Retry, Timeout

import ingest_common as common
from collection_book import Book, decimal, exact
from collection_config import enabled_symbols
from collection_console import CONSOLE_SYMBOL_KEY, ConsoleReporter, market_symbol
from collection_store import DurableSink, context, diagnostic_logger, encode, ensure_tables, record

TOPICS = {"crypto_prices_chainlink": 0, "crypto_prices_twap_thirty": 30, "crypto_prices_twap_sixty": 60}


async def delay(stop, seconds):
    try:
        await asyncio.wait_for(stop.wait(), timeout=max(.001, seconds))
    except asyncio.TimeoutError:
        pass


def timestamp(value):
    if not value:
        return 0
    return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() * 1000)


def fetch_slug(slug):
    endpoint = os.getenv("POLY_GAMMA_URL", "https://gamma-api.polymarket.com/markets")
    # Gamma defaults to closed=false. Explicitly check closed markets too so
    # resolution polling continues after a market disappears from active results.
    for closed in ("false", "true"):
        url = endpoint + ("&" if "?" in endpoint else "?") + urlencode({"slug": slug, "limit": 5, "closed": closed})
        response = common.http.request("GET", url, timeout=Timeout(connect=5, read=12),
                                       retries=Retry(total=2, backoff_factor=.5, allowed_methods={"GET"},
                                                     status_forcelist=(429, 500, 502, 503, 504),
                                                     respect_retry_after_header=False))
        if response.status != 200:
            raise RuntimeError(f"Gamma HTTP {response.status}")
        for market in json.loads(response.data):
            if market.get("slug") == slug:
                return market
    return None


def market_info(market):
    outcomes = market["outcomes"]
    tokens = market["clobTokenIds"]
    if isinstance(outcomes, str):
        outcomes = json.loads(outcomes)
    if isinstance(tokens, str):
        tokens = json.loads(tokens)
    labels = [str(x).lower() for x in outcomes]
    if len(tokens) != 2 or sorted(labels) != ["down", "up"]:
        raise ValueError("Explicit Up/Down token labels required")
    config = market.get("cryptoMarketConfig") or {}
    start = timestamp(market.get("eventStartTime"))
    end = timestamp(market.get("endDate"))
    if not start or end - start != 900000:
        raise ValueError("Expected a verified 15-minute interval")
    return {"market": market["conditionId"], "slug": market["slug"], "symbol": str(config.get("asset") or market["slug"].split("-")[0]).upper(),
            "asset_id": str(tokens[labels.index("up")]), "start_ts_ms": start, "end_ts_ms": end,
            "twap_seconds": int(config.get("twapLookbackSeconds") or 0) if config.get("twapEnabled") else 0}


def is_resolved(market):
    return str(market.get("umaResolutionStatus", "")).lower() == "resolved"


def price_subscriptions(symbol):
    return {"action": "subscribe", "subscriptions": [
        {"topic": topic, "type": "*" if topic == "crypto_prices_chainlink" else "update",
         "filters": encode({"symbol": symbol})} for topic in TOPICS
    ]}


def price_record(msg, symbol, ctx):
    topic = msg.get("topic", "")
    payload = msg.get("payload")
    if topic not in TOPICS or msg.get("type") != "update" or not isinstance(payload, dict) or payload.get("symbol") != symbol:
        return None
    ts = common.to_int(payload.get("timestamp"))
    if ts <= 0:
        raise ValueError("Price observation timestamp missing")
    full = str(payload.get("full_accuracy_value") or "")
    value = decimal(full) / Decimal(10**18) if full else decimal(payload.get("value"))
    if value <= 0:
        raise ValueError("Price must be positive")
    window = TOPICS[topic]
    if window and int(payload.get("window_s", window)) != window:
        raise ValueError("TWAP window differs from its topic")
    return record("crypto_prices_v2", {**ctx, "event_ts_ms": ts}, symbol=symbol, topic=topic,
                  value=exact(value), full_accuracy_value=full, ws_ts_ms=common.to_int(msg.get("timestamp")), window_s=window)


async def submit_records(sink, records):
    # Backpressure must wait for the disk writer, not discard a received frame.
    while True:
        try:
            sink.submit(records)
            return
        except queue.Full:
            await asyncio.sleep(.01)


def stream_error(sink, name, active=True):
    reporter = getattr(sink, "console", None)
    if reporter is not None:
        reporter.error(name, active)


async def finish_tasks(tasks):
    # A failed worker must not bypass draining the other workers before the
    # durable sink is closed.
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, BaseException):
            raise result


def book_payload_for_storage(raw):
    """Keep book headers in the raw log; levels live in snapshot/delta tables."""
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError):
        return raw
    changed = False

    def compact(value):
        nonlocal changed
        if isinstance(value, list):
            return [compact(item) for item in value]
        if isinstance(value, dict) and value.get("event_type") == "book":
            changed = True
            return {**{key: item for key, item in value.items() if key not in {"bids", "asks"}},
                    "snapshot_levels_omitted": True}
        return value

    compacted = compact(payload)
    return encode(compacted) if changed else raw


async def stream(sink, stop, *, stream_name, url, subscription, make_handler, ping_every):
    symbol = market_symbol(stream_name)
    backoff = 1
    gap_started_ns = 0
    connection = ""
    # Ordering must survive reconnects: subsequent connections use the one
    # initial snapshot of this market rather than saving another full book.
    received_seq = 0
    while not stop.is_set():
        connection = uuid.uuid4().hex
        handler = make_handler(connection)
        book = getattr(handler, "__self__", None)
        is_book = isinstance(book, Book)
        inbox = asyncio.Queue(maxsize=10000)
        receive_done = asyncio.Event()
        receive_stop = asyncio.Event()
        receiver_error = []
        recovery_error = []
        last_recv = 0
        connected_at = time.monotonic()
        usable = False
        sink.log("connecting", stream=stream_name, connection_id=connection)
        try:
            # Polymarket documents text PING/PONG. A second protocol-ping
            # watchdog caused false 1011 timeouts even while data kept arriving.
            async with websockets.connect(url, ping_interval=None, ping_timeout=None, open_timeout=15,
                                          close_timeout=3, max_size=10*1024*1024, max_queue=128) as ws:
                await asyncio.wait_for(ws.send(encode(subscription)), timeout=10)
                connected_at = time.monotonic()
                sink.log("subscribed", stream=stream_name, connection_id=connection)

                async def relay_stop():
                    await stop.wait()
                    receive_stop.set()

                async def receive():
                    nonlocal last_recv, received_seq
                    try:
                        async for raw in common.websocket_messages(ws, ping_every=ping_every, stop_event=receive_stop):
                            last_recv = time.time_ns()
                            received_seq += 1
                            if isinstance(raw, bytes):
                                raw = raw.decode("utf-8", errors="replace")
                            raw_ctx = context(sink.run_id, connection, received_seq, last_recv)
                            raw_ctx[CONSOLE_SYMBOL_KEY] = symbol
                            stored_raw = book_payload_for_storage(raw) if is_book else raw
                            await submit_records(sink, [record("raw_events_v2", raw_ctx, stream=stream_name, payload=stored_raw)])
                            await inbox.put((received_seq, last_recv, raw))
                            if received_seq % 100 == 0:
                                await asyncio.sleep(0)
                    except (OSError, TimeoutError, WebSocketException) as exc:
                        receiver_error.append(exc)
                    finally:
                        receive_done.set()

                async def process():
                    nonlocal gap_started_ns, usable
                    requested_at = None
                    last_topics = {topic: connected_at for topic in TOPICS}

                    async def request_snapshot(reason):
                        nonlocal requested_at
                        if is_book:
                            book.require_snapshot()
                        if requested_at is not None or receive_done.is_set() or receive_stop.is_set():
                            return
                        requested_at = time.monotonic()
                        if is_book:
                            request = {"assets_ids": subscription["assets_ids"], "operation": "subscribe",
                                       "custom_feature_enabled": True}
                        else:
                            request = subscription
                        sink.log("resync_requested", level="ERROR", stream=stream_name,
                                 connection_id=connection, reason=reason)
                        stream_error(sink, stream_name)
                        try:
                            await asyncio.wait_for(ws.send(encode(request)), timeout=10)
                        except (OSError, TimeoutError, WebSocketException) as exc:
                            recovery_error.append(exc)
                            receive_stop.set()

                    async def watchdog():
                        if receive_done.is_set() or receive_stop.is_set():
                            return
                        now = time.monotonic()
                        if requested_at is not None:
                            if now - requested_at >= 15:
                                recovery_error.append(TimeoutError("Subscription recovery timed out"))
                                receive_stop.set()
                        elif is_book and not book.ready and now - connected_at >= 15:
                            await request_snapshot("missing_initial_snapshot")
                        elif not is_book and any(now - ts >= 60 for ts in last_topics.values()):
                            await request_snapshot("stale_price_topic")

                    while not receive_done.is_set() or not inbox.empty():
                        try:
                            seq, recv_ns, raw = await asyncio.wait_for(inbox.get(), timeout=.25)
                        except asyncio.TimeoutError:
                            await watchdog()
                            continue
                        ctx = context(sink.run_id, connection, seq, recv_ns)
                        ctx[CONSOLE_SYMBOL_KEY] = symbol
                        # RTDS sends an empty text acknowledgement after subscribing.
                        if not raw.strip():
                            continue
                        try:
                            decoded = json.loads(raw)
                            if not isinstance(decoded, (dict, list)):
                                raise ValueError("Expected a WebSocket object or array")
                            messages = common.normalize_messages(decoded)
                        except (ValueError, TypeError) as exc:
                            sink.log("message_error", level="ERROR", stream=stream_name,
                                     connection_id=connection, seq=seq, error=repr(exc))
                            stream_error(sink, stream_name)
                            if is_book:
                                await request_snapshot("invalid_message")
                            continue
                        for index, msg in enumerate(messages):
                            event_ctx = {**ctx, "event_index": index, "event_ts_ms": max(0, common.to_int(msg.get("timestamp")))}
                            try:
                                records = handler(msg, event_ctx)
                            except (ValueError, TypeError, KeyError, ArithmeticError) as exc:
                                sink.log("message_error", level="ERROR", stream=stream_name,
                                         connection_id=connection, seq=seq, event_index=index, error=repr(exc))
                                stream_error(sink, stream_name)
                                if is_book:
                                    await request_snapshot("invalid_book_event")
                                continue
                            await submit_records(sink, records)
                            healthy = False
                            for table, row in records:
                                if table == "crypto_prices_v2":
                                    last_topics[row["topic"]] = time.monotonic()
                                    healthy = True
                                elif table == "orderbook_v2" and row["valid"]:
                                    healthy = True
                                elif table == "book_checks_v2":
                                    if row["kind"] == "snapshot" and row["valid"]:
                                        healthy = True
                                    if json.loads(row["details"]).get("resync_required"):
                                        await request_snapshot("inconsistent_book")
                            if healthy:
                                usable = True
                                recovered = is_book or (
                                    not is_book and requested_at is not None and all(ts > requested_at for ts in last_topics.values()))
                                if requested_at is not None and recovered:
                                    sink.log("resync_complete", stream=stream_name, connection_id=connection)
                                    requested_at = None
                                if requested_at is None:
                                    stream_error(sink, stream_name, False)
                                if gap_started_ns:
                                    sink.log("gap_end", stream=stream_name, connection_id=connection,
                                             last_receive_ns=gap_started_ns, first_new_receive_ns=recv_ns)
                                    gap_started_ns = 0
                        await watchdog()
                        await asyncio.sleep(0)

                receiver = asyncio.create_task(receive())
                consumer = asyncio.create_task(process())
                stopper = asyncio.create_task(relay_stop())
                try:
                    await asyncio.gather(receiver, consumer)
                    if not stop.is_set():
                        if receiver_error or recovery_error:
                            raise (receiver_error or recovery_error)[0]
                        raise ConnectionError("WebSocket closed before requested stop")
                finally:
                    for task in (receiver, consumer, stopper):
                        task.cancel()
                    await asyncio.gather(receiver, consumer, stopper, return_exceptions=True)
        except asyncio.CancelledError:
            raise
        except (OSError, TimeoutError, WebSocketException) as exc:
            if stop.is_set():
                break
            if usable and time.monotonic() - connected_at >= 30:
                backoff = 1
            event = "reconnect_error" if gap_started_ns else "gap_start"
            gap_started_ns = gap_started_ns or last_recv or time.time_ns()
            sink.log(event, level="ERROR", stream=stream_name, connection_id=connection,
                     last_receive_ns=gap_started_ns, error=repr(exc))
            stream_error(sink, stream_name)
            await delay(stop, backoff + random.uniform(0, backoff * .2))
            backoff = min(backoff * 2, 15)
    stream_error(sink, stream_name, False)
    sink.log("stream_stopped", stream=stream_name, connection_id=connection)


async def market_stream(sink, stop, info):
    book = Book(info["market"], info["asset_id"])

    def handler_factory(connection):
        book.start_connection()
        return book.process
    await stream(sink, stop, stream_name=info["slug"], url=os.getenv("POLY_WS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/market"),
                 subscription={"assets_ids": [info["asset_id"]], "type": "market", "custom_feature_enabled": True},
                 make_handler=handler_factory, ping_every=10)


async def price_stream(sink, stop, symbol):
    def factory(connection):
        def handler(msg, ctx):
            row = price_record(msg, symbol, ctx)
            return [row] if row else []
        return handler
    await stream(sink, stop, stream_name=symbol, url=os.getenv("SPOT_WS_URL", "wss://ws-live-data.polymarket.com"),
                 subscription=price_subscriptions(symbol), make_handler=factory, ping_every=5)


def should_stream(info, now, prefetch, grace):
    return info["start_ts_ms"] / 1000 - prefetch <= now < info["end_ts_ms"] / 1000 + grace


def load_pending_markets(state_file, sink):
    if not state_file.exists():
        return {}
    try:
        saved = json.loads(state_file.read_text())
        if not isinstance(saved, dict):
            raise ValueError("Expected a market cache object")
    except (ValueError, OSError) as exc:
        sink.log("metadata_state_error", level="ERROR", error=repr(exc))
        return {}
    cache = {}
    for slug, market in saved.items():
        try:
            info = market_info(market)
            if info["slug"] != slug:
                raise ValueError("Cache slug differs from market slug")
            cache[slug] = market
        except (ValueError, TypeError, KeyError) as exc:
            sink.log("metadata_state_error", level="ERROR", slug=slug, error=repr(exc))
    return cache


async def run_markets(sink, stop, symbols=None):
    symbols = enabled_symbols("orderbooks") if symbols is None else symbols
    prefetch = max(5, float(os.getenv("POLY_PREFETCH_SEC", "45")))
    grace = max(0, float(os.getenv("POLY_CLOSE_GRACE_SEC", "10")))
    state_file = sink.directory / "pending-markets.json"
    cache = load_pending_markets(state_file, sink)
    workers = {}
    fetched = {}
    emitted_metadata = set()
    completed = set()
    last_heartbeat = 0

    async def refresh():
        floor = int(time.time()) // 900 * 900
        current = {f"{symbol}-updown-15m-{floor}" for symbol in symbols}
        upcoming = {f"{symbol}-updown-15m-{floor+900}" for symbol in symbols}
        wanted = current | upcoming
        wanted.update(slug for slug, m in cache.items()
                      if not is_resolved(m) and market_info(m)["symbol"].lower() in symbols)
        sem = asyncio.Semaphore(4)

        async def one(slug):
            if time.monotonic() < fetched.get(slug, 0):
                return
            async with sem:
                if stop.is_set():
                    return
                try:
                    m = await asyncio.to_thread(fetch_slug, slug)
                    fetched[slug] = time.monotonic() + (30 if m else 5)
                    if m:
                        info = market_info(m)
                        previous = cache.get(slug)
                        cache[slug] = m
                        if previous != m or slug not in emitted_metadata:
                            sink.submit([record("market_metadata_v2", context(sink.run_id, "metadata", time.time_ns()),
                                                **info, closed=int(bool(m.get("closed"))), resolved=int(is_resolved(m)), payload=encode(m))])
                            emitted_metadata.add(slug)
                        stream_error(sink, f"metadata:{slug}", False)
                except Exception as exc:
                    fetched[slug] = time.monotonic() + 5
                    sink.log("metadata_error", level="ERROR", slug=slug, error=repr(exc))
                    stream_error(sink, f"metadata:{slug}")
        # Discovery of missing active markets must precede historical resolution.
        ordered = sorted(wanted, key=lambda slug: (0 if slug in current else 1 if slug in upcoming else 2, slug))
        await asyncio.gather(*(one(slug) for slug in ordered))
        pending = {slug: m for slug, m in cache.items() if not is_resolved(m)}
        temp = state_file.with_suffix(".tmp")
        try:
            await asyncio.to_thread(temp.write_text, encode(pending))
            await asyncio.to_thread(os.replace, temp, state_file)
            stream_error(sink, "metadata_state", False)
        except OSError as exc:
            sink.log("metadata_state_error", level="ERROR", error=repr(exc))
            stream_error(sink, "metadata_state")
        for slug, m in list(cache.items()):
            if is_resolved(m) and market_info(m)["end_ts_ms"] / 1000 + grace < time.time() and slug not in workers:
                cache.pop(slug)
                fetched.pop(slug, None)
                emitted_metadata.discard(slug)
                completed.discard(slug)

    # Discovery runs concurrently with subscription scheduling, including at boundaries.
    refresh_task = asyncio.create_task(refresh())
    try:
        while not stop.is_set():
            now = time.time()
            for slug, market in list(cache.items()):
                info = market_info(market)
                if info["symbol"].lower() not in symbols:
                    continue
                if should_stream(info, now, prefetch, grace) and slug not in workers and slug not in completed:
                    local_stop = asyncio.Event()
                    task = asyncio.create_task(market_stream(sink, local_stop, info))
                    workers[slug] = (task, local_stop, info)
                    sink.log("market_selected", **info, pre_open=now < info["start_ts_ms"]/1000)
            for slug, (task, local_stop, info) in list(workers.items()):
                if now >= info["end_ts_ms"]/1000 + grace:
                    local_stop.set()
                if task.done():
                    task.result()
                    completed.add(slug)
                    del workers[slug]
            if refresh_task.done():
                refresh_task.result()
                refresh_task = asyncio.create_task(refresh())
            if now - last_heartbeat >= 10:
                sink.log("heartbeat", streams=len(workers), memory_queue=sink.queue.qsize(), uploaded_rows=sink.uploaded_rows)
                last_heartbeat = now
            if sink.fatal:
                raise RuntimeError("Storage worker failed") from sink.fatal
            await delay(stop, 1)
    finally:
        for task, local_stop, _ in workers.values():
            local_stop.set()
        await finish_tasks([*(task for task, _, _ in workers.values()), refresh_task])


async def run_prices(sink, stop, symbols=None):
    symbols = enabled_symbols("prices") if symbols is None else symbols
    tasks = [asyncio.create_task(price_stream(sink, stop, symbol)) for symbol in symbols]
    try:
        while not stop.is_set():
            for task in tasks:
                if task.done():
                    task.result()
                    raise RuntimeError("Price stream stopped unexpectedly")
            if sink.fatal:
                raise RuntimeError("Storage worker failed") from sink.fatal
            sink.log("heartbeat", streams=len(tasks), memory_queue=sink.queue.qsize(), uploaded_rows=sink.uploaded_rows)
            await delay(stop, 10)
    finally:
        stop.set()
        await finish_tasks(tasks)


async def run(service):
    symbols = enabled_symbols(service)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    # A bounded run is useful for a smoke test; normal launcher runs until Ctrl+C.
    seconds = float(os.getenv("COLLECTOR_RUN_SECONDS", "0"))
    timer = loop.call_later(seconds, stop.set) if seconds > 0 else None
    if not symbols:
        reporter = ConsoleReporter(service)
        reporter.line("Парсинг выключен в .env.")
        try:
            await stop.wait()
        finally:
            if timer:
                timer.cancel()
        return
    sink = DurableSink(service, prepare=ensure_tables)
    try:
        sink.log("startup", format_version=2, symbols=symbols, book_storage="initial_snapshot_then_deltas")
        for symbol in symbols:
            sink.console.line("Запущен. Ожидаю записи данных.", symbol=market_symbol(symbol))
        if service == "orderbooks":
            await run_markets(sink, stop, symbols)
        else:
            await run_prices(sink, stop, symbols)
    finally:
        stop.set()
        if timer:
            timer.cancel()
        try:
            sink.log("shutdown_requested")
        finally:
            await asyncio.to_thread(sink.close)


def main(service):
    try:
        asyncio.run(run(service))
    except KeyboardInterrupt:
        return 0
    except Exception:
        directory = Path(os.getenv("COLLECTOR_STATE_DIR", str(common.BASE_DIR / ".collector")))
        directory.mkdir(parents=True, exist_ok=True)
        logger = diagnostic_logger(service, directory)
        logger.exception("collector_failed")
        for handler in logger.handlers:
            handler.close()
        ConsoleReporter(service).line("Есть ошибки. Процесс остановлен.")
        return 1
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in {"orderbooks", "prices"}:
        raise SystemExit("Usage: python collector_v2.py orderbooks|prices")
    raise SystemExit(main(sys.argv[1]))
