"""Durable outbox and append-only v2 records. No changes to v1 tables."""
from __future__ import annotations

import fcntl
import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import queue
import re
import sqlite3
import threading
import time
import uuid
from pathlib import Path

import ingest_common as common
from collection_console import CONSOLE_SYMBOL_KEY, ConsoleReporter

BASE_COLUMNS = {
    "event_id": "String", "run_id": "String", "connection_id": "String",
    "seq": "UInt64", "event_index": "UInt32", "row_index": "UInt32",
    "recv_ts_ns": "UInt64", "event_ts_ms": "UInt64",
}
SCHEMAS = {
    "raw_events_v2": {"stream": "String", "payload": "String"},
    "orderbook_v2": {"market": "String", "asset_id": "String", "hash": "String",
                     "bids": "String", "asks": "String", "reason": "String", "valid": "UInt8"},
    "pricechange_v2": {"market": "String", "asset_id": "String", "hash": "String",
                       "side": "String", "price": "String", "size": "String", "delta": "String"},
    "book_checks_v2": {"market": "String", "asset_id": "String", "kind": "String",
                       "best_bid": "String", "best_ask": "String", "valid": "UInt8", "details": "String"},
    "trades_v2": {"market": "String", "asset_id": "String", "price": "String", "size": "String",
                  "side": "String", "transaction_hash": "String", "fee_rate_bps": "String"},
    "crypto_prices_v2": {"symbol": "String", "topic": "String", "value": "String",
                         "full_accuracy_value": "String", "ws_ts_ms": "UInt64", "window_s": "UInt32"},
    "market_metadata_v2": {"market": "String", "slug": "String", "symbol": "String", "asset_id": "String",
                           "start_ts_ms": "UInt64", "end_ts_ms": "UInt64", "twap_seconds": "UInt32",
                           "closed": "UInt8", "resolved": "UInt8", "payload": "String"},
    "collector_log_v2": {"service": "String", "level": "String", "event": "String", "details": "String"},
}


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def context(run_id, connection_id, seq, recv_ns=None, event_index=0, event_ts_ms=0):
    return dict(run_id=run_id, connection_id=connection_id, seq=seq,
                recv_ts_ns=recv_ns or time.time_ns(), event_index=event_index, event_ts_ms=event_ts_ms)


def record(table, ctx, row_index=0, **fields):
    row = {**ctx, "row_index": row_index, **fields}
    key = [table, row["run_id"], row["connection_id"], row["seq"], row["event_index"], row_index]
    row["event_id"] = hashlib.sha256(encode(key).encode()).hexdigest()
    return table, row


def table_name(table):
    prefix = os.getenv("COLLECTOR_TABLE_PREFIX", "")
    name = prefix + table
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError("Invalid COLLECTOR_TABLE_PREFIX")
    return f"{common.CH_DB}.{name}"


def ensure_tables():
    for table, fields in SCHEMAS.items():
        columns = ",\n".join(f"{k} {v}" for k, v in {**BASE_COLUMNS, **fields}.items())
        common.ch_http_command(f"CREATE TABLE IF NOT EXISTS {table_name(table)} ({columns}) "
                               "ENGINE = ReplacingMergeTree ORDER BY (run_id, connection_id, seq, event_index, row_index, event_id)")


def diagnostic_logger(service, directory):
    logger = logging.Logger(f"collector.{service}")
    handler = RotatingFileHandler(Path(directory) / f"{service}-errors.log", maxBytes=2_000_000,
                                  backupCount=2, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    return logger


class DurableSink:
    """Ingestion never waits for ClickHouse. Two threads persist and upload batches.

    Graceful close persists everything accepted by submit before draining the
    outbox. Unacknowledged inserts survive restart. Retry duplicates collapse
    by event_id; readers must use FINAL (or deduplicate by event_id).
    """
    def __init__(self, service, directory=None, insert=None, prepare=None):
        self.service = service
        self.run_id = uuid.uuid4().hex
        self.directory = Path(directory or os.getenv("COLLECTOR_STATE_DIR", str(common.BASE_DIR / ".collector")))
        self.directory.mkdir(parents=True, exist_ok=True)
        self.lock = (self.directory / f"{service}.lock").open("a")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.lock.close()
            raise RuntimeError(f"{service} уже запущен в этой папке") from None
        self.path = self.directory / f"{service}.sqlite3"
        self.insert = insert or common.ch_http_insert_json_each_row
        self.prepare = prepare
        self.queue = queue.Queue(maxsize=100000)
        self.log_queue = queue.SimpleQueue()
        self.fatal = None
        self.closing = threading.Event()
        self.persisted = threading.Event()
        self.wake = threading.Event()
        self.deadline = float("inf")
        self.log_seq = 0
        self.log_lock = threading.Lock()
        self.uploaded_rows = 0
        self.console = ConsoleReporter(service)
        self.diagnostics = diagnostic_logger(service, self.directory)
        db = self.connect()
        db.execute("CREATE TABLE IF NOT EXISTS outbox (id INTEGER PRIMARY KEY, destination TEXT NOT NULL, payload TEXT NOT NULL)")
        db.commit()
        db.close()
        self.writer = threading.Thread(target=self.persist_loop, daemon=True)
        self.uploader = threading.Thread(target=self.upload_loop, daemon=True)
        self.writer.start()
        self.uploader.start()

    def connect(self):
        db = sqlite3.connect(self.path, timeout=15)
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        return db

    def submit(self, records):
        self.check_open()
        if records:
            self.queue.put_nowait(records)

    def check_open(self):
        if self.fatal:
            raise RuntimeError("Local outbox failed") from self.fatal
        if self.closing.is_set():
            raise RuntimeError("Collector is closing")

    def log(self, event, level="INFO", **details):
        with self.log_lock:
            self.check_open()
            self.log_seq += 1
            if level == "ERROR":
                self.diagnostics.error("%s %s", event, encode(details))
            # Control records must still fit while market data is backpressured.
            self.log_queue.put([record("collector_log_v2", context(self.run_id, "log", self.log_seq),
                                       service=self.service, event=event, level=level, details=encode(details))])

    def persist_loop(self):
        db = None
        try:
            db = self.connect()
            while not self.closing.is_set() or not self.queue.empty() or not self.log_queue.empty():
                grouped = {}
                taken = 0
                data_taken = 0
                until = time.monotonic() + .25
                while taken < 1000 and time.monotonic() < until:
                    try:
                        bundle = self.log_queue.get_nowait()
                    except queue.Empty:
                        try:
                            bundle = self.queue.get(timeout=max(.001, until - time.monotonic()))
                            data_taken += 1
                        except queue.Empty:
                            break
                    taken += 1
                    for table, row in bundle:
                        grouped.setdefault(table_name(table), []).append(row)
                if not taken:
                    continue
                with db:
                    for destination, rows in grouped.items():
                        for start in range(0, len(rows), 5000):
                            db.execute("INSERT INTO outbox(destination,payload) VALUES (?,?)",
                                       (destination, encode(rows[start:start+5000])))
                for _ in range(data_taken):
                    self.queue.task_done()
                self.wake.set()
        except BaseException as exc:
            self.fatal = exc
        finally:
            if db is not None:
                db.close()
            self.persisted.set()
            self.wake.set()

    def upload_loop(self):
        db = None
        last_error = 0.0
        prepared = self.prepare is None
        retry_delay = .5
        try:
            db = self.connect()
            while time.monotonic() < self.deadline:
                if not prepared:
                    try:
                        self.prepare()
                        prepared = True
                        self.console.error("storage", False)
                    except Exception as exc:
                        self.console.error("storage")
                        if time.monotonic() - last_error > 10:
                            self.diagnostics.error("schema_error %r", exc)
                            last_error = time.monotonic()
                        self.closing.wait(retry_delay) if not self.closing.is_set() else time.sleep(.2)
                        retry_delay = min(10, retry_delay * 2)
                        continue
                job = db.execute("SELECT id,destination,payload FROM outbox ORDER BY id LIMIT 1").fetchone()
                if not job:
                    if self.persisted.is_set():
                        break
                    self.wake.wait(.2)
                    self.wake.clear()
                    continue
                key, destination, payload = job
                # Coalesce persisted jobs for one table. Tiny HTTP inserts cannot
                # keep up with four active books, even on a healthy database.
                candidates = db.execute("SELECT id,payload FROM outbox WHERE destination=? ORDER BY id LIMIT 200",
                                        (destination,)).fetchall()
                keys, rows, byte_count = [], [], 0
                for candidate_key, candidate_payload in candidates:
                    batch = json.loads(candidate_payload)
                    if rows and (len(rows) + len(batch) > 20000 or byte_count + len(candidate_payload) > 8000000):
                        break
                    keys.append(candidate_key)
                    rows.extend(batch)
                    byte_count += len(candidate_payload)
                # Attribution stays in the durable outbox for retries/restarts;
                # it is console metadata, not a column in ClickHouse.
                columns = [column for column in rows[0] if column != CONSOLE_SYMBOL_KEY]
                try:
                    self.insert(destination, columns, [tuple(r[k] for k in columns) for r in rows])
                    with db:
                        db.executemany("DELETE FROM outbox WHERE id=?", [(key,) for key in keys])
                    self.uploaded_rows += len(rows)
                    self.console.error("storage", False)
                    retry_delay = .5
                    self.console.confirmed(destination, rows)
                except Exception as exc:
                    self.console.error("storage")
                    if time.monotonic() - last_error > 10:
                        self.diagnostics.error("upload_error %r", exc)
                        last_error = time.monotonic()
                    self.closing.wait(retry_delay) if not self.closing.is_set() else time.sleep(min(.5, retry_delay))
                    retry_delay = min(10, retry_delay * 2)
        except BaseException as exc:
            self.fatal = exc
        finally:
            if db is not None:
                db.close()

    def close(self, timeout=40):
        self.closing.set()
        self.writer.join()
        self.deadline = time.monotonic() + timeout
        self.wake.set()
        self.uploader.join()
        try:
            if self.fatal:
                raise RuntimeError("Collector storage failed") from self.fatal
            db = self.connect()
            try:
                pending = db.execute("SELECT count() FROM outbox").fetchone()[0]
            finally:
                db.close()
        finally:
            fcntl.flock(self.lock, fcntl.LOCK_UN)
            self.lock.close()
            for handler in self.diagnostics.handlers:
                handler.close()
        self.console.line("Остановлен. Есть отложенные записи." if pending else "Остановлен.")
        return pending
