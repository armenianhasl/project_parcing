import asyncio
import json
import os
import sqlite3
import tempfile
import threading
import time
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, patch

from collection_book import Book
from collection_store import DurableSink, context, record
import collector_v2 as runtime


def ctx(seq=1):
    return context("run", "connection", seq, 1700000000000000000, event_ts_ms=1700000000000)


def snapshot(bids=None, asks=None):
    return dict(event_type="book", asset_id="up", market="market", hash="book",
                bids=[dict(price=p, size=s) for p, s in (bids if bids is not None else [("0.4", "10")])],
                asks=[dict(price=p, size=s) for p, s in (asks if asks is not None else [("0.6", "20")])])


def change(price="0.4", size="12", side="BUY", asset="up", **extra):
    return dict(event_type="price_change", market="market", price_changes=[
        dict(asset_id=asset, price=price, size=size, side=side, hash="delta", **extra)])


class BookTests(unittest.TestCase):
    def test_every_snapshot_removes_stale_levels(self):
        book = Book("market", "up")
        book.process(snapshot(), ctx())
        refreshed = book.process(snapshot(bids=[("0.7", "3")], asks=[("0.8", "4")]), ctx(2))
        self.assertFalse(any(table == "orderbook_v2" for table, _ in refreshed))
        self.assertEqual([(row['side'], row['price'], row['size'], row['delta'])
                          for table, row in refreshed if table == 'pricechange_v2'],
                         [('ask', '0.6', '0', '-20'), ('ask', '0.8', '4', '4'),
                          ('bid', '0.4', '0', '-10'), ('bid', '0.7', '3', '3')])
        book.process(change("0.75", "5"), ctx(3))
        self.assertEqual(book.best(), (Decimal("0.75"), Decimal("0.8")))
        self.assertNotIn(("ask", Decimal("0.6")), book.levels)

    def test_empty_snapshot_clears_both_sides(self):
        book = Book("market", "up")
        book.process(snapshot(), ctx())
        rows = book.process(snapshot([], []), ctx(2))
        self.assertEqual(book.best(), (None, None))
        self.assertFalse(any(table == 'orderbook_v2' for table, _ in rows))
        self.assertEqual({(row['side'], row['price'], row['size'], row['delta'])
                          for table, row in rows if table == 'pricechange_v2'},
                         {('bid', '0.4', '0', '-10'), ('ask', '0.6', '0', '-20')})

    def test_only_initial_snapshot_is_saved_even_when_it_is_empty(self):
        book = Book('market', 'up')
        first = book.process(snapshot([], []), ctx())
        self.assertEqual(sum(table == 'orderbook_v2' for table, _ in first), 1)
        self.assertEqual(json.loads(first[0][1]['bids']), [])
        added = book.process(snapshot(), ctx(2))
        self.assertEqual(sum(table == 'pricechange_v2' for table, _ in added), 2)
        for records in (added, book.process(snapshot(), ctx(3)), book.process(change(size='10'), ctx(4))):
            self.assertFalse(any(table == 'orderbook_v2' for table, _ in records))
        self.assertFalse(any(table == 'pricechange_v2' for table, _ in book.process(snapshot(), ctx(5))))
        self.assertFalse(any(table == 'pricechange_v2' for table, _ in book.process(change(size='10'), ctx(6))))

    def test_initial_snapshot_and_deltas_reconstruct_every_later_snapshot(self):
        book = Book('market', 'up')
        replayed = {}
        snapshot_count = 0
        source_books = [snapshot(), snapshot(),
                        snapshot([('0.4', '12.000000001'), ('0.5', '2')], [('0.6', '20')]),
                        snapshot([('0.5', '2')], [('0.5', '3')]), snapshot([], []), snapshot()]
        for seq, message in enumerate(source_books, 1):
            records = book.process(message, ctx(seq))
            for table, row in records:
                if table == 'orderbook_v2':
                    snapshot_count += 1
                    replayed = {(side, Decimal(price)): Decimal(size)
                                for side, key in [('bid', 'bids'), ('ask', 'asks')]
                                for price, size in json.loads(row[key])}
                elif table == 'pricechange_v2':
                    key = (row['side'], Decimal(row['price']))
                    size = Decimal(row['size'])
                    self.assertEqual(replayed.get(key, Decimal(0)) + Decimal(row['delta']), size)
                    if size:
                        replayed[key] = size
                    else:
                        replayed.pop(key, None)
                    self.assertNotEqual(Decimal(row['delta']), 0)
            expected = {(side, Decimal(level['price'])): Decimal(level['size'])
                        for side, key in [('bid', 'bids'), ('ask', 'asks')] for level in message[key]}
            self.assertEqual(replayed, expected)
        self.assertEqual(snapshot_count, 1)

    def test_compact_raw_header_cannot_be_mistaken_for_an_empty_book(self):
        book = Book('market', 'up')
        book.process(snapshot(), ctx())
        before = dict(book.levels)
        with self.assertRaisesRegex(ValueError, 'Raw book header has no levels'):
            book.process(dict(event_type='book', market='market', asset_id='up', snapshot_levels_omitted=True), ctx(2))
        self.assertEqual(book.levels, before)

    def test_missing_side_disagrees_with_populated_reported_quote(self):
        book = Book("market", "up")
        book.process(snapshot([], []), ctx())
        rows = book.process(change("0.4", "0", best_bid="0.4", best_ask="1"), ctx(2))
        self.assertFalse(rows[-1][1]["valid"])
        rows = book.process(change("0.4", "0", best_bid="0", best_ask="1"), ctx(3))
        self.assertTrue(rows[-1][1]["valid"])

    def test_prices_are_exact_and_sides_are_independent(self):
        book = Book("market", "up")
        book.process(snapshot([("0.12345", "10.123456789")], [("0.12345", "2"), ("1", "3")]), ctx())
        self.assertEqual(len(book.levels), 3)
        rows = book.process(change("0.12345", "10.123456790"), ctx(2))
        self.assertEqual(rows[0][1]["delta"], "0.000000001")
        self.assertEqual(rows[0][1]["price"], "0.12345")

    def test_changes_have_absolute_size_and_keep_same_ms_order(self):
        book = Book("market", "up")
        book.process(snapshot(), ctx())
        a = book.process(change(size="15"), ctx(2))[0][1]
        b = book.process(change(size="0"), ctx(3))[0][1]
        self.assertEqual((a["size"], a["delta"], b["size"], b["delta"]), ("15", "5", "0", "-15"))
        self.assertNotEqual(a["event_id"], b["event_id"])
        self.assertEqual(a["event_ts_ms"], b["event_ts_ms"])

    def test_down_is_filtered_even_in_mixed_payload(self):
        book = Book("market", "up")
        book.process(snapshot(), ctx())
        self.assertEqual(book.process(change(asset="down"), ctx(2)), [])
        self.assertEqual(book.levels[("bid", Decimal("0.4"))], 10)

    def test_pre_snapshot_changes_are_flagged_not_applied(self):
        book = Book("market", "up")
        rows = book.process(change(), ctx())
        self.assertEqual(rows[0][1]["kind"], "before_snapshot")
        self.assertFalse(rows[0][1]["valid"])
        self.assertEqual(book.levels, {})

    def test_crossed_book_is_marked_not_silently_cleaned(self):
        book = Book("market", "up")
        book.process(snapshot(), ctx())
        rows = book.process(change("0.7", "4"), ctx(2))
        self.assertEqual(book.best(), (Decimal("0.7"), Decimal("0.6")))
        self.assertFalse(rows[-1][1]["valid"])
        book.process(snapshot(), ctx(3))
        self.assertTrue(book.valid)

    def test_partial_update_recovers_when_following_delta_matches(self):
        book = Book("market", "up")
        book.process(snapshot(), ctx())
        rows = book.process(change(best_bid="0.5", best_ask="0.6"), ctx(2))
        self.assertTrue(json.loads(rows[-1][1]["details"])["bbo_mismatch"])
        self.assertFalse(book.valid)
        book.process(change("0.5", "8", best_bid="0.5", best_ask="0.6"), ctx(3))
        self.assertTrue(book.valid)

    def test_bbo_can_arrive_before_level_changes(self):
        book = Book("market", "up")
        book.process(snapshot(), ctx())
        rows = book.process(dict(event_type="best_bid_ask", asset_id="up", best_bid="0.5", best_ask="0.6"), ctx(2))
        self.assertTrue(book.valid)
        details = json.loads(rows[0][1]["details"])
        self.assertTrue(details["bbo_mismatch"])
        self.assertEqual(details["reported_bid"], "0.5")
        book.process(change("0.5", "8", best_bid="0.5", best_ask="0.6"), ctx(3))
        self.assertTrue(book.valid)

    def test_sustained_bad_book_requests_new_snapshot(self):
        book = Book("market", "up")
        book.process(snapshot(), ctx())
        book.process(change("0.7", "8"), ctx(2))
        later = {**ctx(3), "recv_ts_ns": ctx()["recv_ts_ns"] + 3_000_000_000}
        rows = book.process(change("0.7", "9"), later)
        self.assertTrue(json.loads(rows[-1][1]["details"])["resync_required"])
        book.process(snapshot(), ctx(4))
        self.assertEqual(book.bad_since_ns, 0)

    def test_wrong_market_is_rejected(self):
        book = Book("market", "up")
        with self.assertRaises(ValueError):
            book.process({**snapshot(), "market": "different"}, ctx())

    def test_unrelated_market_broadcast_is_ignored(self):
        book = Book("market", "up")
        self.assertEqual(book.process(dict(event_type="new_market", market="other", assets_ids=["x","y"]), ctx()), [])

    def test_invalid_message_cannot_partially_mutate_state(self):
        book = Book("market", "up")
        book.process(snapshot(), ctx())
        msg = change(size="99")
        msg["price_changes"] += change(price="NaN")["price_changes"]
        with self.assertRaises(ValueError):
            book.process(msg, ctx(2))
        self.assertEqual(book.levels[("bid", Decimal("0.4"))], 10)

    def test_trade_and_tick_messages_are_retained(self):
        book = Book("market", "up")
        trade = book.process(dict(event_type="last_trade_price", asset_id="up", price="0.4", size="3"), ctx())
        self.assertEqual(trade[0][0], "trades_v2")
        tick = book.process(dict(event_type="tick_size_change", asset_id="up", new_tick_size="0.0001"), ctx(2))
        self.assertEqual(tick[0][1]["kind"], "tick_size_change")


class FeedTests(unittest.TestCase):
    def test_raw_book_storage_omits_full_levels_and_preserves_mixed_message_order(self):
        messages = [snapshot(), [change(), dict(event_type='book', asset_id='down', bids='[]', asks='[]')]]
        raw = json.dumps(messages)
        saved = json.loads(runtime.book_payload_for_storage(raw))
        self.assertTrue(saved[0]['snapshot_levels_omitted'])
        self.assertTrue(saved[1][1]['snapshot_levels_omitted'])
        for message in (saved[0], saved[1][1]):
            self.assertNotIn('bids', message)
            self.assertNotIn('asks', message)
        self.assertEqual(saved[1][0], messages[1][0])
        self.assertIn('bids', messages[0])
        self.assertEqual(runtime.book_payload_for_storage('invalid json'), 'invalid json')
        raw_delta = json.dumps(change())
        self.assertEqual(runtime.book_payload_for_storage(raw_delta), raw_delta)

    def test_twap_subscriptions_use_exact_json_symbol_filters(self):
        subs = runtime.price_subscriptions("btc/usd")["subscriptions"]
        self.assertEqual({x["topic"] for x in subs}, set(runtime.TOPICS))
        self.assertTrue(all(x["filters"] == '{"symbol":"btc/usd"}' for x in subs))

    def test_twap_preserves_e18_precision(self):
        msg = dict(topic="crypto_prices_twap_sixty", type="update", timestamp=1700000000200,
                   payload=dict(symbol="btc/usd", timestamp=1700000000000, value=99999,
                                full_accuracy_value="65432123456789012345678", window_s=60))
        row = runtime.price_record(msg, "btc/usd", ctx())[1]
        self.assertEqual(row["value"], "65432.123456789012345678")
        self.assertEqual(row["window_s"], 60)
        self.assertIsNone(runtime.price_record(msg, "eth/usd", ctx()))

    def test_twap_rejects_wrong_window(self):
        msg = dict(topic="crypto_prices_twap_sixty", type="update", payload=dict(symbol="btc/usd", timestamp=1,value=10,window_s=30))
        with self.assertRaises(ValueError):
            runtime.price_record(msg, "btc/usd", ctx())

    def test_next_market_is_subscribed_before_boundary(self):
        info = dict(start_ts_ms=900000, end_ts_ms=1800000)
        self.assertFalse(runtime.should_stream(info, 854, 45, 10))
        self.assertTrue(runtime.should_stream(info, 855, 45, 10))
        self.assertTrue(runtime.should_stream(info, 900, 45, 10))
        self.assertFalse(runtime.should_stream(info, 1810, 45, 10))

    def test_up_token_is_selected_by_label_not_position(self):
        m = dict(conditionId="m",slug="btc-updown-15m-0",clobTokenIds='["d","u"]',outcomes='["Down","Up"]',
                 eventStartTime="2026-09-22T11:00:00Z",endDate="2026-09-22T11:15:00Z",cryptoMarketConfig=dict(asset="btc",twapEnabled=True,twapLookbackSeconds=60))
        info = runtime.market_info(m)
        self.assertEqual(info["asset_id"], "u")
        self.assertEqual(info["twap_seconds"], 60)
        m["outcomes"] = '[]'
        with self.assertRaises(ValueError):
            runtime.market_info(m)


class StorageTests(unittest.TestCase):
    def test_disk_worker_startup_failure_is_not_reported_as_success(self):
        class BrokenSink(DurableSink):
            def connect(self):
                if threading.current_thread() is not threading.main_thread():
                    raise OSError("disk unavailable")
                return super().connect()
        with tempfile.TemporaryDirectory() as directory:
            sink = BrokenSink("test", directory, insert=lambda *args:None)
            sink.writer.join(timeout=1)
            with self.assertRaisesRegex(RuntimeError,"storage failed"):
                sink.close(timeout=0)
            # Failure must also release the single-instance lock.
            fresh = DurableSink("test", directory, insert=lambda *args:None)
            self.assertEqual(fresh.close(), 0)

    def test_persisted_small_jobs_are_coalesced(self):
        sent = []
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "test.sqlite3")
            with sqlite3.connect(path) as db:
                db.execute("CREATE TABLE outbox (id INTEGER PRIMARY KEY, destination TEXT NOT NULL, payload TEXT NOT NULL)")
                for n in range(50):
                    row = record("raw_events_v2", ctx(n), stream="test", payload="{}")[1]
                    db.execute("INSERT INTO outbox(destination,payload) VALUES (?,?)", ("test.raw_events_v2",json.dumps([row])))
            db.close()
            sink = DurableSink("test", directory, insert=lambda table,cols,rows:sent.append(rows))
            self.assertEqual(sink.close(), 0)
        self.assertEqual(len(sent), 1)
        self.assertEqual(len(sent[0]), 50)

    def test_shutdown_flushes_all_accepted_records(self):
        sent = []
        with tempfile.TemporaryDirectory() as directory:
            sink = DurableSink("test", directory, insert=lambda table,cols,rows:sent.extend(dict(zip(cols,r)) for r in rows))
            for n in range(100):
                sink.submit([record("raw_events_v2", ctx(n), stream="test", payload="{}")])
            self.assertEqual(sink.close(), 0)
        self.assertEqual(len(sent), 100)
        self.assertEqual(len({r["event_id"] for r in sent}), 100)

    def test_failed_upload_survives_restart_with_same_event_id(self):
        def fail(*args):
            raise OSError("offline")
        sent = []
        with tempfile.TemporaryDirectory() as directory:
            first = DurableSink("test", directory, insert=fail)
            row = record("raw_events_v2", ctx(), stream="test", payload="{}")
            first.submit([row])
            self.assertGreater(first.close(timeout=0), 0)
            second = DurableSink("test", directory, insert=lambda table,cols,rows:sent.extend(dict(zip(cols,r)) for r in rows))
            self.assertEqual(second.close(), 0)
        self.assertEqual(sent[0]["event_id"], row[1]["event_id"])

    def test_duplicate_instance_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            first = DurableSink("test", directory, insert=lambda *args:None)
            try:
                with self.assertRaisesRegex(RuntimeError, "уже запущен"):
                    DurableSink("test", directory, insert=lambda *args:None)
            finally:
                first.close()

    def test_submission_does_not_wait_for_slow_database(self):
        def slow(*args):
            time.sleep(.4)
        with tempfile.TemporaryDirectory() as directory:
            sink = DurableSink("test", directory, insert=slow)
            start = time.monotonic()
            for n in range(50):
                sink.submit([record("raw_events_v2", ctx(n), stream="test", payload="{}")])
            elapsed = time.monotonic() - start
            sink.close()
        self.assertLess(elapsed, .2)


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_reconnect_saves_corrections_from_one_initial_snapshot(self):
        stop = asyncio.Event()
        output = []
        logs = []
        calls = 0
        class Sink:
            run_id = "run"
            service = "test"
            def log(self, event, **details): logs.append((event, details))
            def submit(self, rows): output.extend(rows)
        class Socket:
            send = AsyncMock()
            async def __aenter__(self): return self
            async def __aexit__(self, *args): pass
        async def messages(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                yield json.dumps(snapshot())
                yield json.dumps(change(size="99"))
                raise OSError("disconnected")
            yield json.dumps(change(size="77"))
            yield json.dumps(snapshot())
            yield json.dumps(change(size="12"))
            stop.set()
        async def no_wait(*args): pass
        with patch.object(runtime.websockets,"connect",return_value=Socket()), patch.object(runtime.common,"websocket_messages",messages), patch.object(runtime,"delay",no_wait):
            await runtime.market_stream(Sink(),stop,dict(market="market",asset_id="up",slug="btc-test"))
        snapshots = [r for t,r in output if t == "orderbook_v2"]
        self.assertEqual(len(snapshots), 1)
        deltas = [row for table, row in output if table == 'pricechange_v2']
        self.assertEqual([(row['size'], row['delta']) for row in deltas], [('99', '89'), ('10', '-89'), ('12', '2')])
        self.assertNotEqual(snapshots[0]['connection_id'], deltas[1]['connection_id'])
        self.assertEqual([row['seq'] for table, row in output if table == 'raw_events_v2'], [1, 2, 3, 4, 5])
        self.assertTrue(any(table == 'book_checks_v2' and json.loads(row['details']).get('reason') == 'reconnect'
                            for table, row in output))
        self.assertTrue(any(event == 'gap_end' for event, _ in logs))
        self.assertTrue(any(t=="book_checks_v2" and r["kind"]=="before_snapshot" for t,r in output))

    async def test_graceful_stop_drains_received_frames(self):
        stop = asyncio.Event()
        output = []
        class Sink:
            run_id = "run"
            service = "test"
            def log(self, *args, **kwargs): pass
            def submit(self, rows):output.extend(rows)
        class Socket:
            send = AsyncMock()
            async def __aenter__(self):return self
            async def __aexit__(self,*args):pass
        async def messages(*args, **kwargs):
            yield ""
            for n in range(10):
                yield json.dumps(dict(topic="crypto_prices_chainlink",type="update",payload=dict(symbol="btc/usd",timestamp=n+1,value=10)))
            stop.set()
        with patch.object(runtime.websockets,"connect",return_value=Socket()), patch.object(runtime.common,"websocket_messages",messages):
            await runtime.price_stream(Sink(),stop,"btc/usd")
        self.assertEqual(sum(t=="raw_events_v2" for t,r in output),11)
        self.assertEqual(sum(t=="crypto_prices_v2" for t,r in output),10)
        self.assertEqual([r['seq'] for t,r in output if t=='crypto_prices_v2'],list(range(2,12)))


if __name__ == "__main__":
    unittest.main()
