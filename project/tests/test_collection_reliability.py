"""Regression tests for switches, continuous reception, recovery and both entrypoints."""
import asyncio
import json
import os
import queue
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse
from unittest.mock import AsyncMock, patch

import collector_v2 as runtime
import ingest_common as common
from collection_book import Book
from collection_config import SYMBOLS, enabled_symbols
from collection_console import ConsoleReporter
from collection_store import DurableSink, context, record

ROOT = Path(__file__).resolve().parents[1]


def flags(value='false'):
    return {f'{symbol}_{kind}_ENABLED': value for symbol in SYMBOLS for kind in ('ORDERBOOK', 'SPOT')}


def book_event():
    return dict(event_type='book', market='market', asset_id='up', bids=[dict(price='0.4', size='10')],
                asks=[dict(price='0.6', size='20')])


def price_event(n=1, value='10', topic='crypto_prices_chainlink'):
    return dict(topic=topic, type='update', payload=dict(symbol='btc/usd', timestamp=n, value=value))


class SwitchTests(unittest.TestCase):
    def test_flags_select_books_and_prices_independently(self):
        env = {**flags(), 'BTC_ORDERBOOK_ENABLED': 'true', 'ETH_SPOT_ENABLED': 'true'}
        with patch.dict(os.environ, env, clear=True):
            self.assertEqual(enabled_symbols('orderbooks'), ['btc'])
            self.assertEqual(enabled_symbols('prices'), ['eth/usd'])

    def test_explicit_true_overrides_legacy_exclusion(self):
        env = {**flags(), 'BTC_ORDERBOOK_ENABLED': 'true', 'POLY_SYMBOLS': 'ETH'}
        with patch.dict(os.environ, env, clear=True):
            self.assertEqual(enabled_symbols('orderbooks'), ['btc'])

    def test_missing_switches_preserve_legacy_configuration(self):
        with patch.dict(os.environ, {'POLY_SYMBOLS': 'ETH,ETH', 'SPOT_SYMBOLS': 'xrp/usd'}, clear=True):
            self.assertEqual(enabled_symbols('orderbooks'), ['eth'])
            self.assertEqual(enabled_symbols('prices'), ['xrp/usd'])

    def test_all_disabled_is_valid(self):
        with patch.dict(os.environ, flags(), clear=True):
            self.assertEqual(enabled_symbols('orderbooks'), [])
            self.assertEqual(enabled_symbols('prices'), [])

    def test_invalid_flag_is_rejected(self):
        with patch.dict(os.environ, {**flags(), 'BTC_SPOT_ENABLED': 'treu'}, clear=True):
            with self.assertRaisesRegex(ValueError, 'BTC_SPOT_ENABLED'):
                enabled_symbols('prices')

    def test_metadata_resolution_looks_for_closed_market_with_retries(self):
        market = {'slug': 'btc-test', 'umaResolutionStatus': 'resolved'}
        responses = [SimpleNamespace(status=200, data=b'[]'),
                     SimpleNamespace(status=200, data=json.dumps([market]).encode())]
        with patch.object(common.http, 'request', side_effect=responses) as request:
            self.assertEqual(runtime.fetch_slug('btc-test'), market)
        self.assertEqual([parse_qs(urlparse(call.args[1]).query)['closed'][0]
                          for call in request.call_args_list], ['false', 'true'])
        self.assertTrue(all(call.kwargs['retries'].total == 2 for call in request.call_args_list))


class CaptureSink:
    service = 'test'
    run_id = 'run'
    fatal = None

    def __init__(self):
        self.output = []
        self.logs = []
        self.console = ConsoleReporter('test', enabled=False)

    def log(self, event, **details):
        self.logs.append((event, details))

    def submit(self, rows):
        self.output.extend(rows)


class FakeSocket:
    def __init__(self):
        self.send = AsyncMock()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class StreamTests(unittest.IsolatedAsyncioTestCase):
    async def test_failing_worker_does_not_skip_other_workers_shutdown(self):
        drained = asyncio.Event()

        async def broken():
            raise RuntimeError('worker failed')

        async def finishing():
            await asyncio.sleep(.03)
            drained.set()

        with self.assertRaisesRegex(RuntimeError, 'worker failed'):
            await runtime.finish_tasks([asyncio.create_task(broken()), asyncio.create_task(finishing())])
        self.assertTrue(drained.is_set())

    async def test_malformed_price_does_not_disconnect_or_drop_later_updates(self):
        stop, sink, socket = asyncio.Event(), CaptureSink(), FakeSocket()

        async def messages(*args, **kwargs):
            for value in ('broken json', json.dumps(price_event(value='NaN')), json.dumps(price_event(2))):
                yield value
            stop.set()

        with patch.object(runtime.websockets, 'connect', return_value=socket) as connect, \
             patch.object(common, 'websocket_messages', messages):
            await runtime.price_stream(sink, stop, 'btc/usd')
        self.assertEqual(connect.call_count, 1)
        self.assertIsNone(connect.call_args.kwargs['ping_interval'])
        self.assertEqual(sum(t == 'crypto_prices_v2' for t, _ in sink.output), 1)
        self.assertEqual(sum(t == 'raw_events_v2' for t, _ in sink.output), 3)
        self.assertEqual(sum(event == 'message_error' for event, _ in sink.logs), 2)

    async def test_full_receive_queue_waits_and_drains_every_frame(self):
        stop, sink = asyncio.Event(), CaptureSink()

        async def messages(*args, **kwargs):
            for n in range(12050):
                yield json.dumps(price_event(n + 1))
            stop.set()

        with patch.object(runtime.websockets, 'connect', return_value=FakeSocket()), \
             patch.object(common, 'websocket_messages', messages):
            await asyncio.wait_for(runtime.price_stream(sink, stop, 'btc/usd'), 15)
        rows = [r for t, r in sink.output if t == 'crypto_prices_v2']
        self.assertEqual(len(rows), 12050)
        self.assertEqual([r['seq'] for r in rows], list(range(1, 12051)))
        self.assertFalse(any(event == 'gap_start' for event, _ in sink.logs))

    async def test_snapshot_repair_keeps_connection_and_processes_tail(self):
        stop, sink, socket = asyncio.Event(), CaptureSink(), FakeSocket()
        incoming = asyncio.Queue()
        repair_requested = asyncio.Event()

        async def sent(value):
            if json.loads(value).get('operation') == 'subscribe':
                repair_requested.set()
        socket.send.side_effect = sent

        async def messages(*args, **kwargs):
            while True:
                raw = await incoming.get()
                if raw is None:
                    stop.set()
                    return
                yield raw

        with patch.object(runtime.websockets, 'connect', return_value=socket) as connect, \
             patch.object(common, 'websocket_messages', messages):
            task = asyncio.create_task(runtime.market_stream(sink, stop, dict(market='market', asset_id='up', slug='test')))
            try:
                await incoming.put(json.dumps(book_event()))
                await incoming.put('invalid json')
                await asyncio.wait_for(repair_requested.wait(), 2)
                await incoming.put(json.dumps(book_event()))
                for n in range(25):
                    await incoming.put(json.dumps(dict(event_type='price_change', market='market', price_changes=[
                        dict(asset_id='up', side='BUY', price='0.4', size=str(n + 1))])))
                await incoming.put(None)
                await asyncio.wait_for(task, 3)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(connect.call_count, 1)
        self.assertEqual(sum(t == 'orderbook_v2' for t, _ in sink.output), 1)
        self.assertEqual(sum(t == 'pricechange_v2' for t, _ in sink.output), 25)
        self.assertTrue(any(event == 'resync_complete' for event, _ in sink.logs))
        self.assertFalse(any(event == 'gap_start' for event, _ in sink.logs))

    async def test_gap_start_is_preserved_across_failed_reconnects(self):
        stop, sink = asyncio.Event(), CaptureSink()
        calls = 0

        async def messages(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                yield json.dumps(price_event())
                raise OSError('first disconnect')
            if calls == 2:
                raise OSError('still offline')
            yield json.dumps(price_event(3))
            stop.set()

        with patch.object(runtime.websockets, 'connect', return_value=FakeSocket()), \
             patch.object(common, 'websocket_messages', messages), patch.object(runtime, 'delay', AsyncMock()):
            await runtime.price_stream(sink, stop, 'btc/usd')
        starts = [row for event, row in sink.logs if event == 'gap_start']
        ends = [row for event, row in sink.logs if event == 'gap_end']
        self.assertEqual(len(starts), 1)
        self.assertEqual(len(ends), 1)
        self.assertEqual(starts[0]['last_receive_ns'], ends[0]['last_receive_ns'])

    async def test_storage_backpressure_retries_same_record(self):
        sink = CaptureSink()
        original = sink.submit
        attempts = 0

        def submit(rows):
            nonlocal attempts
            attempts += 1
            if attempts <= 2:
                raise queue.Full()
            original(rows)
        sink.submit = submit
        await runtime.submit_records(sink, [('table', {'value': 'kept'})])
        self.assertEqual(sink.output, [('table', {'value': 'kept'})])

    async def test_heartbeat_continues_while_consumer_is_busy(self):
        socket = SimpleNamespace(recv=AsyncMock(return_value='{}'), send=AsyncMock())
        stop = asyncio.Event()
        async for _ in common.websocket_messages(socket, ping_every=.01, stop_event=stop):
            await asyncio.sleep(.05)
            stop.set()
        self.assertGreaterEqual(socket.send.await_count, 3)

    async def test_dead_peer_is_detected_without_protocol_pings(self):
        incoming = asyncio.Queue()
        socket = SimpleNamespace(recv=AsyncMock(side_effect=incoming.get), send=AsyncMock())
        with self.assertRaises(TimeoutError):
            async for _ in common.websocket_messages(socket, ping_every=.01, idle_timeout=.04):
                pass
        self.assertGreaterEqual(socket.send.await_count, 2)

    async def test_all_disabled_never_creates_storage_or_network_tasks(self):
        with patch.dict(os.environ, {**flags(), 'COLLECTOR_RUN_SECONDS': '.01'}), \
             patch.object(runtime, 'DurableSink') as sink, \
             patch.object(runtime, 'run_markets', AsyncMock()) as books, \
             patch.object(runtime, 'run_prices', AsyncMock()) as prices:
            await runtime.run('orderbooks')
            await runtime.run('prices')
        sink.assert_not_called()
        books.assert_not_called()
        prices.assert_not_called()


class StorageRecoveryTests(unittest.TestCase):
    def test_status_records_survive_a_full_data_queue(self):
        sent = []
        with tempfile.TemporaryDirectory() as directory, patch.object(DurableSink, 'persist_loop'):
            sink = DurableSink('test', directory, insert=lambda table, cols, rows: sent.extend(
                (table, dict(zip(cols, row))) for row in rows))
            sink.writer.join(timeout=1)
            sink.queue = queue.Queue(maxsize=1)
            sink.submit([record('raw_events_v2', context('r', 'c', 1), stream='test', payload='{}')])
            sink.log('heartbeat')
            sink.log('shutdown_requested')
            # Start the real writer after the queue-full scenario is established.
            real_writer = self._real_persist_loop
            sink.writer = threading.Thread(target=real_writer, args=(sink,), daemon=True)
            sink.writer.start()
            self.assertEqual(sink.close(timeout=3), 0)
        self.assertEqual(sum(table.endswith('raw_events_v2') for table, _ in sent), 1)
        self.assertEqual({row['event'] for table, row in sent if table.endswith('collector_log_v2')},
                         {'heartbeat', 'shutdown_requested'})

    _real_persist_loop = staticmethod(DurableSink.persist_loop)

    def test_database_startup_failure_does_not_block_disk_persistence(self):
        failed, release = threading.Event(), threading.Event()
        sent = []

        def prepare():
            if not release.is_set():
                failed.set()
                raise OSError('database temporarily offline')
        with tempfile.TemporaryDirectory() as directory:
            sink = DurableSink('test', directory, insert=lambda t, c, r: sent.extend(r), prepare=prepare)
            try:
                self.assertTrue(failed.wait(1))
                sink.submit([record('raw_events_v2', context('r', 'c', 1), stream='test', payload='{}')])
                deadline = time.monotonic() + 2
                persisted = 0
                while time.monotonic() < deadline and not persisted:
                    with closing(sqlite3.connect(sink.path)) as db:
                        persisted = db.execute('SELECT count() FROM outbox').fetchone()[0]
                    time.sleep(.02)
                self.assertEqual(persisted, 1)
                self.assertEqual(sent, [])
                release.set()
            finally:
                release.set()
                self.assertEqual(sink.close(timeout=3), 0)
            self.assertEqual(len(sent), 1)

    def test_book_cannot_become_valid_until_requested_snapshot_arrives(self):
        book = Book('market', 'up')
        ctx = context('r', 'c', 1)
        book.process(book_event(), ctx)
        book.require_snapshot()
        book.process(dict(event_type='price_change', price_changes=[
            dict(asset_id='up', side='BUY', price='0.4', size='12', best_bid='0.4', best_ask='0.6')]), ctx)
        self.assertFalse(book.valid)
        book.process(book_event(), ctx)
        self.assertTrue(book.valid)

    def test_invalid_bbo_does_not_partially_apply_delta(self):
        book = Book('market', 'up')
        ctx = context('r', 'c', 1)
        book.process(book_event(), ctx)
        before = dict(book.levels)
        with self.assertRaises(ValueError):
            book.process(dict(event_type='price_change', price_changes=[
                dict(asset_id='up', side='BUY', price='0.4', size='999', best_bid='NaN')]), ctx)
        self.assertEqual(book.levels, before)

    def test_tick_notification_does_not_clear_book_inconsistency(self):
        book = Book('market', 'up')
        ctx = context('r', 'c', 1)
        book.process(book_event(), ctx)
        book.process(dict(event_type='price_change', price_changes=[
            dict(asset_id='up', side='BUY', price='0.4', size='10', best_bid='0.5')]), ctx)
        self.assertFalse(book.valid)
        book.process(dict(event_type='tick_size_change', asset_id='up', new_tick_size='0.01'), ctx)
        self.assertFalse(book.valid)


class LauncherTests(unittest.TestCase):
    def test_python_project_starts_both_processes_from_either_directory(self):
        for cwd in (ROOT, ROOT.parent):
            with self.subTest(cwd=cwd), tempfile.TemporaryDirectory() as directory:
                env = {**os.environ, **flags(), 'COLLECTOR_STATE_DIR': directory,
                       'COLLECTOR_RUN_SECONDS': '1', 'COLLECTOR_LOG_ENABLED': 'true'}
                result = subprocess.run([sys.executable, 'project'], cwd=cwd, env=env,
                                        capture_output=True, text=True, timeout=12)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertIn('[СТАКАН] Парсинг выключен', result.stdout)
                self.assertIn('[СПОТ] Парсинг выключен', result.stdout)
                self.assertEqual(result.stderr, '')
                self.assertFalse(list(Path(directory).glob('*.sqlite3')))

    def test_invalid_env_stops_before_starting_either_collector(self):
        with tempfile.TemporaryDirectory() as directory:
            env = {**os.environ, **flags(), 'COLLECTOR_STATE_DIR': directory, 'BTC_SPOT_ENABLED': 'typo'}
            result = subprocess.run([sys.executable, 'project'], cwd=ROOT, env=env,
                                    capture_output=True, text=True, timeout=10)
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn('Оба процесса запущены', result.stdout)
            self.assertFalse(list(Path(directory).glob('*.sqlite3')))
            self.assertIn('BTC_SPOT_ENABLED', (Path(directory) / 'launcher-errors.log').read_text())


if __name__ == '__main__':
    unittest.main()
