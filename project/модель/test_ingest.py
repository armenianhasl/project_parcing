"""Offline regression checks for ingestion and ClickHouse writes."""

import asyncio
import contextlib
import io
import json
import os
import threading
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs, urlsplit

# Keep tests independent of local credentials and market filters.
with patch.dict(os.environ, {}, clear=True), patch('dotenv.load_dotenv'):
    import ingest_common as common
    import spot_ingest as spot
    import ws_ingest as market


class MessageTests(unittest.TestCase):
    def setUp(self):
        self.outcomes = patch.object(market, 'OUTCOME_MAP', {'up': 'UP', 'down': 'DOWN'})
        self.outcomes.start()
        self.addCleanup(self.outcomes.stop)
        self.book = {
            'event_type': 'book', 'market': 'market', 'asset_id': 'up',
            'timestamp': '1700000000000', 'hash': 'snapshot',
            'bids': [{'price': '0.4', 'size': '10'}, {'price': '0.4', 'size': '2'}],
            'asks': [{'price': '0.6', 'size': '20'}],
        }

    def test_message_batches_and_numeric_formats(self):
        a, b = {'a': 1}, {'b': 2}
        self.assertEqual(common.normalize_messages([a, [b, None], 'PING']), [a, b])
        self.assertEqual(common.normalize_messages(a), [a])
        self.assertEqual(common.normalize_messages('PONG'), [])
        self.assertEqual(common.to_float('20,767.81'), 20767.81)
        self.assertEqual(common.to_float('20767,81'), 20767.81)
        self.assertEqual(common.to_int('bad', 7), 7)
        self.assertEqual(common.to_int(float('inf'), 7), 7)

    def test_snapshot_grid_and_aggregated_levels(self):
        bids, asks = market.extract_book_level_sizes(self.book)
        rows = market.build_orderbook_rows_from_initial_book(self.book, bids, asks)
        self.assertEqual(len(rows), 999)
        self.assertEqual((rows[0][6], rows[-1][6]), (0.001, 0.999))
        nonzero = [(row[5], row[6], row[7]) for row in rows if row[7]]
        self.assertEqual(nonzero, [('bid', 0.4, 12.0), ('ask', 0.6, 20.0)])
        self.assertEqual(market.build_orderbook_rows_from_initial_book(self.book, {}, {}), [])
        self.book['asset_id'] = 'down'
        self.assertEqual(market.build_orderbook_rows_from_initial_book(self.book, bids, asks), [])

    def test_price_changes_are_deltas_and_ignore_duplicate_sizes(self):
        tick = market.price_to_tick(0.4)
        state = {f'market|up|bid|{tick}': 12.0}
        msg = {'market': 'market', 'timestamp': '1700000000100', 'price_changes': [
            {'asset_id': 'up', 'side': 'BUY', 'price': '0.4', 'size': str(size)}
            for size in (15, 15, 0)
        ]}
        self.assertEqual(market.build_pricechange_rows(msg, state, set()), [])
        rows = market.build_pricechange_rows(msg, state, {'market|up'})
        self.assertEqual([row[7] for row in rows], [3.0, -15.0])
        self.assertEqual(state, {})

    def test_invalid_prices_and_sizes_do_not_poison_state(self):
        for value in (float('nan'), float('inf'), -1, 1.5):
            self.assertIsNone(market.price_to_tick(value))
        self.book['bids'] = [{'price': '0.4', 'size': 'nan'}]
        self.assertEqual(market.extract_book_level_sizes(self.book)[0], {})
        msg = {'market': 'market', 'timestamp': '1700000000100', 'price_changes': [
            {'asset_id': 'up', 'side': 'BUY', 'price': '0.4', 'size': value}
            for value in ('NaN', 'Infinity', '-1', 'bad', None)
        ]}
        state = {'market|up|bid|399': 12.0}
        self.assertEqual(market.build_pricechange_rows(msg, state, {'market|up'}), [])
        self.assertEqual(state, {'market|up|bid|399': 12.0})

    def test_gamma_outcomes_can_be_json_strings_in_any_order(self):
        row = {'conditionId': 'm', 'clobTokenIds': '["down", "up"]', 'outcomes': '["Down", "Up"]'}
        self.assertEqual(market.build_outcome_map([row]), (['down', 'up'], {'down': 'Down', 'up': 'Up'}))
        self.assertTrue(market._has_up_down_intent(row, ''))

    def test_selection_keeps_nearest_market_for_each_symbol(self):
        now = datetime.now(timezone.utc).timestamp()
        rows = [
            {'conditionId': name, 'slug': slug, 'endDate': now + delta}
            for name, slug, delta in (
                ('old', 'btc-updown-15m', -100), ('later', 'btc-updown-15m', 120),
                ('btc', 'bitcoin-updown-15m', 60), ('eth', 'ethereum-updown-15m', 80),
            )
        ]
        with patch.object(market, 'POLY_SYMBOLS', ['BTC', 'ETH']), patch.object(market, 'POLY_MAX_MATCHED_MARKETS', 4):
            selected = market._select_current_markets_by_symbol(rows)
        self.assertEqual([row['conditionId'] for row in selected], ['btc', 'eth'])

    def test_discovery_follows_cursor_even_when_page_is_short(self):
        rows = [{'id': 'keep'}, {'id': 'skip'}, {'id': 'keep2'}]
        pages = [{'markets': rows[:2], 'next_cursor': 'next-page'}, {'markets': rows[2:], 'next_cursor': None}]
        with patch.object(market, 'POLY_LIMIT', 200), patch.object(market, 'POLY_TRACK_CURRENT_ONLY', False), \
             patch.object(market, 'http_get_json', side_effect=pages) as get, \
             patch.object(market, 'market_passes_filters', side_effect=lambda row: row['id'] != 'skip'):
            self.assertEqual(market.fetch_filtered_markets(), [rows[0], rows[2]])
        queries = [parse_qs(urlsplit(call.args[0]).query) for call in get.call_args_list]
        self.assertNotIn('after_cursor', queries[0])
        self.assertEqual(queries[1]['after_cursor'], ['next-page'])

    def test_discovery_rejects_repeated_cursor(self):
        with patch.object(market, 'http_get_json', return_value={'markets': [], 'next_cursor': 'repeated'}):
            with self.assertRaisesRegex(RuntimeError, 'repeated cursor'):
                market.fetch_filtered_markets()

    def test_spot_subscription_and_filtering(self):
        msg = {'topic': 'crypto_prices_chainlink', 'type': 'update', 'timestamp': 1700000000123,
               'payload': {'symbol': 'btc/usd', 'value': '60000.5', 'timestamp': 1700000000000,
                           'full_accuracy_value': '60000.500000000000'}}
        row = spot.parse_spot_row(msg)
        self.assertEqual(row[1:], ('btc/usd', 'crypto_prices_chainlink', 1700000000000,
                                   1700000000123, 60000.5, '60000.500000000000'))
        subscription = spot.build_subscribe_message_for_symbol('eth/usd')['subscriptions']
        self.assertEqual(len(subscription), 1)
        self.assertEqual(json.loads(subscription[0]['filters']), {'symbol': 'eth/usd'})
        for bad in ('nan', 'Infinity', None):
            msg['payload']['value'] = bad
            self.assertIsNone(spot.parse_spot_row(msg))
        msg['payload'].update(value=1, symbol='unrequested/usd')
        self.assertIsNone(spot.parse_spot_row(msg))

    def test_spot_symbol_configuration_and_legacy_fallback(self):
        with patch.dict(os.environ, {'SPOT_SYMBOLS': ' BTC/USD,eth/usd,btc/usd '}, clear=True):
            self.assertEqual(spot._parse_spot_symbols(), ['btc/usd', 'eth/usd'])
        with patch.dict(os.environ, {'SPOT_SYMBOL': 'SOL/USD'}, clear=True):
            self.assertEqual(spot._parse_spot_symbols(), ['sol/usd'])


class StorageTests(unittest.TestCase):
    def test_insert_serializes_rows_and_keeps_password_out_of_url(self):
        with patch.object(common.http, 'request', return_value=SimpleNamespace(status=200)) as request, \
             patch.object(common, 'CH_PASS', 'test-password'):
            common.ch_http_insert_json_each_row('polyk.test', ['ts', 'value'], [(datetime(2026, 1, 1), 1.5)])
        args, kwargs = request.call_args
        self.assertNotIn('test-password', args[1])
        self.assertEqual(kwargs['headers']['X-ClickHouse-Key'], 'test-password')
        self.assertEqual(json.loads(kwargs['body']), {'ts': '2026-01-01 00:00:00', 'value': 1.5})

    def test_failed_inserts_retry_and_surface_final_failure(self):
        with patch.object(common, 'CH_INSERT_RETRIES', 3), patch.object(common.time, 'sleep'), \
             patch.object(common, '_ch_request', side_effect=[OSError('offline'), None]) as request:
            common.ch_http_insert_json_each_row('polyk.test', ['x'], [(1,)])
            self.assertEqual(request.call_count, 2)
        with patch.object(common, 'CH_INSERT_RETRIES', 2), patch.object(common.time, 'sleep'), \
             patch.object(common, '_ch_request', side_effect=OSError('offline')) as request:
            with self.assertRaises(OSError):
                common.ch_http_insert_json_each_row('polyk.test', ['x'], [(1,)])
            self.assertEqual(request.call_count, 2)

    def test_invalid_insert_payload_is_rejected_before_network(self):
        with patch.object(common, '_ch_request') as request:
            with self.assertRaises(ValueError):
                common.ch_http_insert_json_each_row('polyk.test', ['x'], [(float('nan'),)])
            with self.assertRaises(ValueError):
                common.ch_http_insert_json_each_row('polyk.test', ['x'], [(1, 2)])
            common.ch_http_insert_json_each_row('polyk.test', ['x'], [])
            request.assert_not_called()

    def test_startup_does_not_drop_columns_or_create_disabled_telemetry(self):
        with patch.object(market, 'ch_http_command') as command, \
             patch.object(market, 'HEARTBEAT_ENABLED', False), patch.object(market, 'SERVICE_LOG_ENABLED', False):
            market.ensure_tables()
        self.assertEqual(command.call_count, 2)
        for call in command.call_args_list:
            self.assertIn('CREATE TABLE IF NOT EXISTS', call.args[0])
            self.assertNotIn('ALTER', call.args[0])
        with patch.object(spot, 'ch_http_command') as command:
            spot.ensure_table()
        self.assertEqual(command.call_count, 1)


class WebSocketTests(unittest.IsolatedAsyncioTestCase):
    async def test_application_heartbeat_and_quiet_market_rotation(self):
        ws = SimpleNamespace(send=AsyncMock(), recv=AsyncMock())
        queue = asyncio.Queue()
        ws.recv.side_effect = queue.get
        stop = asyncio.Event()
        received = []
        async def consume():
            async for message in common.websocket_messages(ws, ping_every=0.01, stop_event=stop):
                received.append(message)
        task = asyncio.create_task(consume())
        try:
            await queue.put('PONG')
            await queue.put('{"event_type":"book"}')
            await asyncio.sleep(0.04)
            stop.set()
            await asyncio.wait_for(task, 0.2)
            self.assertEqual(received, ['{"event_type":"book"}'])
            self.assertGreaterEqual(ws.send.await_count, 2)
            self.assertTrue(all(call.args == ('PING',) for call in ws.send.call_args_list))
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_application_heartbeat_with_continuous_messages(self):
        ws = SimpleNamespace(send=AsyncMock(), recv=AsyncMock(return_value='{}'))
        stop = asyncio.Event()
        async for _ in common.websocket_messages(ws, ping_every=0.01, stop_event=stop):
            if ws.send.await_count:
                stop.set()
            await asyncio.sleep(0.002)
        ws.send.assert_awaited_with('PING')

    async def test_reconnect_uses_fresh_book_before_applying_deltas(self):
        from websockets.exceptions import ConnectionClosedOK
        selected = [{'conditionId': 'market', 'clobTokenIds': ['up', 'down'], 'outcomes': ['Up', 'Down']}]
        finished = asyncio.Event()
        deltas = []
        original = market.build_pricechange_rows
        def parse(*args):
            rows = original(*args)
            deltas.extend(row[7] for row in rows)
            if len(deltas) == 2:
                finished.set()
            return rows
        async def hold(*args):
            await asyncio.Event().wait()
        class Socket:
            def __init__(self, size):
                self.send = AsyncMock()
                book = {'event_type': 'book', 'market': 'market', 'asset_id': 'up',
                        'timestamp': '1700000000000', 'bids': [{'price': '0.4', 'size': size}], 'asks': []}
                change = {'event_type': 'price_change', 'market': 'market', 'timestamp': '1700000000100',
                          'price_changes': [{'asset_id': 'up', 'price': '0.4', 'side': 'BUY', 'size': size + 3}]}
                self.messages = iter([json.dumps(book), json.dumps(change)])
            async def __aenter__(self):
                return self
            async def __aexit__(self, *args):
                pass
            async def recv(self):
                try:
                    return next(self.messages)
                except StopIteration:
                    if finished.is_set():
                        await hold()
                    raise ConnectionClosedOK(None, None)
        previous_tasks = asyncio.all_tasks()
        with contextlib.ExitStack() as stack:
            for name in ('refresh_markets_loop', 'flusher_loop', 'heartbeat_loop'):
                stack.enter_context(patch.object(market, name, side_effect=hold))
            stack.enter_context(patch.object(market, 'fetch_filtered_markets', return_value=selected))
            stack.enter_context(patch.object(market, 'ensure_tables'))
            stack.enter_context(patch.object(market, 'service_log', new_callable=AsyncMock))
            stack.enter_context(patch.object(market, 'build_pricechange_rows', side_effect=parse))
            stack.enter_context(patch.object(market.websockets, 'connect', side_effect=[Socket(12), Socket(100)]))
            stack.enter_context(patch.object(market, 'POLY_DEBUG_MARKETS', False))
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            collector_task = asyncio.create_task(market.ingest_forever())
            try:
                await asyncio.wait_for(finished.wait(), 1)
                self.assertEqual(deltas, [3.0, 3.0])
            finally:
                tasks = asyncio.all_tasks() - previous_tasks
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)


class AsyncStorageTests(unittest.IsolatedAsyncioTestCase):
    async def test_slow_insert_allows_other_tasks_to_run(self):
        started, release = threading.Event(), threading.Event()
        def insert(*args):
            started.set()
            if not release.wait(2):
                raise TimeoutError('event loop was blocked')
        rows = [(datetime(2026, 1, 1), 'btc/usd', 'crypto_prices_chainlink', 1, 1, 2.0, '')]
        trigger = asyncio.Event()
        trigger.set()
        with patch.object(spot, 'ch_http_insert_json_each_row', side_effect=insert), contextlib.redirect_stdout(io.StringIO()):
            task = asyncio.create_task(spot.flusher_loop(rows, asyncio.Lock(), trigger))
            try:
                for _ in range(100):
                    if started.is_set():
                        break
                    await asyncio.sleep(0.005)
                self.assertTrue(started.is_set())
                self.assertFalse(release.is_set())
                release.set()
                await asyncio.sleep(0.02)
                self.assertEqual(rows, [])
            finally:
                release.set()
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    async def test_failed_batches_return_before_new_rows(self):
        for module in (spot, market):
            with self.subTest(module=module.__name__):
                rows, changes = [('old',)], [('delta',)]
                failed = asyncio.Event()
                # Use a thread-safe callback to simulate a row arriving during a failed insert.
                loop = asyncio.get_running_loop()
                def failing_insert(table, columns, batch):
                    if table.endswith(module.SPOT_TABLE if module is spot else module.ORDERBOOK_TABLE):
                        loop.call_soon_threadsafe(rows.append, ('new',))
                        loop.call_soon_threadsafe(failed.set)
                        raise OSError('offline')
                trigger = asyncio.Event()
                trigger.set()
                with patch.object(module, 'ch_http_insert_json_each_row', side_effect=failing_insert), \
                     patch.object(market, 'service_log', new_callable=AsyncMock), contextlib.redirect_stdout(io.StringIO()):
                    args = (rows, asyncio.Lock(), trigger) if module is spot else (rows, changes, asyncio.Lock(), trigger, {})
                    task = asyncio.create_task(module.flusher_loop(*args))
                    try:
                        await asyncio.wait_for(failed.wait(), 1)
                        for _ in range(100):
                            if rows == [('old',), ('new',)]:
                                break
                            await asyncio.sleep(0.005)
                        self.assertEqual(rows, [('old',), ('new',)])
                        if module is market:
                            self.assertEqual(changes, [])
                    finally:
                        task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await task


if __name__ == '__main__':
    unittest.main()
