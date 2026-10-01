"""End-to-end collector tests against local HTTP and WebSocket fixtures only."""
import asyncio
import gzip
import json
import os
import signal
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from contextlib import closing
from collections import Counter
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import websockets
from websockets.exceptions import ConnectionClosed

from collection_config import SYMBOLS

ROOT = Path(__file__).resolve().parents[1]


class IntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_both_enabled_services_save_selected_symbols_and_stop_cleanly(self):
        inserted, seen_books, seen_prices = [], [], []
        requests_lock = threading.Lock()
        failures = 1

        class HTTPHandler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                slug = parse_qs(urlparse(self.path).query)['slug'][0]
                start = int(slug.rsplit('-', 1)[1])
                iso = lambda ts: datetime.fromtimestamp(ts, timezone.utc).isoformat()
                market = dict(conditionId='condition-' + slug, slug=slug, outcomes=['Up', 'Down'], clobTokenIds=[slug, 'down-' + slug],
                              eventStartTime=iso(start), endDate=iso(start + 900), cryptoMarketConfig={'asset': 'BTC'})
                body = json.dumps([market]).encode()
                self.send_response(200)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self):
                nonlocal failures
                body = self.rfile.read(int(self.headers.get('Content-Length', 0)))
                if self.headers.get('Content-Encoding') == 'gzip':
                    body = gzip.decompress(body)
                sql = parse_qs(urlparse(self.path).query)['query'][0]
                with requests_lock:
                    fail = failures > 0
                    if fail:
                        failures -= 1
                    elif sql.startswith('INSERT'):
                        table = sql.split()[2].rsplit('.', 1)[-1]
                        inserted.extend((table, json.loads(row)) for row in body.splitlines())
                self.send_response(503 if fail else 200)
                self.send_header('Content-Length', '0')
                self.end_headers()

        http = ThreadingHTTPServer(('127.0.0.1', 0), HTTPHandler)
        server_thread = threading.Thread(target=http.serve_forever, daemon=True)
        server_thread.start()

        async def market_server(ws):
            sub = json.loads(await ws.recv())
            asset = sub['assets_ids'][0]
            seen_books.append(asset)
            snapshot = dict(event_type='book', market='condition-' + asset, asset_id=asset,
                            bids=[dict(price='0.413579', size='10')], asks=[dict(price='0.6', size='20')])
            await ws.send(json.dumps(snapshot))
            await ws.send(json.dumps(snapshot))
            await ws.send(json.dumps({**snapshot, 'asks': [dict(price='0.65', size='30')]}))

            async def updates():
                n = 0
                while True:
                    n += 1
                    await ws.send(json.dumps(dict(event_type='price_change', market='condition-' + asset, price_changes=[
                        dict(asset_id=asset, side='BUY', price='0.413579', size=str(n))])))
                    await asyncio.sleep(.03)
            task = asyncio.create_task(updates())
            try:
                async for raw in ws:
                    if raw == 'PING':
                        await ws.send('PONG')
                    elif json.loads(raw).get('operation') == 'subscribe':
                        await ws.send(json.dumps(snapshot))
            except ConnectionClosed:
                pass
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

        async def prices_server(ws):
            sub = json.loads(await ws.recv())
            subscriptions = sub['subscriptions']
            seen_prices.extend(json.loads(s['filters'])['symbol'] for s in subscriptions)

            async def updates():
                while True:
                    for item in subscriptions:
                        await ws.send(json.dumps(dict(topic=item['topic'], type='update', payload=dict(
                            symbol=json.loads(item['filters'])['symbol'], timestamp=int(time.time() * 1000), value='67890.123456'))))
                    await asyncio.sleep(.03)
            task = asyncio.create_task(updates())
            try:
                async for raw in ws:
                    if raw == 'PING':
                        await ws.send('PONG')
            except ConnectionClosed:
                pass
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

        try:
            async with websockets.serve(market_server, '127.0.0.1', 0, ping_interval=None) as books, \
                       websockets.serve(prices_server, '127.0.0.1', 0, ping_interval=None) as prices:
                with tempfile.TemporaryDirectory() as directory:
                    switches = {f'{s}_{kind}_ENABLED': 'false' for s in SYMBOLS for kind in ('ORDERBOOK', 'SPOT')}
                    env = {**os.environ, **switches, 'BTC_ORDERBOOK_ENABLED': 'true', 'ETH_SPOT_ENABLED': 'true',
                           'POLY_WS_URL': f'ws://127.0.0.1:{books.sockets[0].getsockname()[1]}',
                           'SPOT_WS_URL': f'ws://127.0.0.1:{prices.sockets[0].getsockname()[1]}',
                           'POLY_GAMMA_URL': f'http://127.0.0.1:{http.server_port}/markets',
                           'CLICKHOUSE_HTTP_HOST': '127.0.0.1', 'CLICKHOUSE_HOST': '127.0.0.1',
                           'CLICKHOUSE_HTTP_PORT': str(http.server_port), 'CLICKHOUSE_HTTP_INTERFACE': 'http',
                           'CLICKHOUSE_USER': 'test', 'CLICKHOUSE_PASSWORD': '', 'CLICKHOUSE_DB': 'test',
                           'COLLECTOR_STATE_DIR': directory, 'COLLECTOR_RUN_SECONDS': '0',
                           'COLLECTOR_LOG_ENABLED': 'true'}
                    process = await asyncio.create_subprocess_exec(sys.executable, 'project', cwd=ROOT, env=env,
                                                                  stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                    try:
                        deadline = time.monotonic() + 8
                        while time.monotonic() < deadline:
                            with requests_lock:
                                tables = {t for t, _ in inserted}
                            if {'orderbook_v2', 'pricechange_v2', 'crypto_prices_v2'} <= tables:
                                break
                            await asyncio.sleep(.05)
                        self.assertTrue({'orderbook_v2', 'pricechange_v2', 'crypto_prices_v2'} <= tables)
                        process.send_signal(signal.SIGINT)
                        stdout, stderr = await asyncio.wait_for(process.communicate(), 12)
                        self.assertEqual(process.returncode, 0, stdout.decode() + stderr.decode())
                        self.assertEqual(stderr, b'')
                        self.assertNotIn(b'67890.123456', stdout)
                        self.assertNotIn(b'0.413579', stdout)
                        self.assertNotIn('Процесс идет.', stdout.decode())
                        self.assertIn('[СТАКАН BTC] orderbook_v2 | записано строк: ', stdout.decode())
                        self.assertIn('[СПОТ ETH] crypto_prices_v2 | записано строк: ', stdout.decode())
                        self.assertNotIn('[СПОТ BTC]', stdout.decode())
                        self.assertNotIn('[СТАКАН ETH]', stdout.decode())
                        self.assertNotIn('РЫНОК НЕ ОПРЕДЕЛЁН', stdout.decode())
                        self.assertTrue(seen_books)
                        self.assertTrue(all(asset.startswith('btc-updown-15m-') for asset in seen_books))
                        self.assertEqual(set(seen_prices), {'eth/usd'})
                        for service in ('orderbooks', 'prices'):
                            with closing(sqlite3.connect(Path(directory) / f'{service}.sqlite3')) as db:
                                self.assertEqual(db.execute('SELECT count() FROM outbox').fetchone()[0], 0)
                        with requests_lock:
                            self.assertTrue(all('_console_symbol' not in row for _, row in inserted))
                            snapshots = [row for table, row in inserted if table == 'orderbook_v2']
                            counts = Counter(row['asset_id'] for row in snapshots)
                            self.assertEqual(set(counts), set(seen_books))
                            self.assertTrue(all(count == 1 for count in counts.values()))
                            for table, row in inserted:
                                if table == 'raw_events_v2' and row['stream'].startswith('btc-updown-'):
                                    message = json.loads(row['payload'])
                                    if message.get('event_type') == 'book':
                                        self.assertTrue(message['snapshot_levels_omitted'])
                                        self.assertNotIn('bids', message)
                                        self.assertNotIn('asks', message)
                            self.assertTrue(any(table == 'pricechange_v2' and row['side'] == 'ask'
                                                and row['price'] == '0.6' and row['size'] == '0' for table, row in inserted))
                            logs = [r for t, r in inserted if t == 'collector_log_v2']
                            self.assertFalse(any(r['event'] == 'gap_start' for r in logs))
                            self.assertEqual({r['service'] for r in logs if r['event'] == 'shutdown_requested'}, {'orderbooks', 'prices'})
                    finally:
                        if process.returncode is None:
                            process.kill()
                            await process.communicate()
        finally:
            await asyncio.to_thread(http.shutdown)
            http.server_close()
            server_thread.join(timeout=2)


if __name__ == '__main__':
    unittest.main()
