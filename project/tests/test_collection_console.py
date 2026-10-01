import gzip
import json
import tempfile
import threading
import unittest
from unittest.mock import patch
from types import SimpleNamespace

import ingest_common as common
from collection_console import CONSOLE_SYMBOL_KEY, ConsoleReporter
from collection_store import DurableSink, context, record


class ConsoleTests(unittest.TestCase):
    def test_each_saved_batch_reports_counts_without_market_values(self):
        lines = []
        reporter = ConsoleReporter('orderbooks', emit=lines.append)
        reporter.confirmed('polyk.orderbook_v2', [dict(_console_symbol='BTC', market='opaque-market',
                                                      bids='secret-price', asks='other-price')])
        reporter.confirmed('polyk.pricechange_v2', [dict(market='opaque-market', price='0.12345', size='9999')] * 10000)
        self.assertIn('[СТАКАН BTC] orderbook_v2 | записано строк: 1 | всего за запуск: 1', lines[0])
        self.assertIn('[СТАКАН BTC] pricechange_v2 | записано строк: 10000 | всего за запуск: 10000', lines[1])
        reporter.confirmed('polyk.pricechange_v2', [dict(market='opaque-market')] * 2)
        self.assertIn('записано строк: 2 | всего за запуск: 10002', lines[-1])
        reporter = ConsoleReporter('prices', emit=lines.append)
        reporter.confirmed('polyk.crypto_prices_v2', [dict(symbol='btc/usd', value='60000.12345'),
                                                     dict(symbol='eth/usd', value='60000.12345'),
                                                     dict(symbol='btc/usd', value='60000.12345')])
        self.assertIn('[СПОТ BTC] crypto_prices_v2 | записано строк: 2 | всего за запуск: 2', lines[-2])
        self.assertIn('[СПОТ ETH] crypto_prices_v2 | записано строк: 1 | всего за запуск: 1', lines[-1])
        reporter.confirmed('polyk.crypto_prices_v2', [dict(symbol='eth/usd', value='60000.12345')])
        self.assertIn('[СПОТ ETH] crypto_prices_v2 | записано строк: 1 | всего за запуск: 2', lines[-1])
        reporter.confirmed('polyk.raw_events_v2', [dict(stream='sol/usd', payload='secret-price')])
        self.assertIn('[СПОТ SOL] raw_events_v2 | записано строк: 1 | всего за запуск: 1', lines[-1])
        for forbidden in ['secret-price', '0.12345', '9999', '60000.12345', 'Процесс идет']:
            self.assertNotIn(forbidden, '\n'.join(lines))

    def test_error_status_clears_only_after_all_causes_recover(self):
        lines = []
        reporter = ConsoleReporter('prices', emit=lines.append)
        reporter.error('storage')
        reporter.error('stream')
        reporter.error('storage', False)
        self.assertEqual(len(lines), 1)
        self.assertIn('Есть ошибки.', lines[-1])
        reporter.error('stream', False)
        self.assertIn('Работа восстановлена.', lines[-1])
        reporter.error('stream', False)
        self.assertEqual(len(lines), 2)
        reporter.error('btc/usd')
        reporter.error('eth/usd')
        self.assertIn('[СПОТ BTC] Есть ошибки.', lines[-2])
        self.assertIn('[СПОТ ETH] Есть ошибки.', lines[-1])
        reporter.error('btc/usd', False)
        self.assertIn('[СПОТ BTC] Работа восстановлена.', lines[-1])
        self.assertEqual(reporter.errors, {'eth/usd'})

    def test_market_label_survives_outbox_restart_without_changing_database_columns(self):
        def fail(*args):
            raise OSError('offline')
        sent, lines = [], []
        row = record('orderbook_v2', {**context('run', 'c', 1), CONSOLE_SYMBOL_KEY: 'XRP'},
                     market='opaque-market', bids='[]', asks='[]')
        with tempfile.TemporaryDirectory() as directory, patch.object(ConsoleReporter, 'write_line', side_effect=lines.append):
            first = DurableSink('orderbooks', directory, insert=fail)
            first.submit([row])
            self.assertGreater(first.close(timeout=0), 0)
            second = DurableSink('orderbooks', directory,
                                 insert=lambda table, columns, rows: sent.extend(dict(zip(columns, r)) for r in rows))
            self.assertEqual(second.close(), 0)
        self.assertEqual(sent, [{k: v for k, v in row[1].items() if k != CONSOLE_SYMBOL_KEY}])
        self.assertTrue(any('[СТАКАН XRP] orderbook_v2 | записано строк: 1 | всего за запуск: 1' in line for line in lines))

    def test_disabled_output_and_closed_terminal_do_not_break_collection(self):
        lines = []
        reporter = ConsoleReporter('prices', emit=lines.append, enabled=False)
        reporter.confirmed('polyk.crypto_prices_v2', [{}])
        reporter.error('storage')
        self.assertEqual(lines, [])
        reporter = ConsoleReporter('prices', emit=lambda line: (_ for _ in ()).throw(BrokenPipeError()))
        reporter.confirmed('polyk.crypto_prices_v2', [{}])
        self.assertFalse(reporter.enabled)

    def test_failed_insert_does_not_emit_saved_confirmation(self):
        attempted = threading.Event()
        def fail(*args):
            attempted.set()
            raise OSError('offline')
        with tempfile.TemporaryDirectory() as directory, patch.object(ConsoleReporter,'confirmed') as confirmed:
            sink=DurableSink('test',directory,insert=fail)
            sink.submit([record('raw_events_v2',context('run','c',1),stream='test',payload='{}')])
            self.assertTrue(attempted.wait(2))
            sink.close(timeout=0)
            confirmed.assert_not_called()

    def test_compression_preserves_large_insert_payload(self):
        body=('sample-value\n'*10000).encode()
        with patch.dict('os.environ',{'CH_HTTP_COMPRESSION':'true'}), patch.object(common.http,'request',return_value=SimpleNamespace(status=200)) as request:
            common._ch_request('INSERT INTO polyk.test FORMAT JSONEachRow',body)
        kwargs=request.call_args.kwargs
        self.assertEqual(kwargs['headers']['Content-Encoding'],'gzip')
        self.assertEqual(gzip.decompress(kwargs['body']),body)
        self.assertLess(len(kwargs['body']),len(body)//10)

if __name__=='__main__':unittest.main()
