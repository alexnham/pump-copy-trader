import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from urllib.request import Request, urlopen
from urllib.error import HTTPError

from server import Database, Handler, ThreadingHTTPServer


class DatabaseTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / 'journal.sqlite'
        with sqlite3.connect(self.path) as connection:
            connection.executescript('''
                CREATE TABLE source_transactions(signature TEXT PRIMARY KEY, slot INTEGER, dex TEXT, pool TEXT, observed_at INTEGER, timings_json TEXT);
                CREATE TABLE copy_attempts(id INTEGER PRIMARY KEY, source_signature TEXT, local_signature TEXT, status TEXT, execution_target TEXT, route_latency_ms INTEGER, created_at INTEGER, signed_transaction BLOB);
                INSERT INTO source_transactions VALUES ('abc',100,'pump_swap','pool',1000,'{}');
                INSERT INTO copy_attempts VALUES (1,'abc','def','landed','mainnet',502,1000,X'0102');
            ''')
        self.database = Database(self.path)

    def tearDown(self):
        self.directory.cleanup()

    def test_old_schema_browsing_and_details(self):
        catalog = self.database.catalog()
        self.assertEqual(len(catalog['tables']),2)
        result = self.database.journal()
        self.assertEqual(result['total'],1)
        self.assertFalse(result['has_landed_slots'])
        self.assertIsNone(result['rows'][0][result['columns'].index('copy_slot')])
        self.assertEqual(self.database.journal(target='surfpool')['total'],0)
        self.assertEqual(self.database.journal(status='failed')['total'],0)
        self.assertEqual(self.database.journal(search='abc')['total'],1)
        self.assertEqual(self.database.journal(search="' OR 1=1 --")['total'],0)
        self.assertEqual(self.database.browse('copy_attempts',search='landed')['total'],1)
        self.assertEqual(self.database.browse('copy_attempts',search='%')['total'],0)
        self.assertEqual(self.database.browse('copy_attempts',offset=1)['rows'],[])
        self.assertEqual(self.database.trade('abc')['copy']['signed_transaction'],'[binary: 2 bytes]')
        with self.assertRaises(ValueError):
            self.database.browse('copy_attempts; DROP TABLE copy_attempts')
        with self.assertRaises(ValueError):
            self.database.browse('copy_attempts',sort='no_such_column')

    def test_landing_slot_support(self):
        with sqlite3.connect(self.path) as connection:
            connection.execute('ALTER TABLE copy_attempts ADD COLUMN landed_slot INTEGER')
            connection.execute('UPDATE copy_attempts SET landed_slot=103')
        result = self.database.journal()
        self.assertTrue(result['has_landed_slots'])
        self.assertEqual(result['rows'][0][result['columns'].index('slot_delta')],3)

    def test_flow_averages_group_landed_paths_and_ignore_missing_metrics(self):
        with sqlite3.connect(self.path) as connection:
            connection.execute('ALTER TABLE copy_attempts ADD COLUMN landed_slot INTEGER')
            for number, dex, status, timing in [
                (2, 'pump_fun', 'landed', {'mint_from_source': 1, 'mint_read_ms': 0, 'preparation_ms': 4, 'receipt_to_send_start_ms': 8}),
                (3, 'pump_fun', 'landed', {'mint_from_source': 1, 'mint_read_ms': 0, 'preparation_ms': 6, 'receipt_to_send_start_ms': 12}),
                (4, 'pump_fun', 'landed', {'preparation_ms': 80, 'receipt_to_send_start_ms': 100}),
                (5, 'pump_swap', 'landed', {'route_instruction_build_ms': 60, 'receipt_to_send_start_ms': 120}),
                (6, 'pump_fun', 'failed', {'mint_from_source': 1, 'receipt_to_send_start_ms': 1000}),
            ]:
                signature = f'source-{number}'
                connection.execute('INSERT INTO source_transactions VALUES (?,?,?,?,?,?)',
                                   (signature, 100, dex, None, number, json.dumps(timing)))
                connection.execute('INSERT INTO copy_attempts VALUES (?,?,?,?,?,?,?,?,?)',
                                   (number, signature, None, status, 'mainnet', None, number, None, 101))
        groups = self.database.flow_stats()['groups']
        self.assertEqual(groups['pump_fun_source']['count'], 2)
        self.assertEqual(groups['pump_fun_source']['metrics']['receipt_to_send_start_ms'],
                         {'average': 10.0, 'count': 2})
        self.assertEqual(groups['pump_fun_source']['metrics']['preparation_ms']['average'], 5.0)
        self.assertEqual(groups['pump_fun_rpc']['count'], 1)
        self.assertNotIn('mint_read_ms', groups['pump_fun_rpc']['metrics'])
        self.assertEqual(groups['pump_swap_source']['count'], 1)
        self.assertEqual(groups['pump_swap_source']['metrics']['route_instruction_build_ms']['average'], 60.0)

    def test_read_only_queries_and_no_external_database_access(self):
        self.assertEqual(self.database.query('SELECT count(*) AS n FROM copy_attempts')['rows'],[[1]])
        self.assertEqual(self.database.query('WITH x AS (SELECT 7 AS value) SELECT * FROM x')['rows'],[[7]])
        self.assertTrue(self.database.query("PRAGMA table_info('copy_attempts')")['rows'])
        statements = [
            "DELETE FROM copy_attempts", "UPDATE copy_attempts SET status='failed'",
            "INSERT INTO copy_attempts(id) VALUES(2)", "DROP TABLE copy_attempts",
            "CREATE TABLE bad(x)", "PRAGMA query_only=OFF", "PRAGMA user_version=5",
            "ATTACH DATABASE ':memory:' AS other", "VACUUM INTO '/tmp/workbench-should-not-exist.sqlite'",
            "SELECT load_extension('anything')", "SELECT 1; SELECT 2",
        ]
        for statement in statements:
            with self.subTest(statement=statement), self.assertRaises(sqlite3.Error):
                self.database.query(statement)
        self.assertEqual(self.database.query('SELECT status FROM copy_attempts')['rows'],[['landed']])

    def test_limits_and_exact_integers(self):
        result = self.database.query('WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<600) SELECT x FROM n')
        self.assertEqual(len(result['rows']),500)
        self.assertTrue(result['truncated'])
        self.assertEqual(self.database.query('SELECT 9223372036854775807')['rows'],[['9223372036854775807']])
        with self.assertRaises(sqlite3.Error):
            self.database.query('SELECT zeroblob(2000000)')

    def test_http_serves_ui_and_rejects_foreign_origins(self):
        server = ThreadingHTTPServer(('127.0.0.1',0),Handler)
        server.database = self.database
        thread = threading.Thread(target=server.serve_forever,daemon=True)
        thread.start()
        base = f'http://127.0.0.1:{server.server_port}'
        try:
            with urlopen(base+'/') as response:
                self.assertIn(b'Database workbench',response.read())
                self.assertIn("frame-ancestors 'none'",response.headers['Content-Security-Policy'])
            with urlopen(base+'/api/flow-stats') as response:
                self.assertIn('groups', json.load(response))
            with urlopen(Request(base+'/',headers={'Sec-Fetch-Site':'cross-site','Sec-Fetch-Mode':'navigate'})) as response:
                self.assertEqual(response.status,200)
            request = Request(base+'/api/query',data=json.dumps({'sql':'SELECT count(*) FROM copy_attempts'}).encode(),headers={'Content-Type':'application/json','Origin':base})
            with urlopen(request) as response:
                self.assertEqual(json.load(response)['rows'],[[1]])
            for headers in [{'Origin':'https://example.com'},{'Host':'evil.example'},{'Sec-Fetch-Site':'cross-site'}]:
                with self.subTest(headers=headers), self.assertRaises(HTTPError) as error:
                    urlopen(Request(base+'/api/catalog',headers=headers))
                self.assertEqual(error.exception.code,403)
            request = Request(base+'/api/query',data=json.dumps({'sql':'DELETE FROM copy_attempts'}).encode(),headers={'Content-Type':'application/json'})
            with self.assertRaises(HTTPError) as error:
                urlopen(request)
            self.assertEqual(error.exception.code,400)
            self.assertEqual(self.database.query('SELECT count(*) FROM copy_attempts')['rows'],[[1]])
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == '__main__':
    unittest.main()

class UnsupportedTests(DatabaseTests):
    def test_unsupported_source_without_attempt_is_visible_and_filterable(self):
        with sqlite3.connect(self.path) as connection:
            connection.execute('ALTER TABLE source_transactions ADD COLUMN status TEXT')
            connection.execute('ALTER TABLE source_transactions ADD COLUMN skip_reason TEXT')
            connection.execute('INSERT INTO source_transactions(signature,slot,observed_at,status,skip_reason,timings_json) VALUES (?,?,?,?,?,?)',
                               ('unsupported-source',101,1001,'unsupported',json.dumps({'code':'unsupported_dex','message':'Outside Pump scope'}),'{}'))
        result = self.database.journal(target='mainnet',status='unsupported')
        self.assertEqual(result['total'],1)
        row = dict(zip(result['columns'], result['rows'][0]))
        self.assertEqual(row['status'],'unsupported')
        self.assertEqual(row['source_signature'],'unsupported-source')
        self.assertIsNone(row['copy_slot'])
        self.assertIsNone(row['local_signature'])
        detail = self.database.trade('unsupported-source')
        self.assertIsNone(detail['copy'])
        self.assertEqual(json.loads(detail['source']['skip_reason'])['code'],'unsupported_dex')
        self.assertEqual(self.database.journal(status='landed')['total'],1)
        self.assertEqual(self.database.journal(search='unsupported-source')['total'],1)
