import tempfile
import unittest
from pathlib import Path

from dpe.common import DATASETS, read_json, write_json
from dpe.ingestion import load_batch
from dpe.incremental import audit, run, table
from dpe.incremental_validation import scenario_batches, size_bytes
from dpe.rules import evaluate


class IncrementalTests(unittest.TestCase):
    def test_large_work_population_uses_persisted_membership_without_oversized_sql(self):
        from dpe.incremental import IncidentSelection,rows_in
        ids=['INC-%08d'%n for n in range(100000)]
        selection=IncidentSelection(ids,'lake.work.affected_incidents')
        self.assertEqual(list(selection),ids)
        self.assertEqual(rows_in(selection),'(SELECT DISTINCT incident_id FROM lake.work.affected_incidents)')
        self.assertEqual(rows_in(IncidentSelection(["a'b"],'lake.work.affected_incidents')),"('a''b')")
        with self.assertRaises(ValueError):IncidentSelection(ids,'lake.work.ids; DROP TABLE x')

    def test_large_reason_insert_stays_below_query_limit_and_preserves_every_value(self):
        from dpe.incremental import insert
        class Capture:
            queries=[]
            def sql(self,query):self.queries.append(query)
        client=Capture();rows=[('INC-%06d'%n,'reason-é'*200) for n in range(2500)]
        insert(client,'lake.work.reasons',rows)
        self.assertGreater(len(client.queries),1)
        self.assertTrue(all(len(q.encode())<1000000 for q in client.queries))
        text='\n'.join(client.queries)
        self.assertTrue(all(text.count("'"+key+"'")==1 for key,_ in rows))

    def test_cutoff_only_delivery_has_explicit_empty_manifest_files(self):
        with tempfile.TemporaryDirectory() as directory:
            paths = scenario_batches(Path(directory))
            header, records, checked = load_batch(paths[3])
            for name in ('incident_events', 'incident_snapshots'):
                files = [f for f in header['files'] if f['dataset'] == name]
                self.assertEqual(len(files), 1)
                self.assertEqual(files[0]['bytes'], 0)
                self.assertEqual(checked['rows'][name], 0)
                self.assertEqual(records[name], [])

    def test_edge_case_expectations_are_independent_of_sql(self):
        with tempfile.TemporaryDirectory() as directory:
            histories = {name: [] for name in DATASETS}
            results = []
            for path in scenario_batches(Path(directory)):
                header, records, _ = load_batch(path)
                for name in DATASETS: histories[name].extend(records[name])
                results.append(evaluate(histories, header['cutoff'], header['report_start']))
            self.assertEqual(set(results[0]['quarantined_incidents']), {'REPAIR'})
            self.assertEqual(set(results[1]['quarantined_incidents']), {'BAD'})
            old_keys = {(r['date'], r['service_id'], r['priority']) for r in results[0]['service_daily_metrics']}
            new_keys = {(r['date'], r['service_id'], r['priority']) for r in results[1]['service_daily_metrics']}
            self.assertIn(('2026-01-01', 'S2', 'P4'), old_keys)
            self.assertNotIn(('2026-01-01', 'S2', 'P4'), new_keys)
            final = {r['incident_id']: r for r in results[3]['incident_resolution']}
            self.assertEqual(final['FUTURE']['state'], 'closed')
            self.assertEqual(final['HOLD']['state'], 'on_hold')
            self.assertEqual(final['LATE']['resolved_at'], '2026-01-03T01:00:00Z')
            for name in ('incident_resolution', 'service_daily_metrics'):
                self.assertEqual(results[3][name], results[4][name])

    def test_source_failure_is_recorded_before_any_stack_access(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaises(OSError): run(root/'missing', root/'platform')
            attempts = list((root/'platform/attempts').glob('*.json'))
            self.assertEqual(len(attempts), 1)
            self.assertEqual(read_json(attempts[0])['status'], 'failed')
            self.assertFalse((root/'platform/active.json').exists())

    def test_explicit_audit_detects_archived_byte_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = scenario_batches(root/'source')[0]
            from dpe.common import digest
            import shutil
            checksum = digest(source/'manifest.json')
            archive = root/'sources'/checksum
            shutil.copytree(source, archive)
            write_json(root/'active.json', {'publication_id':'a'*64,'parent_publication_id':None,
                                           'archives':['sources/'+checksum]})
            self.assertEqual(audit(root)['status'], 'passed')
            header = read_json(archive/'manifest.json')
            (archive/header['files'][0]['path']).write_text('corrupted\n')
            with self.assertRaisesRegex(ValueError, 'checksum/size'): audit(root)

    def test_external_table_names_are_rejected(self):
        for value in ('other.schema.table', 'lake.a.table; DROP TABLE x', 'lake.a'):
            with self.assertRaises(ValueError): table(value)

    def test_physical_byte_units_are_not_decimal(self):
        self.assertEqual(size_bytes('1kB'), 1024)
        self.assertEqual(size_bytes('2MB'), 2*1024*1024)
        self.assertEqual(size_bytes('113B'), 113)
        with self.assertRaises(ValueError): size_bytes('missing')

class FileObserverTests(unittest.TestCase):
    def test_observer_skips_old_queries_and_marks_expired_scan_details(self):
        from datetime import datetime, timezone
        from unittest.mock import patch
        from benchmarks.collect_query_io import observe
        class OnePoll:
            stopped = False
            def is_set(self): return self.stopped
            def wait(self, duration): self.stopped = True
        queries = [{'queryId':name,'state':'FINISHED','queryStats':{'createTime':created}} for name,created in
                   [('old','2026-01-01T00:00:00Z'),('read','2026-01-02T00:00:01Z'),('expired','2026-01-02T00:00:02Z')]]
        def response(path):
            if path=='/v1/query': return queries
            if path.endswith('/read'):
                return {'queryStats':{'physicalInputPositions':50,'operatorSummaries':[
                    {'connectorMetrics':{'dataFiles':{'total':3},'bytesReadFromCache':{'total':100},'bytesReadExternally':{'total':20}}}]}}
            if path.endswith('/expired'): return {'queryStats':{'physicalInputPositions':50,'operatorSummaries':[]}}
            self.fail('observer requested old query')
        evidence = {'queries':{}}
        with patch('benchmarks.collect_query_io.request',side_effect=response):
            observe(OnePoll(),evidence,datetime(2026,1,2,tzinfo=timezone.utc))
        self.assertEqual(evidence['queries']['read']['dataFiles'],3)
        self.assertEqual(evidence['queries']['read']['bytesReadFromCache'],100)
        self.assertEqual(evidence['queries']['read']['bytesReadExternally'],20)
        self.assertIn('unavailable',evidence['queries']['expired'])
        self.assertNotIn('old',evidence['queries'])


if __name__ == '__main__':
    unittest.main()
