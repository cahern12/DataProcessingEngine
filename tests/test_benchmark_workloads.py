import tempfile
import unittest
from pathlib import Path
from dpe.common import ROOT,read_json,write_json
from dpe.synthetic import generate
from dpe.sequences import generate_sequence
from dpe.ingestion import load_batch
from dpe.benchmark_workloads import expected
from dpe.benchmark_reports import scorecard

class BenchmarkWorkloadTests(unittest.TestCase):
    def test_empty_fraction_is_explicitly_versioned_and_really_empty(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);config=read_json(ROOT/'synthetic/configs/default.json')
            config.update(allow_empty_delta=True,delta_fraction=0,new_fraction=0)
            write_json(root/'config.json',config);generate(root/'source',count=3,config_path=root/'config.json')
            h,d,_=load_batch(root/'source/delta')
            self.assertFalse(d['incident_events']);self.assertFalse(d['incident_snapshots'])
            self.assertIn('v2',h['generator_version'])
    def test_sequence_regenerates_exactly_and_preserves_parentage(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            for name in ('a','b'):generate_sequence(root/name,days=30,count=3)
            self.assertEqual((root/'a/day-015/manifest.json').read_bytes(),(root/'b/day-015/manifest.json').read_bytes())
            sequence=read_json(root/'a/sequence.json')['batches']
            previous=None
            for batch in sequence:
                h,d,_=load_batch(root/'a'/batch['path']);self.assertEqual(h['parent_batch_id'],previous);previous=h['batch_id']
            self.assertIn('weeks_late_dimension_correction',sequence[15]['scenarios'])
    def test_isolated_workloads_have_independent_known_results(self):
        case=read_json(ROOT/'synthetic/fixtures/rule_cases.v1.json')['cases'][0]
        value=expected(case['datasets'],case['cutoff'])
        self.assertEqual(value['W1'],[{'incident_id':'I1','source_version':1,'priority':'P1'}])
        self.assertEqual(value['W2'],[{'incident_id':'I1','service_id':'S1','priority':'P1','event_count':2}])
        self.assertEqual(value['W3'][1]['elapsed_seconds'],3000)
    def test_smoke_cannot_pass_small_or_azure_gates(self):
        with tempfile.TemporaryDirectory() as directory:
            result=scorecard(Path(directory),[{'workload_id':'W2','engine':'trino','size':'smoke','status':'passed'}])
            self.assertTrue(all(cell['status']=='not_assessed' for cell in result['matrix']))
            self.assertTrue(all(g['verdict']=='not_assessed' for g in result['gates']))
            self.assertEqual(result['recommendation'],'incomplete')

    def test_long_sequences_remain_valid_under_v1_rules(self):
        from dpe.common import DATASETS
        from dpe.rules import evaluate
        with tempfile.TemporaryDirectory() as directory:
            for days in (30,60,90):
                root=Path(directory)/str(days);generate_sequence(root,days=days,count=2)
                combined={k:[] for k in DATASETS}
                for item in read_json(root/'sequence.json')['batches']:
                    header,data,_=load_batch(root/item['path'])
                    for name in DATASETS:combined[name]+=data[name]
                result=evaluate(combined,header['cutoff'],header['report_start'])
                self.assertFalse(result['quarantined_incidents'])

    def test_matrix_retains_topologies_variants_and_deduplicates_attempts(self):
        with tempfile.TemporaryDirectory() as directory:
            run={'attempt_id':'same','workload_id':'W2','engine':'trino','size':'small','status':'passed','quality_status':'passed','runtime_seconds':1}
            result=scorecard(Path(directory),[run]*3)
            cell=next(c for c in result['matrix'] if c['workload_id']=='W2' and c['engine']=='trino' and c['size']=='small')
            self.assertEqual(cell['successful_repetitions'],1)
            self.assertEqual(cell['status'],'not_assessed')
            self.assertTrue(any(c['workers']==4 for c in result['matrix']))
            self.assertTrue(any(c['concurrency']==4 for c in result['matrix']))
            self.assertTrue(any(c['variant']=='catalog_unavailable' for c in result['matrix']))
            self.assertTrue(any(c['variant']=='duplicate_only' for c in result['matrix']))

    def test_preflight_keeps_size_and_preserves_resource_limit(self):
        from dpe.benchmark_calibration import assess
        result=assess({'files':[{'bytes':67108865}]},read_json(ROOT/'benchmarks/specs/acceptance.poc.v1.json'))
        self.assertEqual(result['processing_status'],'resource_limited')
        self.assertEqual(result['logical_input_bytes'],67108865)

    def test_duplicate_only_delivery_has_no_novel_incident_records(self):
        from dpe.common import canonical
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)/'sequence';generate_sequence(root,days=30,count=3)
            previous_events=set()
            for item in read_json(root/'sequence.json')['batches']:
                header,data,_=load_batch(root/item['path'])
                if 'duplicate_only' in item['scenarios']:
                    self.assertFalse(data['incident_snapshots'])
                    self.assertTrue(data['incident_events'])
                    self.assertTrue(all(canonical(r) in previous_events for r in data['incident_events']))
                previous_events.update(canonical(r) for r in data['incident_events'])

    def test_local_allocation_conserves_at_every_incremental_report_size(self):
        from decimal import Decimal
        from dpe.benchmark_runner import observed_usage
        from dpe.costing import calculate
        for count in range(1,41):
            runs=[{'logical_run_id':str(i),'attempt_id':str(i),'engine':'trino','workload_id':'W2','size':'smoke',
                   'workers':1,'concurrency':1,'logical_input_bytes':10,'status':'passed','runtime_seconds':1,
                   'started_at':'2026-10-22T00:00:00Z','finished_at':'2026-10-22T00:00:01Z','evidence':'test'} for i in range(count)]
            usage=observed_usage(runs,'test')
            self.assertEqual(sum(Decimal(v) for v in usage['pools'][0]['weights'].values()),1)
            if count in (11,19,28,40):
                result=calculate(usage)
                self.assertTrue(all(r['fully_allocated_cost'] is None for r in result['workloads']))
                self.assertTrue(all(r['allocated_platform_cost'] is None for r in result['workloads']))
