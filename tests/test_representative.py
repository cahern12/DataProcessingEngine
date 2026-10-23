import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path
from dpe.bounded_io import sources
from dpe.common import DATASETS,read_json
from dpe.representative import deliveries
from dpe.rules import evaluate
from dpe.lakehouse import compare_rows


class RepresentativeTests(unittest.TestCase):
    def test_fixture_rejects_missing_duplicate_rule_even_when_counts_and_gold_match(self):
        from dpe.engines import spark
        case=next(c for c in read_json('synthetic/fixtures/rule_cases.v1.json')['cases']
                  if c['name']=='duplicate_reference_rows_keep_rule_id')
        result={'outputs':{'incident_resolution':[dict(case['expected_incident'],case_id=case['name'])],
                           'service_daily_metrics':[dict(r,case_id=case['name']) for r in case.get('expected_daily',[])]},
                'quarantine':[],'reconciliation':[{'case_id':case['name'],'dataset':'users','ordinal':i,
                    'disposition':'duplicate','rule_id':'BR-002'} for i in range(case['expected_duplicate_count'])]}
        result['reconciliation'][0]['rule_id']=None
        with tempfile.TemporaryDirectory() as directory,patch.object(spark,'read_json',return_value={'cases':[case]}),patch.object(spark,'execute',return_value=result):
            with self.assertRaisesRegex(AssertionError,'duplicate must retain BR-002'):
                spark.fixtures(Path(directory))
            result['reconciliation'][0]['rule_id']='BR-002'
            self.assertEqual(spark.fixtures(Path(directory))['status'],'passed')

    def test_delivery_cases_have_independently_known_changes_and_exact_parent_lineage(self):
        with tempfile.TemporaryDirectory() as directory:
            cases=deliveries(Path(directory),100);paths=[];previous=None;parent=None
            for name,path,changed in cases:
                paths.append(path);header,records,_,_=sources(paths)
                self.assertEqual(header['parent_batch_id'],parent);parent=header['batch_id']
                result=evaluate(records,header['cutoff'],header['report_start'])
                self.assertEqual(len(result['incident_resolution']),100)
                self.assertEqual(result['quarantined_incidents'],{})
                if previous:
                    differences=compare_rows('incident_resolution',previous['incident_resolution'],result['incident_resolution'])
                    self.assertEqual({r['key'][0] for r in differences},set(changed),name)
                    if name in ('duplicate_only','future_deferred'):
                        self.assertEqual(compare_rows('service_daily_metrics',previous['service_daily_metrics'],result['service_daily_metrics']),[])
                if name=='future_deferred':self.assertEqual(sum(r['disposition']=='deferred' for r in result['dispositions']),2)
                if name=='open_heavy_cutoff':
                    manifest=read_json(path/'manifest.json')
                    self.assertEqual(sum(f['rows'] for f in manifest['files'] if f['dataset'].startswith('incident_')),0)
                    self.assertGreater(sum(r['state'] not in ('resolved','closed') for r in result['incident_resolution']),75)
                previous=result
