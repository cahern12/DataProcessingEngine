import tempfile
import unittest
from pathlib import Path
from dpe.bounded_io import sources
from dpe.common import DATASETS,read_json
from dpe.representative import deliveries
from dpe.rules import evaluate
from dpe.lakehouse import compare_rows


class RepresentativeTests(unittest.TestCase):
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
