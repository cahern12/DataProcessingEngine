import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from dpe.common import ROOT,read_json,write_json
from dpe.cloud_model import catalog,model,weights
from dpe.experiment import acceptance

SNAPSHOT=ROOT/'benchmarks/pricing/azure-eastus-2026-10-22'


class CloudModelTests(unittest.TestCase):
    def example(self,root):
        criteria=acceptance('benchmarks/specs/acceptance.poc.v2.json')
        write_json(root/'specification.json',{'criteria':criteria})
        runs=[]
        for index,engine in enumerate(('trino','spark')):
            runs.append({'logical_run_id':engine,'attempt_id':engine,'workload_id':'W4','engine':engine,'size':'small',
                'workers':1,'concurrency':1,'status':'failed' if index else 'passed','logical_input_bytes':100000000,
                'started_at':'2026-10-23T04:00:%02dZ'%(index*20),'finished_at':'2026-10-23T04:00:%02dZ'%(index*20+19),
                'runtime_seconds':19})
        write_json(root/'run_results.json',{'runs':runs,'status':'failed'})
        observed=root/'observations'
        before=[{'bucket':'warehouse','key':'protected','bytes':2**30,'last_modified':'2026-10-23T01:00:00Z'}]
        after=before+[{'bucket':'warehouse','key':'trino-table','bytes':2**30,'last_modified':'2026-10-23T04:00:05Z'},
                      {'bucket':'warehouse','key':'failed-spark-orphan','bytes':2**30,'last_modified':'2026-10-23T04:00:25Z'}]
        write_json(observed/'before.json',before);write_json(observed/'after.json',after)
        write_json(observed/'storage_samples.json',[{'at':'2026-10-23T04:00:00Z','bytes':2**30,'evidence':'before.json'},
                    {'at':'2026-10-23T04:00:40Z','bytes':3*2**30,'evidence':'after.json'}])

    def test_exact_public_meters_convert_transaction_units_and_capacity_tiers(self):
        rates={r['id']:r for r in catalog(SNAPSHOT)['rates']}
        self.assertEqual(Decimal(rates['write']['tiers'][0]['price'])*10000,Decimal('.065'))
        self.assertEqual(rates['node']['provider_rows'][0]['armSkuName'],'Standard_D4as_v5')
        self.assertEqual(len(rates['capacity']['tiers']),3)
        self.assertEqual(rates['nat']['provider_rows'][0]['armRegionName'],'Global')

    def test_shared_node_is_charged_once_and_failed_orphans_remain_costed(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);self.example(root/'source')
            summary=model(root/'source',SNAPSHOT,root/'model')
            report=read_json(root/'model/cost_report.json');usage=read_json(root/'model/usage.json')
            self.assertEqual(Decimal(report['scope_total']),Decimal(report['allocated_total']))
            self.assertEqual(report['cost_completeness'],'complete')
            node_hours=sum(Decimal(r['quantity']) for r in usage['lines'] if r['resource_id']=='application-node')
            self.assertAlmostEqual(node_hours,Decimal(39)/3600,places=25)
            self.assertEqual(summary['storage']['assigned']['spark']['bytes'],2**30)
            failed=next(r for r in report['workloads'] if r['engine']=='spark')
            self.assertGreater(Decimal(failed['failed_attempt_cost_usd']),0)
            self.assertEqual(failed['measurement_completeness'].split(':')[0],'incomplete')
            self.assertGreater(Decimal(failed['allocated_platform_cost']),Decimal(failed['attributed_active_compute_cost']))
            self.assertEqual(summary['actual_azure_cost_status'],'unavailable')

    def test_aks_inventory_and_trino_only_comparison_do_not_duplicate_application_nodes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);self.example(root/'source')
            vm=model(root/'source',SNAPSHOT,root/'vm',spark_retained=False)
            aks=model(root/'source',SNAPSHOT,root/'aks','aks',False)
            self.assertGreater(Decimal(aks['standing_monthly_usd']),Decimal(vm['standing_monthly_usd']))
            self.assertEqual(next(r['count'] for r in aks['inventory'] if r['resource']=='system-pool'),2)
            self.assertEqual(next(r['count'] for r in aks['inventory'] if r['resource']=='application-node'),1)
            report=read_json(root/'vm/cost_report.json')
            self.assertEqual({r['engine'] for r in report['workloads']},{'trino'})
            self.assertEqual(Decimal(report['scope_total']),Decimal(report['allocated_total']))
            self.assertEqual(aks['original_mix_projection']['status'],'unavailable')

    def test_absent_storage_observation_is_refused_and_weight_remainders_conserve(self):
        self.assertEqual(sum(weights({'a':Decimal(1),'b':Decimal(1),'c':Decimal(1)}).values()),1)
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);self.example(root/'source')
            write_json(root/'source/observations/storage_samples.json',[])
            with self.assertRaisesRegex(ValueError,'inventories'):model(root/'source',SNAPSHOT,root/'model')

    def test_allocation_sensitivity_changes_shares_and_conserves_each_scenario(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);self.example(root/'source')
            for variant in ('low','base','high'):
                model(root/'source',SNAPSHOT,root/variant,variant=variant)
                result=read_json(root/variant/'cost_report.json')
                self.assertEqual(Decimal(result['scope_total']),Decimal(result['allocated_total']))
            base=read_json(root/'base/usage.json')['pools'][0]['weights']
            high=read_json(root/'high/usage.json')['pools'][0]['weights']
            self.assertNotEqual(base,high)
            self.assertGreater(Decimal(high['trino']),Decimal(high['spark']))
