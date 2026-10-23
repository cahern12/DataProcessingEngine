import copy
import tempfile
import unittest
from pathlib import Path
from decimal import Decimal
from dpe.common import ROOT,read_json,write_json
from dpe.costing import calculate,charge,report

class CostingTests(unittest.TestCase):
    def setUp(self):
        self.rates=read_json(ROOT/'benchmarks/pricing/synthetic-fixture.v1.json')
        self.usage={'cost_basis':'modeled_cloud','currency':'USD','allocation_policy_version':'v1','required_components':['trino','catalog'],
            'runs':[{'logical_run_id':'a','engine':'trino','workload_id':'W2','logical_input_bytes':1000000000000},
                    {'logical_run_id':'b','engine':'spark','workload_id':'W2','logical_input_bytes':2000000000000}],
            'pools':[{'pool_id':'shared','window_id':'day','driver':'reserved_capacity_time','weights':{'a':'0.25','b':'0.75'}}],
            'lines':[self.line('active-a','trino','marginal','3',logical_run_id='a'),
                     self.line('retry-a','trino','marginal','1',logical_run_id='a',retry=True),
                     self.line('active-b','spark','marginal','2',logical_run_id='b'),
                     self.line('standing','catalog','standing','10',pool_id='shared')]}
    def line(self,key,component,classification,quantity,**kwargs):
        return dict(line_id=key,charge_id=key,resource_id=key,meter_id='node-hour',window_id='day',component=component,
                    classification=classification,quantity=quantity,unit='node_hour',rate_id='compute_hour',
                    started_at='2026-01-01T00:00:00Z',finished_at='2026-01-01T01:00:00Z',source_ref='test:hand-computed',**kwargs)
    def test_independently_computed_all_attempt_and_shared_costs(self):
        self.usage['required_components']=[]
        result=calculate(self.usage,self.rates)
        a,b=result['workloads']
        self.assertEqual(Decimal(a['marginal_cost']),8)
        self.assertEqual(Decimal(a['retry_cost']),2)
        self.assertEqual(Decimal(a['allocated_platform_cost']),5)
        self.assertEqual(Decimal(a['fully_allocated_cost']),13)
        self.assertEqual(Decimal(b['fully_allocated_cost']),19)
        self.assertEqual(Decimal(b['fully_allocated_cost_per_logical_tb']),Decimal('9.5'))
        self.assertEqual(Decimal(result['scope_total']),32)
        self.assertEqual(Decimal(result['allocated_total']),32)
    def test_rate_tiers_are_progressive(self):
        self.assertEqual(charge('15',self.rates['rates'][3]),Decimal('12.5'))
    def test_missing_rates_are_unavailable(self):
        result=calculate(self.usage)
        self.assertIsNone(result['scope_total']);self.assertIsNone(result['workloads'][0]['fully_allocated_cost'])
        self.assertEqual(result['cost_completeness'],'incomplete')
    def test_unknown_unit_rate_and_effective_date_are_rejected(self):
        for field,value in [('unit','cpu_seconds'),('started_at','2025-01-01T00:00:00Z')]:
            usage=copy.deepcopy(self.usage);usage['lines'][0][field]=value
            with self.assertRaises(ValueError): calculate(usage,self.rates)
    def test_duplicate_underlying_charge_cannot_be_counted_twice(self):
        duplicate=dict(self.usage['lines'][0],line_id='different-row')
        self.usage['lines'].append(duplicate)
        with self.assertRaisesRegex(ValueError,'duplicate'):calculate(self.usage,self.rates)
    def test_under_and_over_allocation_are_rejected(self):
        for weights in [{'a':'0.2','b':'0.2'},{'a':'1','b':'1'}]:
            self.usage['pools'][0]['weights']=weights
            with self.assertRaisesRegex(ValueError,'sum to one'):calculate(self.usage,self.rates)
    def test_idle_only_capacity_is_not_free(self):
        self.usage['pools'][0]['weights']={}
        result=calculate(self.usage,self.rates)
        self.assertEqual(Decimal(result['idle_only_total']),20)
        self.assertEqual(result['cost_completeness'],'incomplete')
    def test_zero_input_has_no_fake_unit_cost(self):
        self.usage['runs'][0]['logical_input_bytes']=0
        result=calculate(self.usage,self.rates)
        self.assertIsNone(result['workloads'][0]['fully_allocated_cost_per_logical_tb'])
        self.assertIn('zero',result['workloads'][0]['unit_cost_reason'])
    def test_billed_and_amortized_costs_cannot_be_mixed(self):
        usage=copy.deepcopy(self.usage);usage['cost_basis']='azure_provisional'
        for line in usage['lines']:line.update(billed_amount='1',billing_basis='actual')
        usage['lines'][0]['billing_basis']='amortized'
        usage['billing_scope']={'currency':'USD','basis':'actual','source_ref':'fixture-export','total':'4'}
        with self.assertRaisesRegex(ValueError,'mixed'):calculate(usage)
    def test_reconciled_label_requires_real_reconciliation_evidence(self):
        usage=copy.deepcopy(self.usage);usage['cost_basis']='azure_reconciled'
        for line in usage['lines']:line.update(billed_amount='1',billing_basis='actual')
        with self.assertRaisesRegex(ValueError,'reconciliation'):calculate(usage)
        usage['billing_scope']={'currency':'USD','basis':'actual','source_ref':'fixture-export','total':'4'}
        self.assertEqual(calculate(usage)['reconciliation']['status'],'passed')
        usage['billing_scope']['total']='5'
        with self.assertRaises(ValueError):calculate(usage)
    def test_report_component_rows_equal_workload_totals(self):
        self.usage['required_components']=[]
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory);write_json(path/'usage.json',self.usage);write_json(path/'rates.json',self.rates)
            report(path/'usage.json',path/'out',path/'rates.json')
            value=read_json(path/'out/cost_report.json')
            for run in value['workloads']:
                actual=sum(Decimal(c['fully_allocated_cost']) for c in value['technology'] if c['logical_run_id']==run['logical_run_id'] and c['fully_allocated_cost'] is not None)
                self.assertEqual(actual,Decimal(run['fully_allocated_cost']))
    def test_nonfinite_negative_and_bool_usage_rejected(self):
        for bad in ['NaN','Infinity','-1',True]:
            self.usage['lines'][0]['quantity']=bad
            with self.assertRaises(ValueError):calculate(self.usage,self.rates)

class CostImportTests(unittest.TestCase):
    def test_explicit_billing_import_has_provenance_and_cannot_hide_unknown_resources(self):
        from dpe.cost_import import import_details
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            (root/'bill.csv').write_text('id,resource,meter,currency,amount\nx,vm-a,compute,USD,12.50\n')
            mapping={'billing_basis':'actual','currency':'USD','window_id':'day','allocation_policy_version':'v1',
                'started_at':'2026-01-01T00:00:00Z','finished_at':'2026-01-02T00:00:00Z',
                'columns':{'charge_id':'id','resource_id':'resource','meter_id':'meter','currency':'currency','amount':'amount'},
                'resources':{'vm-a':{'component':'trino','classification':'marginal','logical_run_id':'a','category':'compute'}},
                'runs':[{'logical_run_id':'a','logical_input_bytes':1000000000000}]}
            write_json(root/'mapping.json',mapping)
            import_details(root/'bill.csv',root/'mapping.json',root/'usage.json')
            usage=read_json(root/'usage.json');result=calculate(usage)
            self.assertEqual(result['workloads'][0]['fully_allocated_cost'],'12.50')
            self.assertEqual(result['cost_basis'],'azure_provisional')
            self.assertTrue(usage['import_provenance']['usage_sha256'])
            mapping['resources']={};write_json(root/'mapping.json',mapping)
            with self.assertRaisesRegex(ValueError,'unmapped'):import_details(root/'bill.csv',root/'mapping.json',root/'bad.json')
    def test_missing_required_component_prevents_complete_workload_cost(self):
        test=CostingTests();test.setUp();test.usage['required_components']=['monitoring']
        result=calculate(test.usage,test.rates)
        self.assertIsNone(result['workloads'][0]['fully_allocated_cost'])
        self.assertEqual(result['cost_completeness'],'incomplete')

    def test_frequency_sensitivity_includes_standing_idle_cost(self):
        from dpe.economic_curves import demand_sensitivity
        criteria=read_json(ROOT/'benchmarks/specs/acceptance.poc.v1.json')
        report={'cost_basis':'modeled_cloud','currency':'USD','ledger':[{'classification':'standing','amount':'24'}],
                'workloads':[{'workload_id':'W4','engine':'trino','size':size,'marginal_cost':price,'cost_completeness':'complete'}
                             for size,price in [('small','0.5'),('medium','2'),('large','10')]]}
        rows=demand_sensitivity(report,{'accounting_window_hours':24},criteria)
        base=next(r for r in rows if r['runs_per_day']==24)
        self.assertEqual(Decimal(base['marginal_monthly_cost']),Decimal('525.60'))
        self.assertEqual(Decimal(base['standing_monthly_cost']),720)
        self.assertEqual(Decimal(base['total_monthly_cost']),Decimal('1245.60'))
        report['workloads'].pop()
        self.assertTrue(all(r['total_monthly_cost'] is None for r in demand_sensitivity(report,{'accounting_window_hours':24},criteria)))

    def test_invalid_quantities_and_mixed_billing_rejected_without_prices_or_scope(self):
        test=CostingTests();test.setUp()
        test.usage['lines'][0]['quantity']='-1'
        with self.assertRaisesRegex(ValueError,'negative'):calculate(test.usage)
        test.setUp();test.usage['cost_basis']='azure_provisional'
        for line in test.usage['lines']:line.update(billed_amount='1',billing_basis='actual')
        test.usage['lines'][0]['billing_basis']='amortized'
        with self.assertRaisesRegex(ValueError,'mixed'):calculate(test.usage)

    def test_tier_threshold_cannot_be_restarted_for_each_workload(self):
        test=CostingTests();test.setUp();usage=test.usage
        usage['lines']=usage['lines'][:2]
        for index,line in enumerate(usage['lines']):
            line.update(resource_id='same-store',meter_id='egress',rate_id='tiered',unit='GB',quantity='10')
        with self.assertRaisesRegex(ValueError,'aggregated'):calculate(usage,test.rates)
