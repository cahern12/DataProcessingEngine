import copy
import multiprocessing
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from dpe.common import ROOT,read_json,write_json
from dpe.experiment import acceptance,admission,checksum,frozen,watchdog
from dpe.benchmark_reports import scorecard


def crash_owner(root,ready):
    with admission(Path(root)/'work',root):
        ready.set();os._exit(9)


class CapacityTests(unittest.TestCase):
    def test_jvm_reserved_committed_heap_and_resident_memory_remain_distinct(self):
        from dpe.capacity_reports import jvm_summary
        import json
        with tempfile.TemporaryDirectory() as directory:
            row={'native':{'stdout':'Total: reserved=4096KB, committed=2048KB\n- Java Heap (reserved=3072KB, committed=1024KB)\n'},
                 'heap':{'stdout':'garbage-first heap   total reserved 3072K, committed 1024K, used 512K'},
                 'os':{'stdout':'VmRSS:\t1536 kB\n'}}
            Path(directory,'jvm_samples.jsonl').write_text(json.dumps(row)+'\n')
            result=jvm_summary(directory);values=result['sampled_maxima']
            self.assertEqual(values['heap_used_bytes'],512*1024)
            self.assertEqual(values['os_process_rss_bytes'],1536*1024)
            self.assertEqual(values['nmt_total_committed_bytes'],2048*1024)
            self.assertEqual(result['nmt_categories']['Java Heap']['peak_committed_bytes'],1024*1024)
    def test_custom_success_cannot_be_presented_as_small_milestone_evidence(self):
        from benchmarks.assemble_capacity_evidence import assemble
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            result={'status':'passed','correctness_status':'passed','specification':{'count':10000,'size':'custom'}}
            write_json(root/'custom/run_results.json',result)
            with self.assertRaisesRegex(ValueError,'Small evidence'):
                assemble(root/'out',root/'custom',root/'custom')
    def test_airflow_json_survives_startup_logs_and_rejects_missing_result(self):
        from benchmarks.run_capacity_comparisons import airflow_runs
        rows=airflow_runs('2026-10-23 [info] initializing\n[{"run_id":"custom","state":"success"}]\n')
        self.assertEqual(rows[0]['state'],'success')
        self.assertEqual(airflow_runs('initializing\n[]\n'),[])
        with self.assertRaisesRegex(ValueError,'run list'):airflow_runs('[info] no result\n')
    def test_expired_query_history_uses_durable_events_without_inventing_total_memory(self):
        from dpe.lakehouse import query_telemetry
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            stats=dict(physicalInputBytes=12,physicalInputRows=3,processedInputBytes=20,processedInputRows=3,
                       writtenBytes=4,writtenRows=1,spilledBytes=0,cpuTime=.000001,wallTime=.2,peakUserMemoryBytes=9)
            write_json(root/'data/telemetry/trino-events/query.json',{'statistics':stats})
            with patch('dpe.lakehouse.ROOT',root),patch('urllib.request.urlopen',side_effect=OSError('expired')):
                result=query_telemetry('query')
            self.assertEqual(result['physicalInputDataSize'],'12B')
            self.assertEqual(result['totalCpuTime'],'0.000001000s')
            self.assertIsNone(result['peakTotalMemoryReservation'])
            self.assertEqual(result['source'],'durable_completed_event')
    def test_distinct_deliveries_are_not_pooled_into_comparable_repetitions(self):
        from dpe.benchmark_reports import performance
        with tempfile.TemporaryDirectory() as directory:
            runs=[{'workload_id':'W4','engine':'trino','size':'small','variant':variant,'input_manifest_checksum':variant,
                   'status':'passed','runtime_seconds':1} for variant in ('sparse_delta','late_correction','duplicate_only')]
            rows=performance(directory,runs)
            self.assertEqual(len(rows),3)
            self.assertTrue(all(r['ranking_status']=='insufficient_repetitions' for r in rows))
    def test_profile_preserves_business_limits_and_selects_real_small_budget(self):
        old=acceptance();new=acceptance('benchmarks/specs/acceptance.poc.v2.json')
        self.assertEqual(old['business_limits'],new['business_limits'])
        self.assertEqual(old['engineering_criteria'],new['engineering_criteria'])
        self.assertEqual(new['resources']['local_memory_gib'],16)
        self.assertGreater(new['resources']['maximum_local_logical_input_bytes'],113412943)
        self.assertNotEqual(checksum(old),checksum(new))
        for version in ('v3','v4','v5','v6','v7','v8'):
            revised=acceptance('benchmarks/specs/acceptance.poc.'+version+'.json')
            self.assertEqual(revised['business_limits'],old['business_limits'])
            self.assertEqual(revised['engineering_criteria'],old['engineering_criteria'])
    def test_frozen_status_uses_original_specification(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory);criteria=acceptance('benchmarks/specs/acceptance.poc.v2.json')
            criteria['business_limits']['runtime_seconds']['W2']['small']=1
            write_json(path/'specification.json',{'criteria':criteria})
            run={'attempt_id':'a','workload_id':'W2','engine':'trino','size':'small','status':'passed','quality_status':'passed','runtime_seconds':2}
            result=scorecard(path,[run])
            self.assertEqual(result['local_run_assessments'][0]['completion']['status'],'failed')
            self.assertEqual(result['business_limits'],criteria['business_limits'])
    def test_same_process_and_other_process_cannot_overlap_experiments(self):
        with tempfile.TemporaryDirectory() as directory:
            with admission(Path(directory)/'one',directory):
                with self.assertRaisesRegex(RuntimeError,'admission'):
                    with admission(Path(directory)/'two',directory):pass
            with admission(Path(directory)/'three',directory):pass
    def test_kernel_releases_admission_after_producer_crash(self):
        with tempfile.TemporaryDirectory() as directory:
            context=multiprocessing.get_context('fork');ready=context.Event()
            process=context.Process(target=crash_owner,args=(directory,ready));process.start()
            self.assertTrue(ready.wait(5));process.join(5);self.assertEqual(process.exitcode,9)
            with admission(Path(directory)/'next',directory):pass
            self.assertEqual(read_json(Path(directory)/'.benchmark-owner.json')['status'],'released')
    def test_watchdog_interrupts_and_restores_handler(self):
        import time,signal
        old=signal.getsignal(signal.SIGALRM)
        with self.assertRaises(TimeoutError):
            with watchdog(.01):time.sleep(.1)
        self.assertEqual(signal.getsignal(signal.SIGALRM),old)
    def test_required_observer_failure_retains_early_attempt(self):
        from dpe.benchmark_runner import run
        with tempfile.TemporaryDirectory() as directory,patch.dict(os.environ,{},clear=True):
            path=Path(directory)/'experiment'
            with self.assertRaisesRegex(ValueError,'DPE_RESOURCE_ENDPOINT'):
                run(path,count=1,repeats=1,acceptance_path='benchmarks/specs/acceptance.poc.v2.json')
            result=read_json(path/'run_results.json')
            self.assertEqual(result['status'],'failed');self.assertEqual(result['runs'][0]['workload_id'],'SOURCE')
            self.assertEqual(result['specification']['criteria']['version'],'v2')

    def test_python_bytecode_does_not_change_engine_fingerprint(self):
        from dpe.lakehouse import sql_revision
        folder=ROOT/'infra/local/__pycache__';folder.mkdir(exist_ok=True)
        path=folder/'capacity-fingerprint-test.pyc';before=sql_revision()
        try:
            path.write_bytes(b'not source code')
            self.assertEqual(before,sql_revision())
        finally:path.unlink()
    def test_public_prices_paginate_preserve_sources_and_reject_ambiguous_meters(self):
        from dpe.pricing import retrieve,select
        import json
        pages=[{'Items':[{'currencyCode':'USD','type':'Consumption','meterId':'one'}],'NextPageLink':'https://prices.azure.com/next'},
               {'Items':[{'currencyCode':'USD','type':'Consumption','meterId':'two'}],'NextPageLink':None}]
        with tempfile.TemporaryDirectory() as directory:
            result=retrieve(Path(directory)/'prices','example',fetch=lambda url:json.dumps(pages.pop(0)).encode())
            self.assertEqual(select(result,meterId='one')['meterId'],'one')
            with self.assertRaisesRegex(ValueError,'unique'):select(result,currencyCode='USD')
            self.assertEqual(read_json(Path(directory)/'prices/snapshot.json')['records'],2)
    def test_interrupted_producer_retains_failed_attempt_and_unpriced_costs(self):
        from dpe.experiment import interrupted
        from datetime import datetime,timezone
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);criteria=acceptance('benchmarks/specs/acceptance.poc.v2.json')
            write_json(root/'run_results.json',{'status':'running','specification':{'criteria':criteria,'size':'small','started_at':datetime.now(timezone.utc).isoformat()},'runs':[],
                       'current_attempt':{'workload_id':'W4','engine':'spark','started_at':datetime.now(timezone.utc).isoformat()}})
            interrupted(root,'producer OOM')
            result=read_json(root/'run_results.json');self.assertEqual(result['status'],'failed')
            self.assertEqual(result['runs'][0]['workload_id'],'W4')
            self.assertIsNone(read_json(root/'economics/cost_report.json')['workloads'][0]['fully_allocated_cost'])
