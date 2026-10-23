"""Exploratory Trino read concurrency on one frozen local shared input."""
import argparse
import hashlib
import random
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timezone
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from dpe.common import read_json,write_json
from dpe.lakehouse import Client,sql_revision
from dpe.ingestion import load_batch
from dpe.benchmark_workloads import expected,queries,equal
from dpe.costing import csv_rows


def run(workspace,output,repeats=3,allow_failed_experiment=False):
    workspace=Path(workspace);output=Path(output)
    if output.exists():raise ValueError('fresh output path required')
    output.mkdir(parents=True)
    source=read_json(workspace/'run_results.json')
    if source['status']!='passed' and not allow_failed_experiment:raise ValueError('complete input comparison required, or explicitly allow a failed experiment with committed preparation')
    if source['preparation']['evidence']['status']!='committed':raise ValueError('committed shared input preparation required')
    header,data,_=load_batch(workspace/'source/initial')
    answer=expected(data,header['cutoff'])['W2']
    sql=queries(source['preparation']['evidence']['schema'])['W2']
    schedule=[level for _ in range(repeats) for level in (1,2,4)];random.Random(42).shuffle(schedule)
    spec={'scope':'local exploratory W2; not required Medium/distributed/Azure matrix','revision':sql_revision(),
          'source_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
          'source_experiment_status':source['status'],'allow_failed_experiment':allow_failed_experiment,
          'input_manifest_checksum':source['runs'][0]['input_manifest_checksum'],'count':source['specification']['count'],
          'size':source['specification']['size'],'engine':'trino','workers':1,'concurrency_schedule':schedule,
          'cache_policy':'uncontrolled; no cold-cache claim','cost_basis':'local_measured_usage',
          'resources':source['specification']['resources']}
    write_json(output/'specification.json',spec)
    result={'status':'running','specification':spec,'blocks':[]}
    def execute(barrier):
        client=Client();barrier.wait();clock=datetime.now(timezone.utc).isoformat();begin=time.perf_counter()
        rows=client.sql(sql)
        if not equal(answer,rows):raise AssertionError('concurrent W2 output mismatch')
        return {'started_at':clock,'runtime_seconds':time.perf_counter()-begin,'query':client.queries[-1],'status':'passed'}
    try:
        for index,level in enumerate(schedule):
            barrier=threading.Barrier(level);begin=time.perf_counter()
            with ThreadPoolExecutor(max_workers=level) as executor:
                futures=[executor.submit(execute,barrier) for _ in range(level)]
                rows=[];failures=[]
                for future in futures:
                    try:rows.append(future.result())
                    except Exception as error:failures.append(str(error))
            elapsed=time.perf_counter()-begin
            result['blocks'].append({'block':index,'concurrency':level,'seconds':elapsed,
                'completed':len(rows),'throughput_per_second':len(rows)/elapsed,'attempts':rows,'failures':failures})
            write_json(output/'concurrency.json',result)
            if failures:raise RuntimeError(str(failures))
        summary=[]
        for level in (1,2,4):
            blocks=[b for b in result['blocks'] if b['concurrency']==level]
            summary.append({'concurrency':level,'repetitions':len(blocks),
                'median_throughput_per_second':statistics.median(b['throughput_per_second'] for b in blocks),
                'median_attempt_completion_seconds':statistics.median(a['runtime_seconds'] for b in blocks for a in b['attempts']),
                'fully_allocated_cost':None,'cost_status':'unpriced local usage'})
        result['summary']=summary;result['status']='passed'
        result['four_way_throughput_ratio']=summary[2]['median_throughput_per_second']/summary[0]['median_throughput_per_second']
        result['four_way_completion_ratio']=summary[2]['median_attempt_completion_seconds']/summary[0]['median_attempt_completion_seconds']
        result['gate_status']='not_assessed: exploratory local size; required Medium and cross-engine evidence missing'
        csv_rows(output/'concurrency.csv',summary)
        return result
    except Exception as error:result.update(status='failed',error=str(error));raise
    finally:write_json(output/'concurrency.json',result)

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--workspace',required=True);parser.add_argument('--out',required=True)
    parser.add_argument('--allow-failed-experiment',action='store_true')
    args=parser.parse_args();print(run(args.workspace,args.out,allow_failed_experiment=args.allow_failed_experiment)['status'])
