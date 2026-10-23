"""Local comparative workload runner. Original Azure gates remain explicit gaps."""
import hashlib
import os
import random
import time
import uuid
from datetime import datetime,timezone
from pathlib import Path
from .common import DATASETS,ROOT,canonical,digest,iso,read_json,write_json,validate_schema
from .lakehouse import Client,OUTPUTS,compare_rows,consume,sql_revision
from . import lakehouse
from .engines import spark
from .benchmark_workloads import queries,expected,equal
from .benchmark_telemetry import spark_events
from .benchmark_reports import scorecard,performance
from .costing import report as cost_report,COMPONENTS
from .synthetic import generate
from .ingestion import load_batch
from .rules import evaluate


def observed_usage(runs,window):
    """Expose all components without fabricating cloud prices or request quantities."""
    usage={'version':'v1','cost_basis':'local_measured_usage','currency':'USD','allocation_policy_version':'local-unpriced.v1',
           'runs':[{'logical_run_id':r['logical_run_id'],'workload_id':r['workload_id'],'engine':r['engine'],
                    'logical_input_bytes':r['logical_input_bytes'],'size':r['size'],'workers':r['workers'],'concurrency':r['concurrency'],'attempt_ids':[r['attempt_id']],'status':r['status']} for r in runs],
           'lines':[],'pools':[],'required_components':[c for c in COMPONENTS if c!='reference']}
    if not runs:return usage
    from decimal import Decimal
    # Fixed decimal units keep every intermediate sum exact; repeated 1/N can
    # otherwise round differently when summed versus multiplied (e.g. N=11).
    share=Decimal(10**18//len(runs))/Decimal(10**18);weights={r['logical_run_id']:str(share) for r in runs}
    weights[runs[-1]['logical_run_id']]=str(Decimal(1)-share*(len(runs)-1))
    usage['pools']=[{'pool_id':'local-standing','window_id':window,'driver':'equal run count; monetary allocation unavailable','weights':weights}]
    for run in runs:
        key=run['logical_run_id']+'-'+run['engine']
        usage['lines'].append({'line_id':key,'charge_id':key,'resource_id':'local-'+run['engine'],'meter_id':'unpriced-active-wall-time',
            'window_id':window,'component':run['engine'],'category':'unpriced_local_usage','classification':'marginal',
            'logical_run_id':run['logical_run_id'],'quantity':str(run['runtime_seconds']),'unit':'resource_second','rate_id':None,
            'started_at':run['started_at'],'finished_at':run['finished_at'],'active_capacity':True,
            'source_ref':run['evidence'],'retry':run.get('retry',False),
            'unavailable_reason':'Wall time is diagnostic; marginal cash cost and Azure rates are unavailable'})
    for component in COMPONENTS:
        if component=='reference':continue
        key='standing-'+component
        usage['lines'].append({'line_id':key,'charge_id':key,'resource_id':'local-'+component,'meter_id':'unpriced-standing-capacity',
            'window_id':window,'component':component,'category':'unpriced_local_standing','classification':'standing',
            'pool_id':'local-standing','quantity':None,'unit':'resource_second','rate_id':None,
            'started_at':min(r['started_at'] for r in runs),'finished_at':max(r['finished_at'] for r in runs),
            'source_ref':'unpriced experiment standing scope: '+window,
            'unavailable_reason':'Standing and idle capacity, storage, orchestration and network prices/quantities unavailable'})
    return usage


def validate_outputs(platform,datasets,header):
    oracle=evaluate(datasets,header['cutoff'],header['report_start']);differences=[]
    for name in OUTPUTS:differences+=compare_rows(name,oracle[name],consume(platform,name)['rows'])
    if differences:raise AssertionError('full rebuild differences: '+canonical(differences[:5]))
    return {'status':'passed','differences':[]}


def run(workspace,count=1000,repeats=3,engines=('trino','spark'),orchestration=None):
    workspace=Path(workspace).resolve()
    if workspace.exists():raise ValueError('benchmark workspace exists; choose a fresh path')
    if repeats<1 or count<1:raise ValueError('positive count/repeats required')
    if set(engines)-{'trino','spark'}:raise ValueError('unsupported engine')
    workspace.mkdir(parents=True)
    criteria=read_json(ROOT/'benchmarks/specs/acceptance.poc.v1.json')
    spec={'version':'v1','scope':'local comparative experiments; not an Azure cost/performance claim',
          'revision':sql_revision(),'criteria':criteria,'criteria_sha256':hashlib.sha256(canonical(criteria).encode()).hexdigest(),
          'count':count,'repeats':repeats,'engines':list(engines),'resources':criteria['resources'],
          'cache_policy':criteria['cache_policy'],'orchestration':orchestration or {'kind':'direct'},
          'started_at':iso(datetime.now(timezone.utc)),'size':'smoke' if count==1000 else 'small' if count==100000 else 'medium' if count==1000000 else 'large' if count==10000000 else 'custom'}
    write_json(workspace/'specification.json',spec)
    pending=None
    runs=[];result={'status':'running','specification':spec,'runs':runs,'findings':{}}
    started=time.perf_counter()
    def persist():
        write_json(workspace/'run_results.json',result)
        performance(workspace,runs);scorecard(workspace,runs,result['findings'])
        usage=observed_usage(runs,workspace.name);write_json(workspace/'usage.json',usage)
        if runs:cost_report(workspace/'usage.json',workspace/'economics')
    def row(workload,engine,elapsed,artifact,details=None,start=None):
        item={'logical_run_id':uuid.uuid4().hex,'attempt_id':uuid.uuid4().hex,'workload_id':workload,'engine':engine,
            'output_rows':(details or {}).get('output_rows'),'engine_version':(details or {}).get('engine_version'),'strategy':'full','size':spec['size'],'workers':1,'concurrency':1,
            'started_at':start or iso(datetime.now(timezone.utc)),'finished_at':(details or {}).get('finished_at',iso(datetime.now(timezone.utc))),
            'runtime_seconds':elapsed,'logical_input_bytes':input_bytes,'input_manifest_checksum':checked['manifest_sha256'],
            'status':'passed','quality_status':'passed','evidence':str(artifact),'details':details or {}}
        validate_schema(item,read_json(ROOT/'contracts/benchmark/run.v1.schema.json'))
        runs.append(item);persist();return item
    try:
        generate(workspace/'source',count=count)
        from .benchmark_calibration import assess
        preflight=assess(read_json(workspace/'source/initial/manifest.json'),criteria)
        write_json(workspace/'capacity_preflight.json',preflight)
        if preflight['processing_status']!='eligible':raise ValueError('resource_limited: frozen local input budget exceeded; see capacity_preflight.json')
        header,datasets,checked=load_batch(workspace/'source/initial')
        input_bytes=sum(f['bytes'] for f in header['files'])
        # A single frozen shared Silver dataset gives both read engines the same physical files.
        preparation=lakehouse.run(workspace/'source/initial',workspace/'shared')
        shared_schema=preparation['schema']
        result['preparation']={'evidence':preparation,'scope':'shared input preparation, separate from isolated W1-W3 execution'}
        row('PREPARATION','trino',preparation['runtime_seconds'],workspace/'shared/active.json',{'boundary':'shared preparation; charged once separately'},preparation['started_at'])
        expected_rows=expected(datasets,header['cutoff'])
        schedule=[]
        for repeat in range(repeats):
            order=list(engines);random.Random(42+repeat).shuffle(order)
            schedule.append(order)
        spec['engine_order']=schedule;write_json(workspace/'specification.json',spec)
        for repeat,order in enumerate(schedule):
            for engine in order:
                if engine=='trino':
                    client=Client();version=client.sql('SELECT version() version')[0]['version']
                    for name,sql in queries(shared_schema).items():
                        pending=(name,engine,time.perf_counter(),iso(datetime.now(timezone.utc)),workspace/('read-%03d-trino-%s.json'%(repeat,name)))
                        clock=iso(datetime.now(timezone.utc));begin=time.perf_counter();actual=client.sql(sql);elapsed=time.perf_counter()-begin
                        if not equal(expected_rows[name],actual):raise AssertionError('isolated '+name+' Trino mismatch')
                        artifact=workspace/('read-%03d-trino-%s.json'%(repeat,name));write_json(artifact,{'rows':actual,'query':client.queries[-1],'sql':sql})
                        row(name,engine,elapsed,artifact,{'engine_version':version,'query':client.queries[-1],'output_rows':len(actual)},clock)
                else:
                    folder=workspace/('read-%03d-spark'%repeat)
                    pending=('W1-W3',engine,time.perf_counter(),iso(datetime.now(timezone.utc)),folder/'result.json')
                    submitted=iso(datetime.now(timezone.utc));read_result=spark.execute({'action':'queries','queries':queries(shared_schema,'spark')},folder)
                    telemetry=spark_events(folder/'events')
                    for name,data in read_result['queries'].items():
                        if not equal(expected_rows[name],data['rows']):raise AssertionError('isolated '+name+' Spark mismatch')
                        row(name,engine,data['runtime_seconds'],folder/'result.json',{'engine_version':read_result['engine_version'],
                            'output_rows':len(data['rows']),'application_id':read_result['application_id'],
                            'application_telemetry':telemetry,'telemetry_boundary':'shared Spark application across W1-W3; do not add its totals three times',
                            'submission_runtime_seconds':read_result['submission_runtime_seconds'],'finished_at':data['finished_at']},data['started_at'])
                    overhead=max(0,read_result['submission_runtime_seconds']-sum(q['runtime_seconds'] for q in read_result['queries'].values()))
                    row('APPLICATION_OVERHEAD',engine,overhead,folder/'result.json',{'boundary':'Spark submission/setup/shutdown; one share per application, not repeated per query'},submitted)
                clock=iso(datetime.now(timezone.utc));begin=time.perf_counter();platform=workspace/('full-%03d-%s'%(repeat,engine))
                pending=('W4',engine,time.perf_counter(),clock,platform)
                full=(lakehouse.run if engine=='trino' else spark.run)(workspace/'source/initial',platform)
                execution_seconds=time.perf_counter()-begin
                validation=validate_outputs(platform,datasets,header)
                elapsed=time.perf_counter()-begin
                artifact=workspace/('full-%03d-%s.json'%(repeat,engine));write_json(artifact,full)
                details={'engine_attempt_id':full['attempt_id'],'execution_seconds':execution_seconds,'validation':validation,'publication_id':full['publication_id'],'snapshots':full['snapshots'],
                         'engine_version':full.get('engine_version',full.get('spark',{}).get('engine_version'))}
                if engine=='spark':details['telemetry']=spark_events(platform/'spark'/full['attempt_id']/'events')
                row('W4',engine,elapsed,artifact,details,clock)
                pending=None
        result['findings']={'local_engine_equality':'W1-W4 exact comparisons passed','repetitions':repeats,
            'costs':'per-workload and per-technology outputs emitted; local monetary amounts unavailable',
            'current_state_reference':'Python/SQLite is a correctness harness; organization current-state system not supplied',
            'capacity':'Actual engine worker/executor scaling and distributed W5 M/L remain not assessed',
            'business_limits':'adopted POC targets; Smoke cannot establish the larger-scale/Azure verdict'}
        result['status']='passed'
        if sql_revision()!=spec['revision']:raise ValueError('code changed during experiment; results not comparable')
        return {'status':'passed','runs':len(runs),'report':str(workspace/'run_results.json'),'economics':str(workspace/'economics/cost_report.json'),'poc_verdict':'incomplete'}
    except Exception as error:
        if pending:
            workload,engine,begin,clock,artifact=pending
            runs.append({'logical_run_id':uuid.uuid4().hex,'attempt_id':uuid.uuid4().hex,'workload_id':workload,'engine':engine,
                'strategy':'full','size':spec['size'],'workers':1,'concurrency':1,'status':'failed','quality_status':'failed',
                'output_rows':None,'started_at':clock,'finished_at':iso(datetime.now(timezone.utc)),'runtime_seconds':time.perf_counter()-begin,
                'logical_input_bytes':input_bytes,'input_manifest_checksum':checked['manifest_sha256'],'evidence':str(artifact),'error':str(error)})
        result.update(status='failed',error=str(error));raise
    finally:
        result['runtime_seconds']=time.perf_counter()-started
        persist()
