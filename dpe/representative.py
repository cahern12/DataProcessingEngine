"""Serial, independently checked delivery and recovery cases on a fixed population."""
import copy
import hashlib
import time
import uuid
from datetime import datetime,timedelta,timezone
from pathlib import Path

from . import lakehouse,incremental
from .engines import spark
from .bounded_io import profile,sources,release
from .common import ROOT,DATASETS,canonical,read_json,write_json,iso,timestamp
from .experiment import acceptance,admission,watchdog
from .experiment_observer import Observer
from .synthetic import generate,incident
from .incremental_validation import batch
from .benchmark_runner import validate_outputs,observed_usage
from .benchmark_reports import scorecard,performance


def deliveries(workspace,count):
    workspace=Path(workspace)
    config=read_json(ROOT/'synthetic/configs/default.json')
    config.update(scenario_weights={'resolved':.1,'open':.5,'hold':.3,'reopened':.1},delta_fraction=0,new_fraction=0,allow_empty_delta=True)
    write_json(workspace/'generator.json',config)
    config=read_json(workspace/'generator.json')
    generate(workspace/'source',profile='small' if count==100000 else 'smoke',count=count,config_path=workspace/'generator.json')
    initial=workspace/'source/initial';header,records,_,_=sources([initial])
    refs={name:list(records[name]) if name not in ('incident_snapshots','incident_events') else [] for name in DATASETS}
    cutoff=header['cutoff'];parent=header['batch_id'];result=[('initial',initial,[])]
    def save(name,changes,changed):
        nonlocal parent
        target=workspace/'source'/name
        batch(target,name,parent,cutoff,changes,header['report_start']);parent=name
        result.append((name,target,changed))
    sparse=copy.deepcopy(refs);snapshots,events=incident(config,0);last=snapshots[-1]
    sparse['incident_snapshots']=[dict(last,source_version=3,source_updated_at=iso(timestamp(last['source_updated_at'])+timedelta(seconds=1)),
                                      service_id='SVC-001' if last['service_id']!='SVC-001' else 'SVC-000')]
    save('sparse_delta',sparse,[last['incident_id']])
    late=copy.deepcopy(refs);changed=[]
    for index in range(1,min(count-1,max(2,count//100))):
        snapshots,events=incident(config,index);last=snapshots[-1];changed.append(last['incident_id'])
        late['incident_snapshots'].append(dict(last,source_version=3,source_updated_at=iso(timestamp(last['source_updated_at'])+timedelta(seconds=2)),
                                              service_id='SVC-001' if last['service_id']!='SVC-001' else 'SVC-000'))
        late['incident_events'].append(dict(events[-1],event_id=last['incident_id']+'-LATE-COMMENT',event_type='comment',
                                           event_time=iso(timestamp(cutoff)-timedelta(hours=1)),source_sequence=len(events)+10))
    save('late_correction',late,changed)
    duplicate=copy.deepcopy(refs);snapshots,events=incident(config,0)
    duplicate['incident_snapshots']=[snapshots[0]];duplicate['incident_events']=[events[0]]
    save('duplicate_only',duplicate,[])
    future=copy.deepcopy(refs)
    for index in range(count-1,-1,-1):
        snapshots,events=incident(config,index)
        if snapshots[-1]['status']=='in_progress':break
    else:raise ValueError('future control requires an in-progress incident')
    last=snapshots[-1];when=iso(timestamp(cutoff)+timedelta(hours=12))
    future['incident_snapshots']=[dict(last,source_version=99,status='resolved',source_updated_at=when)]
    future['incident_events']=[dict(events[-1],event_id=last['incident_id']+'-FUTURE-RESOLVE',event_type='resolve',event_time=when,source_sequence=len(events)+10)]
    save('future_deferred',future,[])
    cutoff=iso(timestamp(cutoff)+timedelta(days=1))
    save('open_heavy_cutoff',copy.deepcopy(refs),[last['incident_id']])
    return result


def _run(workspace,count=100000,acceptance_path='benchmarks/specs/acceptance.poc.v4.json',strategies=('trino','spark','targeted')):
    workspace=Path(workspace).resolve()
    if workspace.exists():raise ValueError('representative workspace exists')
    criteria=acceptance(acceptance_path);workspace.mkdir(parents=True)
    spec={'version':'v1','criteria':criteria,'count':count,'size':'small' if count==100000 else 'smoke' if count==1000 else 'custom',
          'revision':lakehouse.sql_revision(),'started_at':iso(datetime.now(timezone.utc)),'criteria_sha256':hashlib.sha256(canonical(criteria).encode()).hexdigest(),
          'resources':criteria['resources'],'cache_policy':criteria['cache_policy'],'scope':'Additional local open-heavy delivery/recovery cases; one attempt per case, not repeated baseline or distributed W5 coverage'}
    write_json(workspace/'specification.json',spec)
    started=time.perf_counter()
    observer=Observer(workspace,criteria);report={'status':'running','specification':spec,'runs':[],'recovery':[],'cases':[]};all_sources=[]
    def persist():
        lakehouse.atomic_json(workspace/'run_results.json',report)
        write_json(workspace/'representative.json',report)
    def measured(name,engine,platform,execute,path,fail_at=None):
        begin=time.perf_counter();clock=iso(datetime.now(timezone.utc));details={};status='failed'
        report['current_attempt']={'engine':'trino' if engine=='targeted' else engine,'workload_id':'W5' if fail_at else 'W4','started_at':clock};persist()
        observer.mark('full-'+name+'-'+engine+('-'+fail_at if fail_at else ''))
        try:
            with watchdog(criteria['operational_watchdogs_seconds']['full']):details=execute(path,platform,fail_at=fail_at)
            if fail_at:raise AssertionError('expected injected publication failure')
            header,data,_,_=sources(all_sources,verify=False)
            validation=validate_outputs(platform,data,header)
            details['independent_validation']=validation;status='passed';return details
        except RuntimeError as error:
            if fail_at and 'injected failure: '+fail_at in str(error):details={'expected_failure':fail_at,'error':str(error)};return details
            raise
        finally:
            logical=sum(sum(f['bytes'] for f in read_json(p/'manifest.json')['files']) for p in all_sources)
            report['runs'].append({'logical_run_id':uuid.uuid4().hex,'attempt_id':uuid.uuid4().hex,'workload_id':'W5' if fail_at else 'W4','variant':name+'-'+fail_at if fail_at else name,
                'engine':'trino' if engine=='targeted' else engine,'strategy':'targeted' if engine=='targeted' else 'full','size':spec['size'],'workers':1,'concurrency':1,
                'started_at':clock,'finished_at':iso(datetime.now(timezone.utc)),'runtime_seconds':time.perf_counter()-begin,
                'logical_input_bytes':logical,'input_manifest_checksum':hashlib.sha256(canonical([read_json(p/'manifest.json') for p in all_sources]).encode()).hexdigest(),
                'status':status,'quality_status':'passed' if status=='passed' else 'failed','failure_injection':fail_at,'evidence':str(platform),'details':details})
            persist();release()
    try:
        with profile(criteria):
            observer.start();observer.mark('source_generation')
            cases=deliveries(workspace,count);previous={}
            from .benchmark_calibration import assess
            preflight=assess(read_json(cases[0][1]/'manifest.json'),criteria)
            write_json(workspace/'capacity_preflight.json',preflight)
            if preflight['processing_status']!='eligible':raise ValueError('representative source exceeds frozen input budget')
            for name,path,changed in cases:
                all_sources.append(path);entry={'variant':name,'expected_changed_incident_keys':changed,'engines':{}};report['cases'].append(entry)
                for engine in strategies:
                    execute={'trino':lakehouse.run,'spark':spark.run,'targeted':incremental.run}[engine];platform=workspace/engine
                    old=read_json(platform/'active.json') if (platform/'active.json').exists() else None
                    before=(platform/'active.json').read_bytes() if old else None
                    if name=='sparse_delta' and engine in ('trino','spark'):
                        for fail in ('between_gold_writes','before_publish'):
                            failed=measured(name,engine,platform,execute,path,fail)
                            if before!=(platform/'active.json').read_bytes():raise AssertionError('partial publication visible after injected failure')
                            for product in lakehouse.OUTPUTS:
                                if fingerprint(lakehouse.consume(platform,product,publication_id=old['publication_id'])['rows'])!=previous[engine]['pins'][product]:raise AssertionError('failed attempt changed old pin')
                            report['recovery'].append({'engine':engine,'failpoint':fail,'status':'publication_preserved','failure_finished_at':report['runs'][-1]['finished_at'],'failure_runtime_seconds':report['runs'][-1]['runtime_seconds'],
                                'detection_boundary':'synchronous injected error, not infrastructure failure detection','old_publication_id':old['publication_id'],'operator_actions':0})
                    details=measured(name,engine,platform,execute,path)
                    if name=='sparse_delta':
                        for recovery in report['recovery']:
                            if recovery['engine']==engine:
                                recovery.update(status='passed',recovery_seconds=(timestamp(report['runs'][-1]['finished_at'])-timestamp(recovery['failure_finished_at'])).total_seconds(),
                                    recovery_boundary='Injected failure return to independently validated retry; earlier failpoint includes the subsequent serial injected attempt')
                    current={p:lakehouse.consume(platform,p)['rows'] for p in lakehouse.OUTPUTS}
                    if old:
                        indexed={r['incident_id']:hashlib.sha256(canonical(r).encode()).hexdigest() for r in current['incident_resolution']}
                        prior=previous[engine]['incident_index'];actual_keys={key for key in indexed.keys()|prior.keys() if indexed.get(key)!=prior.get(key)}
                        if actual_keys!=set(changed):raise AssertionError('known changed population differs: '+canonical(sorted(actual_keys)[:20]))
                        for product in lakehouse.OUTPUTS:
                            if fingerprint(lakehouse.consume(platform,product,publication_id=old['publication_id'])['rows'])!=previous[engine]['pins'][product]:raise AssertionError('new publication changed old pin')
                    previous[engine]={'incident_index':{r['incident_id']:hashlib.sha256(canonical(r).encode()).hexdigest() for r in current['incident_resolution']},
                                      'pins':{p:fingerprint(v) for p,v in current.items()}}
                    del current
                    entry['engines'][engine]={'status':'passed','publication_id':details['publication_id'],
                        'quality':details['quality'],'runtime_seconds':report['runs'][-1]['runtime_seconds'],'candidate_count':details.get('candidates',{}).get('count')}
                    active=(platform/'active.json').read_bytes()
                    for replay in (cases[0][1],path):
                        if execute(replay,platform)['status']!='already_committed':raise AssertionError('replay was not idempotent')
                    if active!=(platform/'active.json').read_bytes():raise AssertionError('replay rolled back active pointer')
                    persist();release()
            report['status']='passed';report['correctness_status']='passed'
            assessments=scorecard(workspace,report['runs'],criteria=criteria)['local_run_assessments']
            report['performance_status']='failed' if any(r['completion']['status']=='failed' for r in assessments) else 'passed' if spec['size']=='small' else 'not_assessed'
            return {'status':'passed','report':str(workspace/'representative.json'),'performance_status':report['performance_status']}
    except Exception as error:report.update(status='failed',error=str(error));raise
    finally:
        observer.stop();report['runtime_seconds']=time.perf_counter()-started;persist();performance(workspace,report['runs']);scorecard(workspace,report['runs'],criteria=criteria)
        write_json(workspace/'usage.json',observed_usage(report['runs'],workspace.name))


def fingerprint(rows):
    digest=hashlib.sha256()
    for row in rows:digest.update((canonical(row)+'\n').encode())
    return digest.hexdigest()


def run(workspace,count=100000,acceptance_path='benchmarks/specs/acceptance.poc.v4.json',strategies=('trino','spark','targeted')):
    with admission(workspace):return _run(workspace,count,acceptance_path,strategies)


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser();parser.add_argument('--workspace',required=True);parser.add_argument('--count',type=int,default=100000)
    parser.add_argument('--acceptance',default='benchmarks/specs/acceptance.poc.v4.json');parser.add_argument('--strategies',nargs='+',choices=['trino','spark','targeted'],default=['trino','spark','targeted'])
    args=parser.parse_args();print(run(args.workspace,args.count,args.acceptance,tuple(args.strategies)))
