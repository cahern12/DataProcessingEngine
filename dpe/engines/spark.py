"""Host adapter for the isolated Spark image and common publication contract."""
import hashlib
import os
import shlex
import subprocess
import time
import uuid
from pathlib import Path
from ..common import ROOT,DATASETS,canonical,read_json,write_json,validate_schema,digest,timestamp,safe_file
from ..lakehouse import (Client,OUTPUTS,atomic_json,committed_publications,sql_revision,workspace_lock,compare_rows,consume)
from ..rules import SCHEMAS
from ..ingestion import load_batch
from ..quality import load_policy,enforce_quality


def execute(spec,folder,fail_at=None,master='local[4]'):
    folder=Path(folder).resolve();folder.mkdir(parents=True,exist_ok=True)
    if not folder.is_relative_to((ROOT/'data').resolve()): raise ValueError('Spark artifacts must be under repository data/')
    write_json(folder/'input.json',spec)
    relative=folder.relative_to(ROOT)
    endpoint=os.environ.get('DPE_SPARK_ENDPOINT')
    if endpoint:
        from urllib.request import Request,urlopen
        request=Request(endpoint.rstrip('/')+'/jobs',data=canonical({'relative_folder':str(relative.relative_to('data')),'spec':spec,'fail_at':fail_at,'master':master}).encode(),headers={'Content-Type':'application/json'})
        start=time.perf_counter()
        import json
        try:
            with urlopen(request,timeout=1920) as stream:result=json.load(stream)
        except BaseException:
            try:
                cancellation=Request(endpoint.rstrip('/')+'/cancel',data=canonical({'relative_folder':str(relative.relative_to('data'))}).encode(),headers={'Content-Type':'application/json'})
                with urlopen(cancellation,timeout=20) as stream:stream.read()
            except Exception:pass
            raise
        result['submission_runtime_seconds']=time.perf_counter()-start
        if result['status']!='passed':raise RuntimeError('Spark failed: '+result.get('error','unknown'))
        if result['implementation_revision']!=sql_revision():raise ValueError('Spark image is stale; rebuild benchmark images')
        return result
    command=shlex.split(os.environ.get('DPE_DOCKER_COMPOSE','docker-compose'))+['-f',str(ROOT/'infra/local/compose.yaml'),'-f',str(ROOT/'infra/local/compose.benchmark.yaml'),'run','--rm','spark','--input','/app/'+str(relative/'input.json'),'--output','/app/'+str(relative/'result.json'),'--master',master]
    if fail_at:command+=['--fail-at',fail_at]
    start=time.perf_counter()
    with (folder/'console.log').open('w') as stream:
        process=subprocess.run(command,cwd=ROOT,stdout=stream,stderr=subprocess.STDOUT)
    if not (folder/'result.json').exists(): raise RuntimeError('Spark did not produce evidence; see '+str(folder/'console.log'))
    result=read_json(folder/'result.json');result['submission_runtime_seconds']=time.perf_counter()-start
    if process.returncode or result['status']!='passed': raise RuntimeError('Spark failed: '+result.get('error','see console.log'))
    if result['implementation_revision']!=sql_revision():raise ValueError('Spark image is stale; rebuild benchmark images')
    return result


def compatibility(workspace):
    client=Client();schema='dpe_spark_probe_'+uuid.uuid4().hex[:10]
    client.sql('CREATE SCHEMA lake.'+schema)
    client.sql('CREATE TABLE lake.'+schema+'.trino_probe AS SELECT 1 id')
    result=execute({'action':'compat','trino_table':'lake.'+schema+'.trino_probe'},workspace)
    actual=client.sql('SELECT * FROM lake.'+result['schema']+'.probe')
    if actual!=[{'id':1,'value':'spark'}] or result['trino_rows']!=1: raise AssertionError('cross-engine catalog read mismatch')
    result['cross_engine_status']='passed';write_json(Path(workspace)/'compatibility.json',result)
    return result


def fixtures(workspace):
    cases=read_json(ROOT/'synthetic/fixtures/rule_cases.v1.json')['cases']
    result=execute({'cases':cases},workspace)
    differences=[]
    for case in cases:
        actual=[{k:v for k,v in r.items() if k!='case_id'} for r in result['outputs']['incident_resolution'] if r['case_id']==case['name']]
        expected=[] if 'expected_quarantine_rule' in case else [case['expected_incident']]
        differences+=compare_rows('incident_resolution',expected,actual,case['name'])
        if 'expected_quarantine_rule' in case and case['expected_quarantine_rule'] not in {r['rule_id'] for r in result['quarantine'] if r['case_id']==case['name']}:
            differences.append({'case':case['name'],'error':'missing expected quarantine rule'})
        if 'expected_daily' in case:
            actual=[{k:v for k,v in r.items() if k!='case_id'} for r in result['outputs']['service_daily_metrics'] if r['case_id']==case['name']]
            differences+=compare_rows('service_daily_metrics',case['expected_daily'],actual,case['name'])
        for key,disposition in [('expected_duplicate_count','duplicate'),('expected_deferred_count','deferred')]:
            if key in case and sum(1 for r in result['reconciliation'] if r['case_id']==case['name'] and r['disposition']==disposition)!=case[key]:
                differences.append({'case':case['name'],'error':'disposition mismatch','disposition':disposition})
    result['fixture_count']=len(cases);result['differences']=differences
    result['status']='failed' if differences else 'passed';write_json(Path(workspace)/'fixtures.json',result)
    if differences:raise AssertionError('Spark fixture differences: '+canonical(differences[:5]))
    return result


def run(batch,workspace,fail_at=None):
    import shutil
    workspace=Path(workspace)
    with workspace_lock(workspace):
        attempt=uuid.uuid4().hex;start=time.perf_counter();path=workspace/'attempts'/(attempt+'.json')
        evidence={'started_at':__import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat(),'status':'running','engine':'spark','attempt_id':attempt,'strategy':'full','revision':sql_revision()};atomic_json(path,evidence)
        try:
            if fail_at not in (None,'between_gold_writes','before_publish'):raise ValueError('unsupported Spark failpoint')
            from ..bounded_io import enabled,sources,release
            bounded=enabled()
            if bounded:header,incoming,_,checked=sources([batch])
            else:header,incoming,checked=load_batch(batch)
            policy=load_policy()
            parent=next(committed_publications(workspace),None)
            identity=hashlib.sha256(canonical([checked['manifest_sha256'],evidence['revision'],policy,'spark-full']).encode()).hexdigest()
            for old in committed_publications(workspace):
                if old['batch_id']==header['batch_id']:
                    if old['identity']!=identity:raise ValueError('committed code/source/policy changed')
                    evidence.update(status='already_committed',publication_id=old['publication_id']);return evidence
            if parent and (parent['revision']!=evidence['revision'] or parent['batch_id']!=header['parent_batch_id'] or timestamp(header['cutoff'])<timestamp(parent['cutoff'])):raise ValueError('parent/code/cutoff mismatch')
            if parent and parent['report_start']!=header['report_start']:raise ValueError('report start changed')
            if not parent and header['batch_kind']!='initial':raise ValueError('initial batch required')
            archive=workspace/'sources'/checked['manifest_sha256']
            if not archive.exists():shutil.copytree(batch,archive)
            archives=(parent['archives'] if parent else [])+[str(archive.relative_to(workspace))]
            combined={k:[] for k in DATASETS};provenance={k:[] for k in DATASETS}
            from ..lakehouse import object_store
            storage=object_store()
            for relative in archives:
                source=safe_file(workspace,relative)
                if bounded:h,d,_,c=sources([source])
                else:h,d,c=load_batch(source)
                if c['manifest_sha256']!=source.name:raise ValueError('archived source identity mismatch')
                for entry in h['files']+[{'path':'manifest.json'}]:
                    file=safe_file(source,entry['path']);storage.put_object(Bucket='raw',Key=c['manifest_sha256']+'/'+entry['path'],Body=file.read_bytes(),Metadata={'sha256':digest(file)})
                if not bounded:
                    for k in DATASETS:
                        combined[k]+=d[k]
                        for file in [f for f in h['files'] if f['dataset']==k]:
                            provenance[k]+=[{'batch_id':h['batch_id'],'source_file':file['path'],'source_row':n+1} for n in range(file['rows'])]
            if bounded:
                _,combined,provenance,_=sources([safe_file(workspace,a) for a in archives],verify=False)
                del incoming
                release()
            validate_references(combined)
            case={'name':'__batch__','cutoff':header['cutoff'],'report_start':header['report_start']}
            if bounded:
                from ..bounded_io import spool
                case['raw_path']=spool(combined,provenance,workspace/'spark'/attempt/'raw.jsonl')
            else:case.update(datasets=combined,provenance=provenance)
            result=execute({'cases':[case],'bounded_result':bounded},workspace/'spark'/attempt,fail_at)
            evidence['spark']=result
            evidence['quality']=enforce_quality({'quarantined_incidents':{r['incident_id']:[] for r in result['quarantine']}},combined,policy)
            for name in DATASETS:
                actual=sum(r['rows'] for r in result['reconciliation'] if r['dataset']==name) if bounded else len([r for r in result['reconciliation'] if r['dataset']==name])
                if actual!=len(combined[name]):raise AssertionError('unbalanced Spark dispositions')
            for name,value in result['outputs'].items():
                rows=Client().sql('SELECT * FROM '+result['snapshots'][name]['table']+' FOR VERSION AS OF '+str(result['snapshots'][name]['snapshot_id'])) if bounded else value
                for row in rows:validate_schema({k:v for k,v in row.items() if k!='case_id'},SCHEMAS[name])
                compare_rows(name,rows,rows) # validates unique contracted keys
                if bounded:del rows;release()
            if fail_at=='before_publish':raise RuntimeError('injected failure: before_publish')
            publication_id=hashlib.sha256(canonical([identity,parent['publication_id'] if parent else None]).encode()).hexdigest()
            publication=dict(publication_id=publication_id,identity=identity,batch_id=header['batch_id'],parent_publication_id=parent['publication_id'] if parent else None,
                cutoff=header['cutoff'],report_start=header['report_start'],revision=evidence['revision'],strategy='spark-full',engine='spark',archives=archives,
                snapshots=result['snapshots'],attempt_id=attempt,rule_version='v1',contract_version='v1',input_checkpoint={'archives':archives,'manifest_sha256':checked['manifest_sha256']})
            # Consumer is unchanged: Trino reads the exact Spark-created Iceberg snapshots.
            atomic_json(workspace/'publications'/(publication_id+'.json'),publication);atomic_json(workspace/'active.json',publication)
            evidence.update(status='committed',publication_id=publication_id,snapshots=result['snapshots']);return evidence
        except Exception as error:evidence.update(status='failed',error=str(error));raise
        finally:evidence['runtime_seconds']=time.perf_counter()-start;atomic_json(path,evidence)


def validate_references(datasets):
    """Input/reference policy only; Gold/state/SLA computation remains Spark SQL."""
    from ..rules import normalize
    for name,key in [('users','user_id'),('services','service_id'),('assignment_groups','group_id'),('sla_targets','priority')]:
        seen={}
        for raw in datasets[name]:
            value=normalize(name,raw)
            if value[key] in seen and value!=seen[value[key]]:raise ValueError('conflicting reference key: '+name)
            seen[value[key]]=value
        if name=='sla_targets':
            import csv
            with (ROOT/'rules/sla_targets.v1.csv').open() as stream:
                targets={r['priority']:(int(r['response_minutes']),int(r['resolution_minutes'])) for r in csv.DictReader(stream)}
            if set(seen)!=set(targets) or any((r['response_minutes'],r['resolution_minutes'])!=targets[p] or r['rule_version']!='v1' for p,r in seen.items()):raise ValueError('immutable v1 SLA targets required')
