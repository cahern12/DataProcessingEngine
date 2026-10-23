"""Frozen experiment profiles, single-experiment admission and bounded stages."""
import fcntl
import hashlib
import json
import os
import signal
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from .common import ROOT, canonical, read_json
from .lakehouse import atomic_json

DEFAULT = 'benchmarks/specs/acceptance.poc.v1.json'


def acceptance(path=None):
    if isinstance(path,dict):value=json.loads(canonical(path))
    else:
        path=Path(path or DEFAULT)
        if not path.is_absolute():path=ROOT/path
        value=read_json(path)
    if value.get('version') not in ('v1','v2','v3','v4','v5','v6'):raise ValueError('unsupported acceptance version')
    if value['engineering_criteria']['repetitions']<1:raise ValueError('positive repetitions required')
    resources=value['resources']
    if resources['maximum_local_logical_input_bytes']<1:raise ValueError('positive input budget required')
    return value


def checksum(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def memory_admission(criteria,input_bytes,count):
    """Conservative producer estimate from an explicitly identified measured run."""
    selection=criteria.get('memory_admission')
    if not selection or count<selection.get('minimum_count',100000):return {'status':'not_assessed','reason':'no same-profile calibration required at this count'}
    root=ROOT/selection['reference_workspace'];spec=read_json(root/'specification.json')
    if spec['criteria'].get('source_ingestion')!=criteria.get('source_ingestion'):raise ValueError('memory admission calibration ingestion profile mismatch')
    rows=[json.loads(line) for line in (root/'observations/producer_samples.jsonl').read_text().splitlines()]
    before=rows[0]['rss_bytes'];peak=max(r['rss_bytes'] for r in rows)
    markers=root/'observations/stages.jsonl'
    if markers.exists():
        fixed=[json.loads(line)['rss_bytes'] for line in markers.read_text().splitlines() if json.loads(line)['stage']=='dependencies_ready']
        if fixed:before=max(before,max(fixed))
    manifest=read_json(root/'source/initial/manifest.json');logical=sum(f['bytes'] for f in manifest['files'])
    projected=(before+max(0,peak-before)*input_bytes/logical)*selection.get('safety_factor',1.1)
    limit=criteria['resources'].get('producer_limit_gib',1.5)*1024**3
    return {'status':'eligible' if projected<=limit else 'resource_limited','estimated_producer_peak_bytes':int(projected),
            'producer_limit_bytes':int(limit),'reference_peak_sampled_bytes':peak,'reference_input_bytes':logical,
            'reference_fixed_runtime_rss_bytes':before,
            'reference_revision':spec['revision'],'reference':str(root),'source_profile_sha256':checksum(criteria.get('source_ingestion')),
            'method':'fixed initialized-runtime RSS plus observed incremental RSS scaled by raw bytes, with explicit safety factor; conservative estimate, not a capacity guarantee'}


def frozen(workspace,selected=None):
    path=Path(workspace)/'specification.json'
    if path.exists():return read_json(path)['criteria']
    path=Path(workspace)/'frozen_criteria.json'
    return read_json(path) if path.exists() else acceptance(selected)


@contextmanager
def admission(workspace,root=None):
    root=Path(root or ROOT/'data');root.mkdir(parents=True,exist_ok=True)
    with (root/'.benchmark-admission.lock').open('a+') as stream:
        try:fcntl.flock(stream,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:raise RuntimeError('another local benchmark owns experiment admission')
        owner=root/'.benchmark-owner.json'
        atomic_json(owner,{'status':'active','workspace':str(Path(workspace).resolve()),'pid':os.getpid(),
                           'started_at':datetime.now(timezone.utc).isoformat()})
        try:yield
        finally:
            atomic_json(owner,{'status':'released','workspace':str(Path(workspace).resolve()),'pid':os.getpid()})
            fcntl.flock(stream,fcntl.LOCK_UN)


@contextmanager
def watchdog(seconds):
    if not seconds:
        yield;return
    old=signal.getsignal(signal.SIGALRM)
    def expired(*args):raise TimeoutError('experiment stage exceeded operational watchdog: '+str(seconds)+' seconds')
    signal.signal(signal.SIGALRM,expired);signal.setitimer(signal.ITIMER_REAL,seconds)
    try:yield
    finally:signal.setitimer(signal.ITIMER_REAL,0);signal.signal(signal.SIGALRM,old)


def interrupted(workspace,error):
    import uuid
    path=Path(workspace)/'run_results.json'
    if not path.exists():return
    result=read_json(path)
    if result['status']!='running':return
    current=result.get('current_attempt',{'workload_id':'INTERRUPTED','engine':'reference','started_at':result['specification']['started_at']})
    spec=result['specification'];now=datetime.now(timezone.utc)
    manifest=Path(workspace)/'source/initial/manifest.json'
    logical=sum(f['bytes'] for f in read_json(manifest)['files']) if manifest.exists() else 0
    result['runs'].append(dict(current,logical_run_id=uuid.uuid4().hex,attempt_id=uuid.uuid4().hex,status='failed',quality_status='failed',
        strategy='full',size=spec['size'],workers=1,concurrency=1,logical_input_bytes=logical,
        runtime_seconds=(now-datetime.fromisoformat(current['started_at'].replace('Z','+00:00'))).total_seconds(),
        finished_at=now.isoformat(),input_manifest_checksum='unavailable',output_rows=None,evidence=str(path),error=error))
    result.update(status='failed',error=error)
    atomic_json(path,result)
    from .benchmark_reports import scorecard,performance
    scorecard(workspace,result['runs'],result.get('findings'),spec['criteria']);performance(workspace,result['runs'])
    from .benchmark_runner import observed_usage
    from .costing import report
    write_path=Path(workspace)/'usage.json';atomic_json(write_path,observed_usage(result['runs'],Path(workspace).name))
    report(write_path,Path(workspace)/'economics')
