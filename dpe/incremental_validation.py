"""Real-stack equivalence, recovery and frozen IO gates for targeted execution."""
import copy
import hashlib
import re
import time
from pathlib import Path

from .common import DATASETS, ROOT, canonical, read_json, write_json
from .ingestion import load_batch
from . import incremental, lakehouse
from .rules import evaluate
from .synthetic import BatchWriter, generate


def batch(root, name, parent, cutoff, records, report_start='2026-01-01T00:00:00Z'):
    writer = BatchWriter(root, 10000)
    for dataset in DATASETS:
        for record in records[dataset]: writer.add(dataset, record)
    manifest = {'batch_id':name,'batch_kind':'delta' if parent else 'initial','parent_batch_id':parent,
                'cutoff':cutoff,'report_start':report_start,'dataset_version':'v1','rule_version':'v1',
                'generator_version':'incremental-scenarios.v1','seed':42,'configuration':{},'files':writer.finish()}
    write_json(root/'manifest.json',manifest)
    return root


def scenario_batches(root):
    fixtures = read_json(ROOT/'synthetic/fixtures/rule_cases.v1.json')['cases']
    base = copy.deepcopy(fixtures[0]['datasets'])
    references = {k: v if k not in incremental.INCIDENTS else [] for k,v in base.items()}
    initial = copy.deepcopy(references)
    def add(name, key):
        source = next(c['datasets'] for c in fixtures if c['name']==name)
        for dataset in incremental.INCIDENTS:
            for item in source[dataset]:
                record = dict(item,incident_id=key)
                if 'event_id' in record: record['event_id'] = key+'-'+record['event_id']
                initial[dataset].append(record)
    for n in range(20): add('ordinary-resolved', 'STABLE-'+str(n))
    for name,key in [('open-hold','HOLD'),('future-records-deferred','FUTURE'),('ordinary-resolved','MOVE'),
                     ('ordinary-resolved','BAD'),('ordinary-resolved','LATE')]: add(name,key)
    initial['incident_events'].append(dict(base['incident_events'][0],incident_id='REPAIR',event_id='REPAIR-E1'))
    for item in initial['incident_snapshots']:
        if item['incident_id'] in ('MOVE','BAD'): item.update(service_id='S2',priority='P4')
    changes = copy.deepcopy(references)
    snapshot = base['incident_snapshots'][0]
    changes['incident_snapshots'] = [dict(snapshot,incident_id='REPAIR'),
        dict(snapshot,incident_id='MOVE',source_version=2,source_updated_at='2026-01-01T00:30:00Z',
             created_at='2025-12-31T23:30:00Z',service_id='S1',priority='P2',status='in_progress'),
        dict(snapshot,incident_id='BAD',source_version=2,source_updated_at='2026-01-01T02:00:00Z',
             service_id='MISSING',status='resolved')]
    changes['incident_events'] = [dict(base['incident_events'][0],incident_id='LATE',event_id='LATE-REOPEN',
        event_type='reopen',event_time='2026-01-02T01:00:00Z',source_sequence=3),
        dict(base['incident_events'][0],incident_id='LATE',event_id='LATE-RESOLVE',event_type='resolve',
             event_time='2026-01-03T01:00:00Z',source_sequence=4)]
    duplicate = copy.deepcopy(references)
    duplicate['incident_snapshots'] = [next(r for r in initial['incident_snapshots'] if r['incident_id']=='STABLE-0')]
    duplicate['incident_events'] = [next(r for r in initial['incident_events'] if r['incident_id']=='STABLE-0')]
    future_duplicate = copy.deepcopy(references)
    for dataset in incremental.INCIDENTS:
        future_duplicate[dataset] = [r for r in initial[dataset] if r['incident_id']=='FUTURE' and r.get('source_version', r.get('source_sequence'))==2+(dataset=='incident_events')]
    definitions = [('initial',None,'2026-01-08T00:00:00Z',initial),
                   ('corrections','initial','2026-01-08T00:00:00Z',changes),
                   ('future-duplicate','corrections','2026-01-08T00:00:00Z',future_duplicate),
                   ('cutoff','future-duplicate','2026-01-10T00:00:00Z',references),
                   ('duplicate','cutoff','2026-01-10T00:00:00Z',duplicate)]
    return [batch(root/name, name, parent, cutoff, records) for name,parent,cutoff,records in definitions]


def compare(platform, combined, header):
    start = time.perf_counter()
    expected = evaluate(combined, header['cutoff'], header['report_start'])
    differences = []
    for name in lakehouse.OUTPUTS:
        differences.extend(lakehouse.compare_rows(name,expected[name],lakehouse.consume(platform,name)['rows']))
    if differences: raise AssertionError('full rebuild differences: '+canonical(differences[:5]))
    return {'differences':differences,'independent_validation_seconds':round(time.perf_counter()-start,6)}


def scenarios(workspace):
    sources = scenario_batches(workspace/'source'); platform = workspace/'platform'
    evidence = {'status':'running','runs':[],'failpoints':{}}
    combined = {name:[] for name in DATASETS}
    try:
        for path in sources:
            header, incoming, _ = load_batch(path)
            if path.name=='corrections':
                before = (platform/'active.json').read_bytes()
                old = read_json(platform/'active.json')['publication_id']
                pinned = {name:lakehouse.consume(platform,name,publication_id=old)['rows'] for name in lakehouse.OUTPUTS}
                for point in incremental.FAILPOINTS:
                    try: incremental.run(path,platform,fail_at=point)
                    except RuntimeError as error:
                        if str(error)!='injected failure: '+point: raise
                    else: raise AssertionError('failure injection did not interrupt publication')
                    if before!=(platform/'active.json').read_bytes(): raise AssertionError('failed attempt advanced checkpoint')
                    for name in lakehouse.OUTPUTS:
                        if lakehouse.consume(platform,name,publication_id=old)['rows'] != pinned[name]: raise AssertionError('pinned product changed')
                    evidence['failpoints'][point]='passed'
            result = incremental.run(path,platform)
            for name in DATASETS: combined[name].extend(incoming[name])
            result['comparison'] = compare(platform,combined,header)
            expected_candidates = {'corrections':{'MOVE','BAD','LATE','REPAIR'},'cutoff':{'HOLD','FUTURE','REPAIR'},'future-duplicate':set(),'duplicate':set()}
            if path.name in expected_candidates and set(result['candidates']['incident_ids']) != expected_candidates[path.name]:
                raise AssertionError('unexpected candidate identities: '+canonical(result['candidates']))
            if path.name=='future-duplicate':
                published = read_json(platform/'active.json')
                offsets = result['run_specification']['parent_input_checkpoint']['rows']
                predicate = ' OR '.join('(dataset='+lakehouse.literal(k)+' AND ordinal>='+str(offsets[k])+')' for k in incremental.INCIDENTS)
                dispositions = lakehouse.Client().sql('SELECT disposition,rule_id FROM '+incremental.pinned(published['dispositions'])+' WHERE '+predicate)
                if not dispositions or any(r['disposition']!='deferred' or r['rule_id'] is not None for r in dispositions): raise AssertionError('future duplicate lost deferred disposition')
            if result['quality']['incident_count']!=26: raise AssertionError('quality denominator is not full population')
            evidence['runs'].append(result)
        active = (platform/'active.json').read_bytes()
        for path in sources:
            if incremental.run(path,platform)['status']!='already_committed': raise AssertionError('non-idempotent replay')
        mutated = copy.deepcopy(load_batch(sources[-1])[1]); mutated['services'][0]['name']='unsupported mutation'
        path = batch(workspace/'source/reference-mutation','reference-mutation','duplicate','2026-01-10T00:00:00Z',mutated)
        try: incremental.run(path,platform)
        except ValueError as error:
            if str(error)!='unsupported v1 reference mutation': raise
        else: raise AssertionError('v1 reference mutation accepted')
        if active!=(platform/'active.json').read_bytes(): raise AssertionError('replay changed active state')
        evidence['audit'] = incremental.audit(platform)
        evidence['status']='passed'
        return evidence
    except Exception as error:
        evidence.update(status='failed',error=str(error)); raise
    finally: write_json(workspace/'scenarios.json',evidence)


def size_bytes(value):
    match = re.fullmatch(r'([0-9.]+)([A-Za-z]+)',value)
    if not match: raise ValueError('unrecognized Trino size: '+value)
    units = {'B':1,'kB':1024,'KB':1024,'MB':1024**2,'GB':1024**3,'TB':1024**4}
    return round(float(match[1])*units[match[2]])


def metrics(attempt):
    stages = {}; query_count = missing = 0
    def accumulate(query, name):
        nonlocal query_count, missing
        query_count += 1
        stage = stages.setdefault(name, {'physical_read_bytes':0,'physical_written_bytes':0,'input_rows':0,
            'queries':0,'runtime_seconds':0,'cpu_seconds':0,'peak_query_memory_bytes':0,'spilled_bytes':0})
        telemetry = query.get('telemetry',{})
        if not telemetry or 'unavailable' in telemetry: missing += 1; return
        stage['physical_read_bytes'] += size_bytes(telemetry['physicalInputDataSize'])
        stage['physical_written_bytes'] += size_bytes(telemetry['physicalWrittenDataSize'])
        stage['input_rows'] += telemetry['physicalInputPositions']; stage['queries'] += 1
        stage['runtime_seconds'] += query.get('runtime_seconds',0)
        cpu = re.fullmatch(r'([0-9.]+)([a-z]+)', telemetry['totalCpuTime'])
        stage['cpu_seconds'] += float(cpu[1])*{'ns':1e-9,'us':1e-6,'ms':1e-3,'s':1,'m':60,'h':3600}[cpu[2]]
        peak=telemetry.get('peakTotalMemoryReservation')
        if peak is None:
            stage['peak_query_memory_bytes']=None
            stage['peak_memory_status']='unavailable for one or more queries; completed-event user peak is not total query memory'
        elif stage['peak_query_memory_bytes'] is not None:stage['peak_query_memory_bytes'] = max(stage['peak_query_memory_bytes'],size_bytes(peak))
        stage['spilled_bytes'] += size_bytes(telemetry['spilledDataSize'])
    for query in attempt.get('queries',[]): accumulate(query,query.get('stage','unclassified'))
    for invocation in attempt.get('dbt',[]):
        for query in invocation.get('queries',[]): accumulate(query,invocation['stage'])
        stages[invocation['stage']]['runtime_seconds'] += invocation['runtime_seconds']
    return {'stages':stages,'runtime_seconds':attempt['runtime_seconds'],
            'telemetry_coverage':(query_count-missing)/query_count if query_count else 0,
            'total_physical_read_bytes':sum(s['physical_read_bytes'] for s in stages.values()),
            'total_physical_written_bytes':sum(s['physical_written_bytes'] for s in stages.values()),
            'coordinator_and_incoming_validation_seconds':max(0,attempt['runtime_seconds']-sum(s['runtime_seconds'] for s in stages.values()))}


def workload(workspace, name, count, criteria):
    config = read_json(ROOT/'synthetic/configs/default.json'); config.update(criteria['workloads'][name])
    config_path = workspace/'configuration.json'; write_json(config_path,config)
    evidence = {'status':'running','count':count,'configuration':config,'frozen_criteria':criteria}
    write_json(workspace/'specification.json',evidence)
    try:
        generate(workspace/'source',count=count,config_path=config_path)
        target = workspace/'targeted'; full = workspace/'full'
        evidence['initial'] = incremental.run(workspace/'source/initial',target)
        evidence['delta'] = incremental.run(workspace/'source/delta',target)
        # Preserved SQL full-rebuild implementation; independent Python evaluator too.
        lakehouse.run(workspace/'source/initial',full)
        evidence['full_delta'] = lakehouse.run(workspace/'source/delta',full)
        first, incoming, _ = load_batch(workspace/'source/initial')
        header, updates, _ = load_batch(workspace/'source/delta')
        combined = {k:incoming[k]+updates[k] for k in DATASETS}
        evidence['python_comparison'] = compare(target,combined,header)
        differences = []
        for dataset in lakehouse.OUTPUTS:
            differences.extend(lakehouse.compare_rows(dataset,lakehouse.consume(full,dataset)['rows'],lakehouse.consume(target,dataset)['rows']))
        evidence['sql_differences'] = differences
        targeted = metrics(evidence['delta']); baseline = metrics(evidence['full_delta'])
        processing = criteria['efficiency']['processing_stages']
        def reads(measure): return sum(v['physical_read_bytes'] for k,v in measure['stages'].items() if k in processing)
        ratio = reads(targeted)/reads(baseline) if reads(baseline) else None
        fraction = evidence['delta']['candidates']['count']/evidence['delta']['quality']['incident_count']
        gates = {'zero_sql_differences':not differences,
            'complete_telemetry':min(targeted['telemetry_coverage'],baseline['telemetry_coverage'])>=criteria['efficiency']['required_telemetry_coverage']}
        if name=='controlled':
            gates.update(candidate_fraction=fraction<=criteria['efficiency']['controlled_maximum_candidate_fraction'],
                processing_reads_reduced=ratio is not None and ratio<criteria['efficiency']['controlled_maximum_processing_read_ratio_exclusive'])
        evidence['measurement'] = {'targeted':targeted,'full':baseline,'processing_read_ratio':ratio,
                                   'candidate_fraction':fraction,'gates':gates}
        evidence['status'] = 'passed' if all(gates.values()) else 'failed'
        if evidence['status']=='failed': raise AssertionError('incremental acceptance failed: '+canonical(gates))
        return evidence
    except Exception as error:
        evidence.update(status='failed',error=str(error)); raise
    finally: write_json(workspace/'workload.json',evidence)


def verify(workspace, count=1000):
    workspace = Path(workspace)
    if workspace.exists(): raise ValueError('verification workspace exists; choose a fresh path')
    criteria = read_json(ROOT/'benchmarks/acceptance.incremental.v1.json')
    workspace.mkdir(parents=True)
    evidence = {'status':'running','revision':lakehouse.sql_revision(),'baseline_commit':criteria['baseline_commit'],
                'criteria':criteria,'criteria_sha256':hashlib.sha256(canonical(criteria).encode()).hexdigest()}
    write_json(workspace/'specification.json',evidence)
    try:
        evidence['scenarios'] = scenarios(workspace/'edge_cases')
        evidence['workloads'] = {}
        for name in criteria['workloads']:
            evidence['workloads'][name] = workload(workspace/name,name,count,criteria)
        evidence['status']='passed'
        return {'status':'passed','report':str(workspace/'verification.json'),
                'workloads':{k:v['measurement'] for k,v in evidence['workloads'].items()}}
    except Exception as error:
        evidence.update(status='failed',error=str(error)); raise
    finally: write_json(workspace/'verification.json',evidence)


def restart_check(workspace):
    """Run after restarting services; re-read every committed pair, including parents."""
    workspace = Path(workspace); results = []
    for platform in [workspace/'edge_cases/platform',workspace/'controlled/targeted',workspace/'open_heavy/targeted']:
        combined = {k:[] for k in DATASETS}
        for publication in reversed(list(lakehouse.committed_publications(platform))):
            archive = platform/publication['archives'][-1]
            header, incoming, _ = load_batch(archive)
            for name in DATASETS: combined[name].extend(incoming[name])
            expected = evaluate(combined,header['cutoff'],header['report_start'])
            for name in lakehouse.OUTPUTS:
                differences = lakehouse.compare_rows(name,expected[name],lakehouse.consume(platform,name,publication_id=publication['publication_id'])['rows'])
                if differences: raise AssertionError('restart changed pinned products: '+canonical(differences[:5]))
            results.append({'workspace':str(platform),'publication_id':publication['publication_id'],'status':'passed'})
    result = {'status':'passed','publications':results}
    write_json(workspace/'restart.json',result)
    return result
