"""Targeted v1 processing over immutable, partitioned Iceberg deliveries.

Tables are never mutated after publication: Nessie's local REST endpoint exposes
only the current snapshot. The active manifest is the single commit/checkpoint.
"""
import hashlib
import shutil
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .common import DATASETS, canonical, digest, iso, read_json, safe_file, timestamp, validate_schema
from .ingestion import load_batch
from .lakehouse import (Client, OUTPUTS, archived_batch, atomic_json, committed_publications,
                        dbt, identifier, literal, object_store, sql_revision, workspace_lock)
from .quality import enforce_quality, load_policy
from .rules import SCHEMAS, normalize

INCIDENTS = ('incident_snapshots', 'incident_events')
RAW_COLUMNS = 'case_id,dataset,ordinal,batch_id,source_file,source_row,payload'
FAILPOINTS = ('after_input_persistence', 'during_selective_writes', 'between_gold_writes', 'before_publish')


def table(value):
    parts = value.split('.')
    if len(parts) != 3 or parts[0] != 'lake':
        raise ValueError('invalid table reference')
    return '.'.join(identifier(part) for part in parts)


def pinned(reference):
    if type(reference['snapshot_id']) is not int:
        raise ValueError('invalid snapshot')
    return table(reference['table']) + ' FOR VERSION AS OF ' + str(reference['snapshot_id'])


def ref(client, name):
    parts = table(name).split('.')
    snapshots = client.sql('SELECT snapshot_id FROM ' + '.'.join(parts[:2]) + '."' + parts[2] + '$snapshots"')
    return {'table': name, 'snapshot_id': snapshots[0]['snapshot_id']}


def rows_in(values):
    return '(' + ','.join(literal(v) for v in values) + ')' if values else '(SELECT cast(NULL as varchar) WHERE false)'


def raw_union(deliveries, predicate='true', columns='*'):
    return ' UNION ALL '.join('SELECT ' + columns + ' FROM ' + pinned(d) + ' WHERE ' + predicate for d in deliveries)


def reference_hashes(incoming):
    return {name: hashlib.sha256(canonical(sorted((normalize(name, r) for r in incoming[name]), key=canonical)).encode()).hexdigest()
            for name in DATASETS if name not in INCIDENTS}


def delivery(client, schema, manifest, incoming, parent, checksum):
    """Write only incoming bytes; attempts have disjoint immutable namespaces."""
    target = 'lake.' + schema + '.delivery'
    client.sql('CREATE TABLE ' + target + '''(case_id varchar,dataset varchar,ordinal bigint,batch_id varchar,
        source_file varchar,source_row bigint,payload varchar,record_incident varchar,event_id varchar,
        record_time varchar,record_valid boolean)
        WITH (format='PARQUET',partitioning=ARRAY['dataset','bucket(record_incident,32)'])''')
    offsets = parent['input_checkpoint']['rows'] if parent else dict.fromkeys(DATASETS, 0)
    buffer = []
    for name in DATASETS:
        origins = [(f['path'], n+1) for f in manifest['files'] if f['dataset'] == name for n in range(f['rows'])]
        for n, record in enumerate(incoming[name]):
            valid, normalized = True, record
            try:
                normalized = normalize(name, record)
            except (ValueError, TypeError):
                valid = False
            clock = normalized.get('source_updated_at', normalized.get('event_time')) if valid else None
            if isinstance(clock, datetime): clock = iso(clock)
            incident = str(record.get('incident_id', '')).strip() if name in INCIDENTS else None
            buffer.append(('__batch__', name, offsets[name]+n, manifest['batch_id'], *origins[n], canonical(record),
                           incident, str(record.get('event_id', '')).strip() if name == 'incident_events' else None, clock, valid))
            if len(buffer) == 500:
                insert(client, target, buffer); buffer = []
    if buffer: insert(client, target, buffer)
    # Bucketed compact identity index detects event IDs moving to a different incident.
    client.sql('CREATE TABLE lake.' + schema + ".event_ids WITH (partitioning=ARRAY['bucket(event_id,32)']) AS SELECT DISTINCT event_id,record_incident FROM " + target + " WHERE dataset='incident_events'")
    result = ref(client, target)
    result.update(manifest_sha256=checksum, batch_id=manifest['batch_id'], event_ids=ref(client, 'lake.'+schema+'.event_ids'))
    return result


def insert(client, target, rows):
    client.sql('INSERT INTO ' + target + ' VALUES ' + ','.join('(' + ','.join(
        ('true' if v else 'false') if type(v) is bool else literal(v) for v in row) + ')' for row in rows))


def candidates(client, schema, current, parent, manifest):
    """Discovery reads compact state plus incoming IDs' bucketed history."""
    incoming_ids = [r['record_incident'] for r in client.sql('SELECT DISTINCT record_incident FROM ' + pinned(current) + ' WHERE record_incident IS NOT NULL')]
    reasons = []
    if not parent:
        reasons = [(v, 'initial') for v in incoming_ids]
    elif incoming_ids:
        prior = raw_union(parent['input_checkpoint']['deliveries'], 'record_incident IN ' + rows_in(incoming_ids), 'dataset,payload')
        novel = client.sql('SELECT DISTINCT n.record_incident FROM ' + pinned(current) + ' n WHERE n.record_incident IS NOT NULL AND NOT EXISTS (SELECT 1 FROM (' + prior + ') p WHERE p.dataset=n.dataset AND p.payload=n.payload)')
        reasons.extend((r['record_incident'], 'source_change') for r in novel)
        event_ids = [r['event_id'] for r in client.sql('SELECT event_id FROM ' + pinned(current['event_ids']))]
        if event_ids:
            prior_ids = ' UNION ALL '.join('SELECT * FROM ' + pinned(d['event_ids']) + ' WHERE event_id IN ' + rows_in(event_ids) for d in parent['input_checkpoint']['deliveries'])
            conflicts = client.sql('SELECT DISTINCT p.record_incident FROM ('+prior_ids+') p JOIN '+pinned(current['event_ids'])+' n ON n.event_id=p.event_id WHERE p.record_incident<>n.record_incident')
            reasons.extend((r['record_incident'], 'event_identity_conflict') for r in conflicts)
    if parent and timestamp(manifest['cutoff']) > timestamp(parent['cutoff']):
        old = 'from_iso8601_timestamp('+literal(parent['cutoff'])+')'; new = 'from_iso8601_timestamp('+literal(manifest['cutoff'])+')'
        state = pinned(parent['state'])
        for condition, reason in (
                ('next_eligibility<=' + new, 'future_eligibility'),
                ("state NOT IN ('resolved','closed')", 'open_clock'),
                ('has_response=false AND state IS NOT NULL', 'response_clock'),
                ('boundary_time>='+old, 'report_boundary')):
            reasons.extend((r['incident_id'], reason) for r in client.sql('SELECT incident_id FROM '+state+' WHERE '+condition))
    # A future conflicting event can invalidate an incident with no new delivery.
    all_deliveries = (parent['input_checkpoint']['deliveries'] if parent else []) + [current]
    known = {v for v, _ in reasons}
    while known:
        identities = ' UNION ALL '.join('SELECT * FROM '+pinned(d['event_ids']) for d in all_deliveries)
        linked = client.sql('SELECT DISTINCT record_incident FROM ('+identities+') WHERE event_id IN (SELECT event_id FROM ('+identities+') WHERE record_incident IN '+rows_in(sorted(known))+')')
        extra = {r['record_incident'] for r in linked} - known
        if not extra: break
        reasons.extend((v, 'event_identity_conflict') for v in extra); known.update(extra)
    client.sql('CREATE TABLE lake.'+schema+'.affected_incidents(incident_id varchar,reason varchar)')
    if reasons: insert(client, 'lake.'+schema+'.affected_incidents', sorted(set(reasons)))
    return sorted({v for v, _ in reasons})


def materialize_histories(client, schema, deliveries, selected, manifest):
    predicate = 'record_incident IN ' + rows_in(selected) + ' OR record_incident IS NULL'
    # Static predicates reach each bucketed delivery before any JSON parsing/window work.
    client.sql('CREATE TABLE lake.'+schema+'.raw_records AS '+raw_union(deliveries, predicate, RAW_COLUMNS))
    client.sql('CREATE TABLE lake.'+schema+'.batch_context(case_id varchar,cutoff varchar,report_start varchar)')
    insert(client, 'lake.'+schema+'.batch_context', [('__batch__', manifest['cutoff'], manifest['report_start'])])


def retained(parent, name, selected):
    return 'SELECT * FROM ' + pinned(parent[name]) + ' WHERE incident_id NOT IN ' + rows_in(selected)


def combine(client, schema, name, parent_ref, selected):
    """Isolation fallback: copy unchanged output rows, with its IO in evidence."""
    client.sql('ALTER TABLE lake.'+schema+'.'+name+' RENAME TO '+name+'_changed')
    client.sql('CREATE TABLE lake.'+schema+'.'+name+' AS SELECT * FROM lake.'+schema+'.'+name+'_changed UNION ALL SELECT * FROM '+pinned(parent_ref)+' WHERE incident_id NOT IN '+rows_in(selected))


def transform(client, schema, workspace, attempt, parent, selected, fail_at, evidence):
    folder = workspace/'dbt'/attempt
    calls = evidence.setdefault('dbt', [])
    def execute(**kwargs):
        result = dbt(schema, folder, **kwargs)
        result['stage'] = evidence['stage']; calls.append(result)
    evidence['stage'] = client.stage = 'incident_transformation'
    execute(exclude=['gold_incident_resolution','gold_service_daily_metrics','incident_daily_contributions','compatibility_probe'], command='run')
    if fail_at == 'during_selective_writes': raise RuntimeError('injected failure: '+fail_at)
    execute(selection=['gold_incident_resolution'], command='run')
    execute(selection=['incident_daily_contributions'], command='run')
    # Keep per-incident contributions so affected groups can include ALL contributors.
    if parent:
        evidence['stage'] = client.stage = 'retained_output_copy'
        combine(client, schema, 'gold_incident_resolution', parent['snapshots']['incident_resolution'], selected)
        combine(client, schema, 'incident_daily_contributions', parent['contributions'], selected)
        combine(client, schema, 'quarantine', parent['quarantine'], selected)
    if fail_at == 'between_gold_writes': raise RuntimeError('injected failure: '+fail_at)
    evidence['stage'] = client.stage = 'daily_aggregation'
    key = 'date,service_id,priority'
    contributions = 'lake.'+schema+'.incident_daily_contributions'
    impacts = 'SELECT '+key+' FROM '+contributions+' WHERE incident_id IN '+rows_in(selected)
    if parent:
        impacts += ' UNION SELECT '+key+' FROM '+pinned(parent['contributions'])+' WHERE incident_id IN '+rows_in(selected)
    client.sql('CREATE TABLE lake.'+schema+'.affected_daily_groups AS SELECT DISTINCT '+key+' FROM ('+impacts+')')
    execute(selection=['gold_service_daily_metrics'], command='run', variables={'targeted_daily': True})
    if parent:
        evidence['stage'] = client.stage = 'retained_output_copy'
        client.sql('ALTER TABLE lake.'+schema+'.gold_service_daily_metrics RENAME TO gold_service_daily_metrics_changed')
        client.sql('CREATE TABLE lake.'+schema+'.gold_service_daily_metrics AS SELECT * FROM lake.'+schema+'.gold_service_daily_metrics_changed UNION ALL SELECT * FROM '+pinned(parent['snapshots']['service_daily_metrics'])+' WHERE ('+key+') NOT IN (SELECT '+key+' FROM lake.'+schema+'.affected_daily_groups)')
    evidence['stage'] = client.stage = 'validation'
    execute(selection=['unique_gold_keys','valid_reference_data'], command='test')


def checkpoint_tables(client, schema, parent, current, deliveries, selected, manifest):
    """Full population index and delivery reconciliation, independent of work-set size."""
    client.stage = 'checkpoint_maintenance'
    state = 'lake.'+schema+'.incident_state'
    chosen = raw_union(deliveries, 'record_incident IN '+rows_in(selected),
                       'record_incident,record_time')
    # ISO timestamps are canonicalized on delivery, so index comparisons are chronological.
    client.sql('CREATE TABLE '+state+' AS SELECT r.record_incident incident_id,min(from_iso8601_timestamp(r.record_time)) FILTER(WHERE from_iso8601_timestamp(r.record_time)>from_iso8601_timestamp('+literal(manifest['cutoff'])+''')) next_eligibility,
        g.state,g.first_response_at IS NOT NULL has_response,
        greatest(g.created_at,coalesce(g.resolved_at,g.created_at)) boundary_time
        FROM ('''+chosen+') r LEFT JOIN lake.'+schema+'''.gold_incident_resolution g ON g.incident_id=r.record_incident
        GROUP BY r.record_incident,g.state,g.first_response_at,g.created_at,g.resolved_at'''+
        (' UNION ALL SELECT * FROM '+pinned(parent['state'])+' WHERE incident_id NOT IN '+rows_in(selected) if parent else ''))
    # dbt computes dispositions for all selected history and all references.
    if parent:
        client.stage = 'retained_output_copy'
        client.sql('ALTER TABLE lake.'+schema+'.record_dispositions RENAME TO record_dispositions_changed')
        client.sql('CREATE TABLE lake.'+schema+'.record_dispositions AS SELECT * FROM lake.'+schema+'.record_dispositions_changed UNION ALL SELECT * FROM '+pinned(parent['dispositions'])+" WHERE dataset IN ('incident_snapshots','incident_events') AND incident_id NOT IN "+rows_in(selected)+
                   " UNION ALL SELECT n.case_id,n.dataset,n.ordinal,n.record_incident incident_id,CASE WHEN n.record_valid AND from_iso8601_timestamp(n.record_time)>from_iso8601_timestamp("+literal(manifest['cutoff'])+") THEN 'deferred' WHEN n.record_valid THEN 'duplicate' ELSE 'quarantined' END disposition,CASE WHEN n.record_valid AND from_iso8601_timestamp(n.record_time)>from_iso8601_timestamp("+literal(manifest['cutoff'])+") THEN cast(NULL as varchar) WHEN n.record_valid THEN 'BR-002' ELSE 'BR-001' END rule_id,cast(NULL as varchar) reason FROM "+pinned(current)+" n WHERE n.record_incident IS NOT NULL AND n.record_incident NOT IN "+rows_in(selected))
    return {name: ref(client, 'lake.'+schema+'.'+relation) for name, relation in
            [('state','incident_state'),('quarantine','quarantine'),('contributions','incident_daily_contributions'),('dispositions','record_dispositions')]}


def run(batch, workspace, fail_at=None, api_url=None, policy_path=None):
    workspace = Path(workspace)
    with workspace_lock(workspace):
        return _run(Path(batch), workspace, fail_at, api_url, policy_path)


def _run(batch, workspace, fail_at, api_url, policy_path):
    attempt = uuid.uuid4().hex; started = time.perf_counter()
    evidence = {'attempt_id':attempt,'status':'running','stage':'incoming_validation','strategy':'targeted',
                'revision':sql_revision(),'started_at':iso(datetime.now(timezone.utc)),'dbt':[]}
    path = workspace/'attempts'/(attempt+'.json'); atomic_json(path, evidence)
    client = None
    try:
        if fail_at not in (None, *FAILPOINTS): raise ValueError('unsupported targeted failpoint')
        manifest, incoming, checked = load_batch(batch, api_url, workspace/'api-landing'/attempt)
        policy = load_policy(policy_path)
        identity = hashlib.sha256(canonical([checked['manifest_sha256'], evidence['revision'], policy, 'targeted']).encode()).hexdigest()
        parent = next(committed_publications(workspace), None)
        for previous in committed_publications(workspace):
            if previous['batch_id'] == manifest['batch_id']:
                if identity != previous['identity']: raise ValueError('committed source/code/policy changed; use a fresh workspace')
                evidence.update(status='already_committed', publication_id=previous['publication_id']); return evidence
        hashes = reference_hashes(incoming)
        if parent:
            if parent.get('strategy') != 'targeted': raise ValueError('targeted processing requires its own workspace')
            if parent['revision'] != evidence['revision'] or parent['quality_policy'] != policy: raise ValueError('code/policy changed; use a fresh workspace')
            if manifest['parent_batch_id'] != parent['batch_id'] or timestamp(manifest['cutoff']) < timestamp(parent['cutoff']): raise ValueError('delta must follow active parent and cutoff')
            if manifest['report_start'] != parent['report_start']: raise ValueError('reporting start changed')
            if hashes != parent['reference_hashes']: raise ValueError('unsupported v1 reference mutation')
        elif manifest['batch_kind'] != 'initial': raise ValueError('process initial batch first')
        evidence['run_specification'] = {'parent_publication_id':parent['publication_id'] if parent else None,
            'input_manifest_sha256':checked['manifest_sha256'],'previous_cutoff':parent['cutoff'] if parent else None,
            'cutoff':manifest['cutoff'],'rule_version':manifest['rule_version'],'dataset_version':manifest['dataset_version'],
            'report_start':manifest['report_start'],'reference_hashes':hashes,'quality_policy':policy,
            'parent_input_checkpoint':parent['input_checkpoint'] if parent else None}
        archive = workspace/'sources'/checked['manifest_sha256']
        if not archive.exists():
            temporary = archive.with_name(archive.name+'.'+attempt+'.tmp')
            try:
                shutil.copytree(batch, temporary); load_batch(temporary); temporary.rename(archive)
            finally:
                if temporary.exists(): shutil.rmtree(temporary)
        archived_batch(archive)  # only the incoming delivery; historical audit is separate
        store = object_store()
        for file in manifest['files'] + [{'path':'manifest.json'}]:
            source = safe_file(archive, file['path'])
            store.put_object(Bucket='raw',Key=checked['manifest_sha256']+'/'+file['path'],Body=source.read_bytes(),Metadata={'sha256':digest(source)})
        for page in (workspace/'api-landing'/attempt).glob('*.json'):
            store.put_object(Bucket='raw',Key='api/'+attempt+'/'+page.name,Body=page.read_bytes())
        evidence['incoming_bytes'] = sum(f['bytes'] for f in manifest['files'])
        evidence['historical_raw_bytes_audited'] = 0
        evidence['stage'] = 'input_persistence'
        client = Client(); client.stage = evidence['stage']
        schema = 'dpe_inc_'+attempt[:16]; client.sql('CREATE SCHEMA lake.'+schema)
        evidence['schema'] = schema
        evidence['engine_version'] = {'trino':client.sql('SELECT version() version')[0]['version']}
        current = delivery(client, schema, manifest, incoming, parent, checked['manifest_sha256'])
        deliveries = (parent['input_checkpoint']['deliveries'] if parent else []) + [current]
        evidence['input_delivery'] = current
        if fail_at == 'after_input_persistence': raise RuntimeError('injected failure: '+fail_at)
        evidence['stage'] = client.stage = 'candidate_discovery'
        selected = candidates(client, schema, current, parent, manifest)
        evidence['candidates'] = {'incident_ids':selected,'count':len(selected),
            'reasons':client.sql('SELECT reason,count(*) incidents FROM lake.'+schema+'.affected_incidents GROUP BY 1')}
        evidence['stage'] = client.stage = 'history_selection'
        materialize_histories(client, schema, deliveries, selected, manifest)
        transform(client, schema, workspace, attempt, parent, selected, fail_at, evidence)
        evidence['stage'] = client.stage = 'validation'
        checkpoints = checkpoint_tables(client, schema, parent, current, deliveries, selected, manifest)
        client.stage = 'validation'
        population = client.sql('SELECT incident_id FROM '+pinned(checkpoints['state']))
        quarantines = client.sql('SELECT DISTINCT incident_id FROM '+pinned(checkpoints['quarantine']))
        evidence['quality'] = enforce_quality({'quarantined_incidents':{r['incident_id']:[] for r in quarantines}},
            {'incident_snapshots':population,'incident_events':[]}, policy)
        counts = {name: (parent['input_checkpoint']['rows'][name] if parent else 0)+len(incoming[name]) for name in DATASETS}
        evidence['reconciliation'] = client.sql('SELECT dataset,disposition,count(*) rows FROM '+pinned(checkpoints['dispositions'])+' GROUP BY 1,2')
        evidence['delivery_reconciliation'] = client.sql('SELECT dataset,disposition,count(*) rows FROM '+pinned(checkpoints['dispositions'])+' WHERE '+ ' OR '.join('(dataset='+literal(name)+' AND ordinal>='+str(counts[name]-len(incoming[name]))+')' for name in DATASETS)+' GROUP BY 1,2')
        for name in DATASETS:
            if sum(r['rows'] for r in evidence['reconciliation'] if r['dataset']==name) != counts[name]: raise AssertionError('unbalanced historical dispositions: '+name)
            if sum(r['rows'] for r in evidence['delivery_reconciliation'] if r['dataset']==name) != len(incoming[name]): raise AssertionError('unbalanced incoming dispositions: '+name)
        evidence['daily_work'] = client.sql('SELECT count(*) groups FROM lake.'+schema+'.affected_daily_groups')[0]
        snapshots = {}
        for name in OUTPUTS:
            snapshots[name] = ref(client, 'lake.'+schema+'.gold_'+name)
            rows = client.sql('SELECT * FROM '+pinned(snapshots[name]))
            for row in rows: validate_schema({k:v for k,v in row.items() if k!='case_id'}, SCHEMAS[name])
            snapshots[name]['rows'] = len(rows)
        # Include physical files read/written by the isolated publication's persistent products.
        evidence['storage'] = {}
        for name in ['delivery','gold_incident_resolution','gold_service_daily_metrics','incident_daily_contributions']:
            evidence['storage'][name] = client.sql('SELECT count(*) files,coalesce(sum(file_size_in_bytes),0) bytes FROM lake.'+schema+'."'+name+'$files"')[0]
        evidence['stage'] = client.stage = 'publication'
        if fail_at == 'before_publish': raise RuntimeError('injected failure: '+fail_at)
        pub_id = hashlib.sha256(canonical([identity, parent['publication_id'] if parent else None]).encode()).hexdigest()
        publication = dict(checkpoints, publication_id=pub_id, identity=identity,batch_id=manifest['batch_id'],
            parent_publication_id=parent['publication_id'] if parent else None,cutoff=manifest['cutoff'],
            report_start=manifest['report_start'],revision=evidence['revision'],strategy='targeted',rule_version='v1',contract_version='v1',
            quality_policy=policy,reference_hashes=hashes,snapshots=snapshots,attempt_id=attempt,
            input_checkpoint={'deliveries':deliveries,'rows':counts},
            archives=(parent['archives'] if parent else [])+[str(archive.relative_to(workspace))])
        atomic_json(workspace/'publications'/(pub_id+'.json'), publication)
        atomic_json(workspace/'active.json', publication)
        evidence.update(status='committed',publication_id=pub_id,snapshots=snapshots)
        return evidence
    except Exception as error:
        evidence.update(status='failed',error=str(error)); raise
    finally:
        evidence.update(runtime_seconds=round(time.perf_counter()-started,6),finished_at=iso(datetime.now(timezone.utc)))
        evidence['queries'] = client.queries if client else []
        atomic_json(path, evidence)


def audit(workspace):
    """Explicitly pay the cost of validating every committed raw delivery."""
    started = time.perf_counter(); rows = []; size = 0
    parent = next(committed_publications(workspace), None)
    if parent is None: raise ValueError('workspace has no committed publication')
    for archive in parent['archives']:
        path = safe_file(workspace, archive)
        manifest, _, checked = archived_batch(path)
        size += sum(f['bytes'] for f in manifest['files'])
        rows.append(checked)
    result = {'status':'passed','publication_id':parent['publication_id'],'deliveries':rows,
              'raw_bytes':size,'runtime_seconds':round(time.perf_counter()-started,6)}
    atomic_json(Path(workspace)/'audit.json',result)
    return result
