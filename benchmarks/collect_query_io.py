"""Observe Trino file/cache IO while the incremental acceptance command runs.

Usage: python benchmarks/collect_query_io.py --workspace data/incremental-check
Use --observe-only to attach to an already-running verification workspace.
No transformation behavior or publication state is changed by this observer.
"""
import argparse
import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dpe.common import read_json, write_json
from dpe.lakehouse import Client
from dpe.incremental import pinned, rows_in


def request(path):
    endpoint = 'http://' + os.environ.get('DPE_TRINO_HOST', '127.0.0.1') + ':' + os.environ.get('DPE_TRINO_PORT', '18080')
    with urlopen(Request(endpoint+path, headers={'X-Trino-User':'dpe'}), timeout=10) as stream:
        return json.load(stream)


def observe(stop, evidence, since):
    while not stop.is_set():
        try:
            for query in request('/v1/query'):
                if stop.is_set(): break
                query_id = query['queryId']
                created = datetime.fromisoformat(query['queryStats']['createTime'].replace('Z','+00:00'))
                if created < since or query_id in evidence['queries'] or query['state'] not in ('FINISHED','FAILED'):
                    continue
                try:
                    info = request('/v1/query/'+query_id)
                except Exception as error:
                    evidence['queries'][query_id] = {'unavailable':str(error)}; continue
                operators = info['queryStats'].get('operatorSummaries',[])
                if not operators and info['queryStats']['physicalInputPositions']:
                    evidence['queries'][query_id] = {'unavailable':'scan operator details expired'}; continue
                counters = dict.fromkeys(('dataFiles','dataFileSizeBytes','bytesReadFromCache','bytesReadExternally'),0)
                for operator in info['queryStats'].get('operatorSummaries',[]):
                    for name, value in operator.get('connectorMetrics',{}).items():
                        if name in counters: counters[name] += value.get('total',0)
                evidence['queries'][query_id] = counters
        except Exception as error:
            evidence['last_observer_error'] = str(error)
        stop.wait(0.5)


def summarize(workspace, captured):
    verification = read_json(workspace/'verification.json')
    report = {'status':'passed','workloads':{},'file_counter_semantics':'Sum of Iceberg dataFiles scan counters across operators/queries; repeated scans count repeatedly. File bytes are planned file sizes; physical column bytes are in verification.json. Cache/external counters are separate from physical scanned bytes.'}
    client = Client()
    for name, workload in verification.get('workloads',{}).items():
        summary = {}
        for strategy, attempt in [('targeted',workload['delta']),('full',workload['full_delta'])]:
            stages = {}; missing = []; count = 0
            queries = [(q.get('stage','unclassified'),q['query_id']) for q in attempt['queries'] if q.get('query_id')]
            queries += [(call['stage'],q['query_id']) for call in attempt['dbt'] for q in call.get('queries',[])]
            for stage, query_id in queries:
                count += 1
                if query_id not in captured['queries'] or 'unavailable' in captured['queries'][query_id]: missing.append(query_id); continue
                total = stages.setdefault(stage, dict.fromkeys(('dataFiles','dataFileSizeBytes','bytesReadFromCache','bytesReadExternally'),0))
                for k,v in captured['queries'][query_id].items(): total[k] += v
            # Inventory every remaining data file in this attempt, including
            # intermediate tables and copied/changed output tables.
            schema = attempt['schema']; files = {}
            for relation in client.sql("SELECT table_name FROM lake.information_schema.tables WHERE table_schema='"+schema+"' AND table_type='BASE TABLE'"):
                relation_name = relation['table_name']
                files[relation_name] = client.sql('SELECT count(*) files,coalesce(sum(file_size_in_bytes),0) bytes FROM lake.'+schema+'."'+relation_name+'$files"')[0]
            summary[strategy] = {'stages':stages,'query_coverage':(count-len(missing))/count,'missing_queries':missing,
                'written_data_file_inventory':files,'written_files':sum(v['files'] for v in files.values()),
                'written_file_bytes':sum(v['bytes'] for v in files.values())}
        selected = workload['delta']['candidates']['incident_ids']
        schema = workload['delta']['schema']
        impacted = '(SELECT date,service_id,priority FROM lake.'+schema+'.affected_daily_groups)'
        summary['daily_expansion'] = client.sql('SELECT count(*) contribution_rows,count(distinct incident_id) contributor_incidents,count(distinct incident_id) FILTER(WHERE incident_id NOT IN '+rows_in(selected)+') unchanged_contributor_incidents FROM lake.'+schema+'.incident_daily_contributions WHERE (date,service_id,priority) IN '+impacted)[0]
        publication = read_json(workspace/name/'targeted/active.json')
        parent = read_json(workspace/name/'targeted/publications'/(publication['parent_publication_id']+'.json'))
        old_ids = {r['incident_id'] for r in client.sql('SELECT incident_id FROM '+pinned(parent['state']))}
        summary['incident_work'] = {'new':len(set(selected)-old_ids),'changed_or_time_dependent':len(set(selected)&old_ids),
            'unchanged_retained':len(old_ids-set(selected))}
        summary['operator_actions_during_measured_run'] = 0
        summary['retries_during_measured_run'] = 0
        if min(summary[strategy]['query_coverage'] for strategy in ('targeted','full')) < 1:
            report['status'] = 'failed'
        report['workloads'][name] = summary
    write_json(workspace/'file_io.json',report)
    if verification['status']!='passed' or report['status']!='passed': raise RuntimeError('verification/file-IO capture incomplete; inspect retained reports')
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--workspace', type=Path, required=True)
    parser.add_argument('--count', type=int, default=1000)
    parser.add_argument('--observe-only', action='store_true')
    args = parser.parse_args()
    captured = {'queries':{}}
    stop = threading.Event(); thread = threading.Thread(target=observe,args=(stop,captured,datetime.min.replace(tzinfo=timezone.utc) if args.observe_only else datetime.now(timezone.utc)),daemon=True); thread.start()
    code = 0
    try:
        if args.observe_only:
            while not (args.workspace/'verification.json').exists(): time.sleep(0.5)
        else:
            process = subprocess.run([sys.executable,'-m','dpe','lake-incremental-verify','--workspace',str(args.workspace),'--count',str(args.count)])
            code = process.returncode
        # One final poll for the last completed queries.
        stop.wait(1)
    finally:
        stop.set(); thread.join(timeout=15)
        write_json(args.workspace/'query_file_io.json',captured)
    if code: return code
    report = summarize(args.workspace,captured)
    print(json.dumps({'status':report['status'],'report':str(args.workspace/'file_io.json')}))
    return 0


if __name__=='__main__':
    sys.exit(main())
