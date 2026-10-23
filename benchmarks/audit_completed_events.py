"""Audit persisted completed-query events referenced by a comparative workspace."""
import argparse
import sys
import time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from dpe.common import ROOT,read_json,write_json


def audit(workspace,events):
    workspace=Path(workspace);events=Path(events);identities=set()
    def visit(value):
        if isinstance(value,dict):
            if isinstance(value.get('query_id'),str):identities.add(value['query_id'])
            for item in value.values():visit(item)
        elif isinstance(value,list):
            for item in value:visit(item)
    for path in [workspace/'run_results.json',*workspace.glob('full-*-trino.json'),*workspace.glob('full-*-trino/dbt/**/run_results.json'),*workspace.glob('shared/dbt/**/run_results.json')]:
        visit(read_json(path))
    for _ in range(20):
        missing=[key for key in sorted(identities) if not (events/(key+'.json')).exists()]
        if not missing:break
        time.sleep(0.25)
    result={'status':'passed' if identities and not missing else 'incomplete','referenced_query_ids':len(identities),
            'persisted_completed_events':len(identities)-len(missing),'missing':missing,
            'scope':'Query IDs referenced by run/preparation/full-Trino/dbt artifacts; unreferenced metadata/consumer queries excluded'}
    write_json(workspace/'completed-event-audit.json',result)
    if result['status']!='passed':raise AssertionError('completed-query evidence missing')
    return result

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--workspace',required=True);parser.add_argument('--events',default=str(ROOT/'data/telemetry/trino-events'))
    args=parser.parse_args();print(audit(args.workspace,args.events))
