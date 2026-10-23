"""Restart the local persistence/query services and retain verified health evidence."""
import argparse
import json
import os
import subprocess
import sys
from datetime import datetime,timezone
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from dpe.common import ROOT,read_json,write_json
from dpe.experiment import admission,acceptance,checksum


def restart(output,criteria_path):
    output=Path(output)
    if output.exists():raise ValueError('restart evidence requires a fresh output path')
    criteria=acceptance(criteria_path)
    names=('postgres','objectstore','catalog','trino')
    environment=dict(os.environ,DOCKER_CONTEXT='colima-dpe-capacity')
    compose=['docker-compose','-f','infra/local/compose.yaml','-f','infra/local/compose.benchmark.yaml',
             '-f','infra/local/compose.capacity.yaml','-f',criteria['resources']['runtime_overlay']]
    def states():
        rows=json.loads(subprocess.check_output(['docker','inspect',*['dpe-local-'+name+'-1' for name in names]],env=environment,text=True))
        return {name:row for name,row in zip(names,rows)}
    result={'status':'running','started_at':datetime.now(timezone.utc).isoformat(),
            'criteria_sha256':checksum(criteria),'context':'colima-dpe-capacity','services':{}}
    try:
        with admission(output):
            before=states();write_json(output,result)
            with output.with_suffix('.log').open('w') as log:
                subprocess.run(compose+['restart',*names],cwd=ROOT,env=environment,stdout=log,stderr=subprocess.STDOUT,check=True)
                subprocess.run([sys.executable,'infra/local/capacity.py','up','--acceptance',criteria_path],cwd=ROOT,env=environment,
                               stdout=log,stderr=subprocess.STDOUT,check=True)
            after=states()
            for name in names:
                old,new=before[name],after[name]
                entry={'before_id':old['Id'],'after_id':new['Id'],'image_id':new['Image'],
                       'before_started_at':old['State']['StartedAt'],'after_started_at':new['State']['StartedAt'],
                       'health':new['State'].get('Health',{}).get('Status')}
                result['services'][name]=entry
                if entry['after_started_at']<=entry['before_started_at'] or entry['health']!='healthy':
                    raise ValueError('restart or readiness did not complete: '+name)
            result['status']='passed';return result
    except Exception as error:result.update(status='failed',error=str(error));raise
    finally:
        result['finished_at']=datetime.now(timezone.utc).isoformat();write_json(output,result)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--out',required=True)
    parser.add_argument('--acceptance',default='benchmarks/specs/acceptance.poc.v7.json')
    args=parser.parse_args();print(restart(args.out,args.acceptance)['status'])
