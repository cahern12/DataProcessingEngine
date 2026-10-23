"""Reproducible capacity-profile commands with host observer and crash cleanup."""
import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from urllib.request import Request,urlopen

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT))
ENV=dict(os.environ,DOCKER_CONTEXT='colima-dpe-capacity')
COMPOSE=['docker-compose','-f','infra/local/compose.yaml','-f','infra/local/compose.benchmark.yaml','-f','infra/local/compose.capacity.yaml']


def observer():
    try:
        with urlopen('http://127.0.0.1:18113/health',timeout=3) as stream:json.load(stream)
        return
    except OSError:pass
    log=(ROOT/'data/capacity-resource-agent.log').open('a')
    process=subprocess.Popen([sys.executable,str(ROOT/'infra/local/resource_agent.py')],cwd=ROOT,env=ENV,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    (ROOT/'data/capacity-resource-agent.pid').write_text(str(process.pid))
    for _ in range(30):
        try:
            with urlopen('http://127.0.0.1:18113/health',timeout=2) as stream:json.load(stream)
            return
        except OSError:time.sleep(.2)
    raise RuntimeError('host observer did not start')


def main():
    parser=argparse.ArgumentParser();parser.add_argument('action',choices=['up','build','benchmark','verify','down'])
    parser.add_argument('--workspace',default='data/capacity-check');parser.add_argument('--count',type=int,default=1000);parser.add_argument('--repeats',type=int,default=3)
    parser.add_argument('--acceptance',default='benchmarks/specs/acceptance.poc.v2.json');args=parser.parse_args()
    with (ROOT/args.acceptance).open() as stream:criteria=json.load(stream)
    if criteria['resources'].get('runtime_overlay'):COMPOSE.extend(['-f',criteria['resources']['runtime_overlay']])
    if args.action=='build':return subprocess.call(COMPOSE+['build','runner','spark','spark-gateway','benchmark-coordinator'],cwd=ROOT,env=ENV)
    if args.action=='down':return subprocess.call(COMPOSE+['--profile','benchmark','--profile','orchestration','down'],cwd=ROOT,env=ENV)
    observer()
    if args.action=='up':return subprocess.call(COMPOSE+['up','-d','--wait','postgres','objectstore','bucket-init','catalog','telemetry','trino','spark-gateway','benchmark-coordinator','airflow'],cwd=ROOT,env=ENV)
    command=['poc-verify','--workspace',args.workspace,'--count',str(args.count),'--acceptance',args.acceptance] if args.action=='verify' else ['poc-benchmark','--workspace',args.workspace,'--count',str(args.count),'--repeats',str(args.repeats),'--acceptance',args.acceptance]
    code=subprocess.call(COMPOSE+['run','--rm','runner',*command],cwd=ROOT,env=ENV)
    if code:
        from dpe.experiment import interrupted
        interrupted(ROOT/args.workspace,'direct producer exited with code '+str(code))
        try:
            body=json.dumps({'workspace':str((Path(args.workspace)/'observations').relative_to('data'))}).encode()
            with urlopen(Request('http://127.0.0.1:18113/stop',data=body,headers={'Content-Type':'application/json'}),timeout=45) as stream:json.load(stream)
        except Exception:pass
    return code

if __name__=='__main__':sys.exit(main())
