"""Run the tuned comparisons through real local Airflow, serially and durably."""
import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from dpe.common import ROOT,read_json,write_json
from dpe.experiment import acceptance,checksum


def run(criteria_path,prefix='bounded'):
    criteria=acceptance(criteria_path);environment=dict(os.environ,DOCKER_CONTEXT='colima-dpe-capacity')
    phases=[('capacity-custom-'+prefix+'-10000',10000),('capacity-small-'+prefix+'-100000',100000)]
    evidence={'status':'running','criteria_sha256':checksum(criteria),'phases':[]}
    output=ROOT/'data'/('capacity-comparisons-'+prefix+'.json')
    try:
        for workspace,count in phases:
            deployed=subprocess.run(['docker','exec','dpe-local-benchmark-coordinator-1','python','-c','from dpe.lakehouse import sql_revision; print(sql_revision())'],env=environment,capture_output=True,text=True,check=True).stdout.strip()
            if evidence.get('implementation_revision') and evidence['implementation_revision']!=deployed:raise ValueError('implementation changed between campaign phases')
            evidence['implementation_revision']=deployed
            path=ROOT/'data'/workspace/'run_results.json'
            if path.exists():
                old=read_json(path)
                if old['specification']['criteria_sha256']!=checksum(criteria):raise ValueError('existing experiment has different criteria: '+workspace)
                if old['specification']['revision']!=deployed:raise ValueError('existing experiment has different implementation: '+workspace)
                if old['status']!='passed':raise ValueError('existing experiment is incomplete/failed; retain and investigate: '+workspace)
            else:
                conf={'workspace':workspace,'count':count,'repeats':3,'acceptance':criteria_path}
                command=['docker','exec','dpe-local-airflow-1','airflow','dags','trigger','dpe_benchmarks','--run-id',workspace,'--conf',json.dumps(conf)]
                with (ROOT/'data'/(workspace+'-submit.log')).open('w') as stream:
                    subprocess.run(command,cwd=ROOT,env=environment,stdout=stream,stderr=subprocess.STDOUT,check=True)
                begin=time.monotonic()
                while True:
                    if path.exists() and read_json(path)['status'] in ('passed','failed','rejected'):break
                    if time.monotonic()-begin>8*3600:raise TimeoutError('campaign phase did not reach terminal state')
                    time.sleep(5)
            result=read_json(path)
            if result['status']!='passed':raise RuntimeError(workspace+': '+result.get('error',result['status']))
            # Preserve a real orchestration result, including validate after execute.
            for _ in range(20):
                state=subprocess.run(['docker','exec','dpe-local-airflow-1','airflow','dags','list-runs','dpe_benchmarks','-o','json'],env=environment,capture_output=True,text=True,check=True)
                (ROOT/'data'/(workspace+'-airflow-runs.json')).write_text(state.stdout)
                try:found=next(r for r in json.loads(state.stdout) if r['run_id']==workspace)
                except (ValueError,KeyError,StopIteration):found={}
                if found.get('state')=='success':break
                if found.get('state')=='failed':raise RuntimeError('Airflow validation failed: '+workspace)
                time.sleep(3)
            else:raise RuntimeError('Airflow success could not be verified: '+workspace)
            evidence['phases'].append({'workspace':workspace,'status':result['status'],'performance_status':result.get('performance_status'),'runs':len(result['runs']),'airflow':found})
            write_json(output,evidence)
        evidence['status']='passed'
    except Exception as error:evidence.update(status='failed',error=str(error));raise
    finally:write_json(output,evidence)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--acceptance',default='benchmarks/specs/acceptance.poc.v4.json')
    parser.add_argument('--prefix',default='bounded')
    args=parser.parse_args()
    import re
    if not re.fullmatch('[a-z0-9-]+',args.prefix):raise ValueError('prefix requires lowercase letters, digits or hyphens')
    run(args.acceptance,args.prefix)
