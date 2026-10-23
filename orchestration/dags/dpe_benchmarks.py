"""Manually triggered local POC benchmark; HTTP retries reuse request identity."""
import json
import os
import time
from datetime import datetime,timedelta
from urllib.request import Request,urlopen
from airflow.sdk import dag,task,get_current_context

ENDPOINT=os.environ.get('DPE_BENCHMARK_ENDPOINT','http://benchmark-coordinator:18112')

@dag(dag_id='dpe_benchmarks',schedule=None,start_date=datetime(2026,1,1),catchup=False,max_active_runs=1,
     default_args={'retries':1,'retry_delay':timedelta(seconds=10)},tags=['local','poc'])
def pipeline():
    @task(execution_timeout=timedelta(hours=2))
    def execute():
        context=get_current_context();conf=context['dag_run'].conf or {}
        import hashlib
        workspace=conf.get('workspace','airflow-'+hashlib.sha256(context['run_id'].encode()).hexdigest()[:16])
        spec={'workspace':workspace,'count':conf.get('count',1000),'repeats':conf.get('repeats',3),
            'engines':conf.get('engines',['trino','spark']),
            'orchestration':{'kind':'airflow','dag_id':context['dag'].dag_id,'dag_run_id':context['run_id'],'task_id':'execute'}}
        request=Request(ENDPOINT+'/jobs',data=json.dumps(spec,sort_keys=True).encode(),headers={'Content-Type':'application/json'})
        with urlopen(request,timeout=30) as stream:job=json.load(stream)
        for _ in range(3600):
            with urlopen(ENDPOINT+'/jobs/'+job['job_id'],timeout=30) as stream:state=json.load(stream)
            if state['status']=='passed':return {'workspace':workspace,'job_id':job['job_id'],'result':state['result']}
            if state['status'] in ('failed','interrupted'):raise RuntimeError(json.dumps(state))
            time.sleep(2)
        raise TimeoutError('benchmark coordinator did not finish')
    @task
    def validate(result):
        root=os.environ.get('DPE_DATA_ROOT','/app/data')
        with open(root+'/'+result['workspace']+'/run_results.json') as stream:report=json.load(stream)
        if report['status']!='passed':raise RuntimeError('benchmark failed')
        with open(root+'/'+result['workspace']+'/poc_scorecard.json') as stream:score=json.load(stream)
        if score['recommendation']!='incomplete':raise RuntimeError('local run incorrectly closes original POC')
        return {'benchmark_status':'passed','poc_recommendation':score['recommendation']}
    validate(execute())

pipeline()
