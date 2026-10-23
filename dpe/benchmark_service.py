"""Private coordinator; each experiment gets an isolated producer process."""
import argparse
import hashlib
import json
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from .common import ROOT,canonical,read_json,write_json
from .experiment import acceptance,checksum
from .lakehouse import atomic_json

EXECUTOR=ThreadPoolExecutor(max_workers=1)
LOCK=threading.Lock()
JOBS=ROOT/'data/benchmark-jobs'


def execute_job(identity,spec):
    path=JOBS/(identity+'.json');folder=JOBS/identity;folder.mkdir(parents=True,exist_ok=True)
    value={'job_id':identity,'status':'running','request':spec};atomic_json(path,value)
    try:
        write_json(folder/'criteria.json',spec['criteria']);write_json(folder/'orchestration.json',spec.get('orchestration',{'kind':'coordinator'}))
        command=[sys.executable,'-m','dpe','poc-benchmark','--workspace',str(ROOT/'data'/spec['workspace']),
                 '--count',str(spec.get('count',1000)),'--repeats',str(spec.get('repeats',3)),
                 '--acceptance',str(folder/'criteria.json'),'--orchestration-json',str(folder/'orchestration.json'),
                 '--engines',*spec.get('engines',['trino','spark'])]
        with (folder/'producer.log').open('w') as log:process=subprocess.run(command,stdout=log,stderr=subprocess.STDOUT)
        if process.returncode:
            from .experiment import interrupted
            interrupted(ROOT/'data'/spec['workspace'],'producer exited with code '+str(process.returncode))
        report=ROOT/'data'/spec['workspace']/'run_results.json'
        result=read_json(report) if report.exists() else {'status':'failed','error':'producer exited without a terminal report'}
        if process.returncode or result['status']!='passed':raise RuntimeError(result.get('error','producer failed; see '+str(folder/'producer.log')))
        value.update(status='passed',result={'status':'passed','workspace':spec['workspace'],'performance_status':result.get('performance_status'),'poc_verdict':'incomplete'})
    except Exception as error:value.update(status='failed',error=str(error))
    finally:
        import os
        if os.environ.get('DPE_RESOURCE_ENDPOINT'):
            try:
                from .experiment_observer import Observer
                Observer(ROOT/'data'/spec['workspace'],spec['criteria']).remote('stop')
            except Exception as error:value['observer_cleanup_error']=str(error)
    atomic_json(path,value)


class Handler(BaseHTTPRequestHandler):
    def send(self,status,value):
        self.send_response(status);self.send_header('Content-Type','application/json');self.end_headers();self.wfile.write(canonical(value).encode())
    def do_GET(self):
        if self.path=='/health':return self.send(200,{'status':'ready'})
        identity=self.path.rsplit('/',1)[-1]
        if len(identity)!=64 or any(c not in '0123456789abcdef' for c in identity):return self.send(400,{'error':'invalid identity'})
        path=JOBS/(identity+'.json')
        self.send(200,read_json(path)) if path.exists() else self.send(404,{'error':'unknown job'})
    def do_POST(self):
        try:
            spec=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            workspace=(ROOT/'data'/spec['workspace']).resolve()
            if not workspace.is_relative_to((ROOT/'data').resolve()):raise ValueError('workspace escapes data')
            spec['criteria']=acceptance(spec.get('criteria') or spec.get('acceptance'))
            spec['criteria_sha256']=checksum(spec['criteria'])
            identity=hashlib.sha256(canonical(spec).encode()).hexdigest();path=JOBS/(identity+'.json')
            with LOCK:
                if path.exists():return self.send(200,read_json(path))
                atomic_json(path,{'job_id':identity,'status':'queued','request':spec})
                EXECUTOR.submit(execute_job,identity,spec)
            self.send(202,{'job_id':identity,'status':'queued'})
        except Exception as error:self.send(400,{'error':str(error)})
    def log_message(self,*args):pass


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--port',type=int,default=18112);args=parser.parse_args()
    JOBS.mkdir(parents=True,exist_ok=True)
    for path in JOBS.glob('*.json'):
        value=read_json(path)
        if value['status'] in ('running','queued'):
            value.update(status='interrupted',error='coordinator restarted; retained workspace needs inspection');atomic_json(path,value)
    ThreadingHTTPServer(('0.0.0.0',args.port),Handler).serve_forever()

if __name__=='__main__':main()
