"""Private asynchronous local coordinator used by Airflow; no Docker socket."""
import argparse
import hashlib
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from .common import ROOT,canonical,read_json
from .lakehouse import atomic_json

EXECUTOR=ThreadPoolExecutor(max_workers=1)
LOCK=threading.Lock()
JOBS=ROOT/'data/benchmark-jobs'


def execute_job(identity,spec):
    from .benchmark_runner import run
    path=JOBS/(identity+'.json')
    value={'job_id':identity,'status':'running','request':spec};atomic_json(path,value)
    try:
        value['result']=run(ROOT/'data'/spec['workspace'],spec.get('count',1000),spec.get('repeats',3),tuple(spec.get('engines',['trino','spark'])),spec.get('orchestration'))
        value['status']='passed'
    except Exception as error:value.update(status='failed',error=str(error))
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
    # A process restart must not leave a request claiming it is still running.
    for path in JOBS.glob('*.json'):
        value=read_json(path)
        if value['status'] in ('running','queued'):
            value.update(status='interrupted',error='coordinator restarted; retained workspace needs explicit inspection');atomic_json(path,value)
    ThreadingHTTPServer(('0.0.0.0',args.port),Handler).serve_forever()

if __name__=='__main__':main()
