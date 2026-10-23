"""Host-only read-only Docker observer; never exposes a Docker socket to jobs."""
import argparse
import json
import subprocess
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
LOCK=threading.Lock();ACTIVE={}


def command(*args):
    p=subprocess.run(['docker',*args],capture_output=True,text=True,timeout=30)
    if p.returncode:raise RuntimeError(p.stderr.strip())
    return p.stdout


def state():
    ids=command('ps','-aq','--filter','label=com.docker.compose.project=dpe-local').split()
    rows=json.loads(command('inspect',*ids)) if ids else []
    return [{'name':r['Name'].lstrip('/'),'id':r['Id'],'image':r['Config']['Image'],
             'memory_limit_bytes':r['HostConfig']['Memory'],'state':r['State']} for r in rows]


def runtime():
    raw=json.loads(command('info','--format','{{json .}}'))
    mem=command('exec','dpe-local-telemetry-1','cat','/proc/meminfo')
    memory={line.split(':')[0]:int(line.split()[1])*1024 for line in mem.splitlines() if line.split(':')[0] in ('MemTotal','MemAvailable','SwapTotal','SwapFree')}
    counters={}
    for name in ('benchmark-coordinator','spark-gateway','trino'):
        try:counters[name]={k:int(v) for k,v in (line.split() for line in command('exec','dpe-local-'+name+'-1','cat','/sys/fs/cgroup/memory.events').splitlines())}
        except Exception as error:counters[name]={'unavailable':str(error)}
    return {'vm_memory':memory,'memory_events':counters,'context':command('context','show').strip(),**{k:raw.get(k) for k in ('NCPU','MemTotal','Architecture','ServerVersion','ID')},'containers':state()}


def sample(folder,stop,interval):
    with (folder/'container_samples.jsonl').open('w') as stream:
        while not stop.is_set():
            begin=time.monotonic();row={'at':datetime.now(timezone.utc).isoformat()}
            try:
                raw=command('stats','--no-stream','--format','{{json .}}')
                row.update(status='captured',containers=[json.loads(line) for line in raw.splitlines() if line.strip()])
            except Exception as error:row.update(status='unavailable',error=str(error),containers=[])
            row['collector_seconds']=time.monotonic()-begin
            stream.write(json.dumps(row,sort_keys=True)+'\n');stream.flush()
            stop.wait(interval)


class Handler(BaseHTTPRequestHandler):
    def send(self,status,value):
        self.send_response(status);self.send_header('Content-Type','application/json');self.end_headers();self.wfile.write(json.dumps(value).encode())
    def do_GET(self):
        try:self.send(200,runtime() if self.path=='/runtime' else {'status':'ready'})
        except Exception as error:self.send(500,{'error':str(error)})
    def do_POST(self):
        try:
            value=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            folder=(ROOT/'data'/value['workspace']).resolve()
            if not folder.is_relative_to((ROOT/'data').resolve()):raise ValueError('observer path escapes data')
            key=str(folder)
            with LOCK:
                if self.path=='/start':
                    if ACTIVE:raise ValueError('observer already owns an active experiment')
                    folder.mkdir(parents=True,exist_ok=True)
                    info=runtime();(folder/'runtime_before.json').write_text(json.dumps(info,indent=2)+'\n')
                    stop=threading.Event();worker=threading.Thread(target=sample,args=(folder,stop,value.get('interval_seconds',2)),daemon=True)
                    ACTIVE[key]=(stop,worker);worker.start();return self.send(200,info)
                if self.path=='/stop':
                    item=ACTIVE.pop(key,None)
                    if item:item[0].set();item[1].join(timeout=35)
                    info=runtime();(folder/'runtime_after.json').write_text(json.dumps(info,indent=2)+'\n')
                    return self.send(200,info)
                raise ValueError('unsupported observer operation')
        except Exception as error:self.send(400,{'error':str(error)})
    def log_message(self,*args):pass

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--port',type=int,default=18113);args=parser.parse_args()
    ThreadingHTTPServer(('127.0.0.1',args.port),Handler).serve_forever()
