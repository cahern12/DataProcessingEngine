"""Replay staging CTAS with JVM diagnostics; never count this as a benchmark repetition."""
import argparse
import json
import re
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timezone
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from dpe.common import read_json,write_json,digest
from dpe.lakehouse import Client


def run(source,output):
    source=Path(source);output=Path(output)
    if output.exists():raise ValueError('fresh diagnostic workspace required')
    output.mkdir(parents=True);stop=threading.Event()
    docker=['docker','--context','colima-dpe-capacity']
    def command(args):
        before=time.monotonic()
        p=subprocess.run(docker+args,capture_output=True,text=True,timeout=20)
        return {'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr,'collector_seconds':time.monotonic()-before}
    def sample():
        with (output/'memory.jsonl').open('w') as stream:
            while not stop.is_set():
                row={'at':datetime.now(timezone.utc).isoformat()}
                for key,args in [('native',['jcmd','1','VM.native_memory','summary','scale=KB']),
                                 ('heap',['jcmd','1','GC.heap_info']),
                                 ('os',['cat','/proc/1/status','/sys/fs/cgroup/memory.current','/sys/fs/cgroup/memory.stat'])]:
                    try:row[key]=command(['exec','dpe-local-trino-1',*args])
                    except Exception as error:row[key]={'error':str(error)}
                stream.write(json.dumps(row)+'\n');stream.flush();stop.wait(1)
    result={'status':'running','started_at':datetime.now(timezone.utc).isoformat(),'queries':[],
            'scope':'Concurrent staging-only diagnostic, not W4 or a controlled benchmark repetition',
            'limits':'NMT committed/reserved values are not RSS; native libraries may be untracked; diagnostic collection has overhead',
            'source':str(source),'source_specification_sha256':digest(source/'specification.json'),
            'runtime':command(['inspect','dpe-local-trino-1'])}
    write_json(output/'diagnostic.json',result);worker=threading.Thread(target=sample,daemon=True);worker.start()
    started=time.monotonic()
    try:
        schema='dpe_diag_'+uuid.uuid4().hex[:12];Client().sql('CREATE SCHEMA lake.'+schema)
        def execute(name):
            paths=list((source/'shared/dbt').rglob('target/run/dpe_local/models/'+name+'.sql'))
            if len(paths)!=1:raise ValueError('one compiled staging statement required: '+name)
            sql=re.sub(r'(?is)(create\s+table\s+)"lake"\."[^"]+"\.',lambda m:m.group(1)+'"lake"."'+schema+'".',paths[0].read_text(),count=1).strip().rstrip(';')
            (output/(name+'.sql')).write_text(sql);client=Client();begin=time.monotonic()
            try:
                rows=client.sql(sql)
                return {'stage':name,'status':'passed','seconds':time.monotonic()-begin,'result':rows,'query':client.queries[-1]}
            except Exception as error:return {'stage':name,'status':'failed','seconds':time.monotonic()-begin,'error':str(error)}
        with ThreadPoolExecutor(max_workers=2) as pool:
            result['queries']=list(pool.map(execute,['stg_events','stg_snapshots']))
        result['status']='passed' if all(r['status']=='passed' for r in result['queries']) else 'failed'
        return result
    except Exception as error:
        result.update(status='failed',error=str(error));raise
    finally:
        stop.set();worker.join(timeout=65);result['runtime_seconds']=time.monotonic()-started
        result['runtime_after']=command(['inspect','dpe-local-trino-1']);write_json(output/'diagnostic.json',result)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--source',required=True);parser.add_argument('--out',required=True)
    args=parser.parse_args();print(run(args.source,args.out)['status'])
