"""Private Spark submission/cancellation service; no Docker socket."""
import json
import os
import signal
import subprocess
import threading
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
import sys
sys.path.insert(0,'/app')
from dpe.common import write_json,read_json

LOCK=threading.Lock();PROCESS_LOCK=threading.Lock();PROCESSES={}


def cancel(process):
    if process.poll() is None:
        os.killpg(process.pid,signal.SIGTERM)
        try:process.wait(timeout=10)
        except subprocess.TimeoutExpired:os.killpg(process.pid,signal.SIGKILL);process.wait()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200);self.end_headers();self.wfile.write(b'{"status":"ready"}')
    def do_POST(self):
        try:
            value=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            folder=(Path('/app/data')/value['relative_folder']).resolve()
            if Path('/app/data') not in folder.parents:raise ValueError('artifact path escapes data')
            if self.path=='/cancel':
                with PROCESS_LOCK:process=PROCESSES.get(str(folder))
                if process:cancel(process)
                result={'status':'cancelled'}
            else:
                folder.mkdir(parents=True,exist_ok=True);write_json(folder/'input.json',value['spec'])
                with LOCK,(folder/'console.log').open('w') as log:
                    command=['/opt/spark/bin/spark-submit','--driver-memory','2g','--conf','spark.driver.host=spark-gateway',
                        '--conf','spark.sql.shuffle.partitions=2','/app/dpe/engines/spark_job.py','--input',str(folder/'input.json'),
                        '--output',str(folder/'result.json'),'--master',value.get('master','local[4]')]
                    if value.get('fail_at'):command+=['--fail-at',value['fail_at']]
                    process=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
                    with PROCESS_LOCK:PROCESSES[str(folder)]=process
                    try:process.wait(timeout=value['spec'].get('query_watchdog_seconds',0)*3+120 if value['spec'].get('query_watchdog_seconds') else 1800)
                    except subprocess.TimeoutExpired:cancel(process)
                    finally:
                        with PROCESS_LOCK:PROCESSES.pop(str(folder),None)
                result=read_json(folder/'result.json') if (folder/'result.json').exists() else {'status':'failed','error':'Spark process produced no report'}
                result['exit_code']=process.returncode
                if process.returncode:result.update(status='failed',error=result.get('error','Spark process exited '+str(process.returncode)))
            self.send_response(200);self.end_headers();self.wfile.write(json.dumps(result).encode())
        except Exception as error:
            self.send_response(500);self.end_headers();self.wfile.write(json.dumps({'status':'failed','error':str(error)}).encode())
    def log_message(self,*args):pass

if __name__=='__main__':ThreadingHTTPServer(('0.0.0.0',18111),Handler).serve_forever()
