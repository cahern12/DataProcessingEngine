"""Private local submission service; no Docker socket is exposed to Airflow."""
import json
import subprocess
import threading
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
import sys
sys.path.insert(0,'/app')
from dpe.common import write_json,read_json

LOCK=threading.Lock()
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200);self.end_headers();self.wfile.write(b'{"status":"ready"}')
    def do_POST(self):
        try:
            value=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            folder=Path('/app/data')/value['relative_folder']
            if Path('/app/data') not in folder.resolve().parents:raise ValueError('artifact path escapes data')
            folder.mkdir(parents=True,exist_ok=True);write_json(folder/'input.json',value['spec'])
            with LOCK,(folder/'console.log').open('w') as log:
                command=['/opt/spark/bin/spark-submit','--driver-memory','2g','--conf','spark.driver.host=spark-gateway',
                    '--conf','spark.sql.shuffle.partitions=2','/app/dpe/engines/spark_job.py','--input',str(folder/'input.json'),
                    '--output',str(folder/'result.json'),'--master',value.get('master','local[4]')]
                if value.get('fail_at'):command+=['--fail-at',value['fail_at']]
                process=subprocess.run(command,stdout=log,stderr=subprocess.STDOUT)
            result=read_json(folder/'result.json') if (folder/'result.json').exists() else {'status':'failed','error':'Spark process produced no report'}
            result['exit_code']=process.returncode
            self.send_response(200);self.end_headers();self.wfile.write(json.dumps(result).encode())
        except Exception as error:
            self.send_response(500);self.end_headers();self.wfile.write(json.dumps({'status':'failed','error':str(error)}).encode())
    def log_message(self,*args):pass

if __name__=='__main__':ThreadingHTTPServer(('0.0.0.0',18111),Handler).serve_forever()
