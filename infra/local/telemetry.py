"""Persist completed Trino events outside the coordinator's bounded history."""
import json
import os
import re
import uuid
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path

ROOT=Path(os.environ.get('DPE_EVENTS_DIR','/data/trino-events'))
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200);self.end_headers();self.wfile.write(b'{"status":"ready"}')
    def do_POST(self):
        try:
            body=self.rfile.read(int(self.headers.get('Content-Length','0')))
            event=json.loads(body)
            identity=event.get('metadata',{}).get('queryId',event.get('queryId',uuid.uuid4().hex))
            if not re.fullmatch(r'[a-zA-Z0-9_-]+',identity):raise ValueError('invalid query identity')
            ROOT.mkdir(parents=True,exist_ok=True)
            target=ROOT/(identity+'.json');temporary=ROOT/(identity+'.'+uuid.uuid4().hex+'.tmp')
            temporary.write_text(json.dumps(event));temporary.replace(target)
            self.send_response(204);self.end_headers()
        except Exception as error:
            self.send_response(400);self.end_headers();self.wfile.write(str(error).encode())
    def log_message(self,*args):pass

if __name__=='__main__':ThreadingHTTPServer(('0.0.0.0',18110),Handler).serve_forever()
