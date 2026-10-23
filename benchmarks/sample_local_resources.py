"""Host-side Docker sampler; diagnostic utilization is separate from billed capacity."""
import argparse
import hashlib
import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


def sample(output, terminal_report, maximum_seconds=1800, interval_seconds=2):
    started=time.monotonic()
    output=Path(output);output.parent.mkdir(parents=True,exist_ok=True)
    metadata={'kind':'local_docker_stats','source_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'interval_seconds':interval_seconds,'maximum_seconds':maximum_seconds,
              'boundary':'All visible local containers, including non-engine services; sampled utilization, cumulative network/block IO, no billed capacity inference.'}
    Path(str(output)+'.metadata.json').write_text(json.dumps(metadata,indent=2)+'\n')
    with output.open('x') as stream:
        while time.monotonic()-started<maximum_seconds:
            begin=time.monotonic()
            result=subprocess.run(['docker','stats','--no-stream','--format','{{json .}}'],capture_output=True,text=True,timeout=30)
            record={'at':datetime.now(timezone.utc).isoformat(),'collector_seconds':time.monotonic()-begin,
                    'status':'captured' if result.returncode==0 else 'failed','containers':[]}
            if result.returncode:
                record['error']=result.stderr
            else:
                record['containers']=[json.loads(line) for line in result.stdout.splitlines() if line.strip()]
            stream.write(json.dumps(record,sort_keys=True)+'\n');stream.flush()
            if Path(terminal_report).exists():
                try:state=json.loads(Path(terminal_report).read_text()).get('status')
                except json.JSONDecodeError:state=None
                if state in ('passed','failed'):return
            time.sleep(interval_seconds)
    raise TimeoutError('Resource sampling stopped at its declared maximum duration')

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--out',required=True);parser.add_argument('--terminal-report',required=True)
    parser.add_argument('--maximum-seconds',type=int,default=1800);parser.add_argument('--interval-seconds',type=float,default=2)
    args=parser.parse_args();sample(args.out,args.terminal_report,args.maximum_seconds,args.interval_seconds)
