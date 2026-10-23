"""Re-read every committed Gold pin after service restart, including failed runs."""
import argparse
import hashlib
import resource
import re
import sys
import time
from datetime import datetime
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from dpe.common import canonical,read_json,write_json,digest
from dpe.bounded_io import sources,release
from dpe.lakehouse import OUTPUTS,committed_publications,consume,compare_rows
from dpe.rules import evaluate


def verify(workspaces,output,restart_evidence=None):
    started=time.perf_counter();groups={};result={'status':'running','publications':[],
        'boundary':'Host-client exact Gold audit, outside timed benchmark workloads; failed experiments retain their original status',
        'restart_status':'not_assessed: no service restart receipt supplied'}
    if restart_evidence:
        receipt=read_json(restart_evidence)
        if receipt['status']!='passed':raise ValueError('successful service restart receipt required')
        for service in ('postgres','objectstore','catalog','trino'):
            item=receipt['services'][service]
            # Docker emits nanoseconds; Python 3.9 accepts at most microseconds.
            def docker_time(value):return datetime.fromisoformat(re.sub(r'(\.\d{6})\d+(?=Z|[+-])',r'\1',value).replace('Z','+00:00'))
            before=docker_time(item['before_started_at']);after=docker_time(item['after_started_at'])
            if after<=before or item['health']!='healthy':
                raise ValueError('restart/health evidence incomplete for '+service)
        result.update(restart_status='passed',restart_evidence=str(restart_evidence),restart_evidence_sha256=digest(Path(restart_evidence)))
    for workspace in map(Path,workspaces):
        for pointer in workspace.rglob('active.json'):
            platform=pointer.parent
            if not (platform/'publications').exists():continue
            for publication in committed_publications(platform):
                archives=[platform/path for path in publication['archives']]
                key=hashlib.sha256(canonical([read_json(path/'manifest.json') for path in archives]).encode()).hexdigest()
                groups.setdefault(key,[]).append((platform,publication,archives))
    try:
        if not groups:raise ValueError('no committed publications to verify')
        for entries in groups.values():
            _,_,archives=entries[0];header,records,_,_=sources(archives)
            oracle=evaluate(records,header['cutoff'],header['report_start'])
            for key in list(oracle):
                if key not in OUTPUTS:del oracle[key]
            release()
            for platform,publication,_ in entries:
                for name in OUTPUTS:
                    actual=consume(platform,name,publication_id=publication['publication_id'])['rows']
                    differences=compare_rows(name,oracle[name],actual)
                    if differences:raise AssertionError('pinned '+name+' differs: '+canonical(differences[:5]))
                    del actual;release()
                result['publications'].append({'platform':str(platform),'publication_id':publication['publication_id'],'status':'passed'})
                write_json(output,result)
            del oracle;release()
        result['status']='passed';return result
    except Exception as error:result.update(status='failed',error=str(error));raise
    finally:
        result['runtime_seconds']=time.perf_counter()-started
        peak=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        result['peak_process_rss_bytes']=peak if sys.platform=='darwin' else peak*1024
        write_json(output,result)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--workspace',action='append',required=True);parser.add_argument('--out',required=True)
    parser.add_argument('--restart-evidence')
    args=parser.parse_args();print(verify(args.workspace,args.out,args.restart_evidence)['status'])
