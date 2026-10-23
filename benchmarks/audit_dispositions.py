"""Independent row-level disposition audit, outside timed workload boundaries."""
import argparse
import itertools
import sys
import time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from dpe.common import canonical,read_json,write_json
from dpe.bounded_io import sources,release
from dpe.experiment import admission
from dpe.lakehouse import Client,committed_publications,normalized
from dpe.rules import evaluate


def audit(workspace):
    workspace=Path(workspace);started=time.perf_counter();groups={};result={'status':'running','publications':[],
        'boundary':'Independent additional audit, outside timed benchmark. Exact dataset/ordinal/incident/disposition/rule ID; free-text reason wording excluded.'}
    for path in workspace.rglob('active.json'):
        platform=path.parent
        if not (platform/'publications').exists():continue
        for publication in committed_publications(platform):
            archives=[platform/value for value in publication['archives']]
            key=canonical([read_json(p/'manifest.json') for p in archives])
            groups.setdefault(key,[]).append((platform,publication,archives))
    if not groups:raise ValueError('no committed publications to audit')
    try:
        with admission(workspace):
            for entries in groups.values():
                _,publication,archives=entries[0];header,records,_,_=sources(archives)
                oracle=evaluate(records,header['cutoff'],header['report_start'])
                expected=sorted(oracle['dispositions'],key=lambda r:(r['dataset'],r['row']))
                del oracle;release()
                for platform,publication,archives in entries:
                    prefix=publication['snapshots']['incident_resolution']['table'].rsplit('.',1)[0]
                    client=Client();cursor=client.connection.cursor();rows=0;differences=[];begin=time.perf_counter()
                    try:
                        cursor.execute('SELECT dataset,ordinal,incident_id,disposition,rule_id FROM '+prefix+'.record_dispositions ORDER BY dataset,ordinal')
                        def streamed():
                            while True:
                                chunk=cursor.fetchmany(5000)
                                if not chunk:return
                                for row in chunk:yield tuple(normalized(v) for v in row)
                        desired=((r['dataset'],r['row'],r['incident_id'],r['disposition'],r['rule_id']) for r in expected)
                        for left,right in itertools.zip_longest(desired,streamed()):
                            rows+=1
                            if left!=right:
                                if len(differences)<5:differences.append({'expected':left,'actual':right})
                        entry={'platform':str(platform),'publication_id':publication['publication_id'],'rows_compared':rows,'status':'failed' if differences else 'passed',
                               'differences':differences,'seconds':time.perf_counter()-begin,'query_id':cursor.query_id,'stats':cursor.stats}
                        result['publications'].append(entry)
                        if differences:raise AssertionError('independent disposition mismatch: '+canonical(differences))
                    finally:cursor.close()
                    write_json(workspace/'disposition_audit.json',result);release()
                del expected;release()
        result['status']='passed';return result
    except Exception as error:result.update(status='failed',error=str(error));raise
    finally:
        result['runtime_seconds']=time.perf_counter()-started
        write_json(workspace/'disposition_audit.json',result)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--workspace',required=True)
    print(audit(parser.parse_args().workspace)['status'])
