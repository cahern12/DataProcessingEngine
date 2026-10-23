"""After restart, verify every published Gold pair and active input snapshots."""
import argparse
import sys
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from dpe.common import DATASETS, read_json, write_json
from dpe.incremental import pinned, raw_union
from dpe.incremental_validation import restart_check
from dpe.lakehouse import Client


def verify(workspace):
    report = restart_check(workspace)
    client = Client(); report['input_checkpoints'] = []
    for platform in [workspace/'edge_cases/platform',workspace/'controlled/targeted',workspace/'open_heavy/targeted']:
        publication = read_json(platform/'active.json')
        counts = dict.fromkeys(DATASETS,0)
        for delivery in publication['input_checkpoint']['deliveries']:
            for row in client.sql('SELECT dataset,count(*) rows FROM '+pinned(delivery)+' GROUP BY 1'):
                counts[row['dataset']] += row['rows']
        if counts != publication['input_checkpoint']['rows']:
            raise AssertionError('restart changed committed delivery membership')
        raw_ids = client.sql('SELECT count(distinct record_incident) identities FROM ('+raw_union(
            publication['input_checkpoint']['deliveries'],'record_incident IS NOT NULL','record_incident')+')')[0]['identities']
        state_ids = client.sql('SELECT count(*) rows,count(distinct incident_id) identities FROM '+pinned(publication['state']))[0]
        if raw_ids!=state_ids['identities'] or raw_ids!=state_ids['rows']:
            raise AssertionError('restart changed population index')
        dispositions = client.sql('SELECT count(*) rows FROM '+pinned(publication['dispositions']))[0]['rows']
        if dispositions!=sum(counts.values()): raise AssertionError('restart changed reconciliation')
        report['input_checkpoints'].append({'publication_id':publication['publication_id'],'delivery_rows':counts,
                                           'population':raw_ids,'dispositions':dispositions,'status':'passed'})
    report['input_queries'] = client.queries
    write_json(workspace/'restart.json',report)
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--workspace',type=Path,required=True)
    result=verify(parser.parse_args().workspace)
    print('Restart verification passed: %d publications; %d input checkpoints' %
          (len(result['publications']),len(result['input_checkpoints'])))
