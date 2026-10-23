"""Isolated read-workload SQL and independently calculated expected rows."""
from collections import defaultdict
from .common import canonical,timestamp,iso
from .rules import normalize


def queries(schema,engine='trino'):
    prefix='lake.'+schema+'.'
    seconds=lambda expression:('to_unixtime('+expression+')' if engine=='trino' else '(unix_micros('+expression+')/1000000.0)')
    return {
      'W1':"SELECT incident_id,source_version,priority FROM "+prefix+"eligible_snapshots WHERE priority='P1'",
      'W2':'''WITH ranked AS (SELECT *,row_number() OVER(PARTITION BY case_id,incident_id ORDER BY source_updated_at DESC,source_version DESC) n FROM '''+prefix+'''eligible_snapshots)
       SELECT s.incident_id,s.service_id,s.priority,count(e.event_id) event_count
       FROM ranked s JOIN '''+prefix+'''stg_references svc ON svc.case_id=s.case_id AND svc.dataset='services' AND svc.reference_id=s.service_id
       JOIN '''+prefix+'''stg_references grp ON grp.case_id=s.case_id AND grp.dataset='assignment_groups' AND grp.reference_id=s.group_id
       LEFT JOIN '''+prefix+'''eligible_events e ON e.case_id=s.case_id AND e.incident_id=s.incident_id
       WHERE s.n=1 GROUP BY s.incident_id,s.service_id,s.priority''',
      'W3':'''WITH ordered AS (SELECT incident_id,event_id,event_time,source_sequence,
       row_number() OVER(PARTITION BY incident_id ORDER BY event_time,source_sequence) event_rank,
       lag(event_time) OVER(PARTITION BY incident_id ORDER BY event_time,source_sequence) previous_time FROM '''+prefix+'''eligible_events)
       SELECT incident_id,event_id,event_rank,'''+seconds('event_time')+'-'+seconds('previous_time')+' elapsed_seconds FROM ordered'}


def expected(datasets,cutoff):
    snapshots={};events={};cutoff=timestamp(cutoff)
    for raw in datasets['incident_snapshots']:
        row=normalize('incident_snapshots',raw)
        if timestamp(row['source_updated_at'])<=cutoff:snapshots[(row['incident_id'],row['source_version'])]=row
    for raw in datasets['incident_events']:
        row=normalize('incident_events',raw)
        if timestamp(row['event_time'])<=cutoff:events[row['event_id']]=row
    selected=defaultdict(list);history=defaultdict(list)
    for r in snapshots.values():selected[r['incident_id']].append(r)
    for r in events.values():history[r['incident_id']].append(r)
    result={'W1':[],'W2':[],'W3':[]}
    for r in snapshots.values():
        if r['priority']=='P1':result['W1'].append({k:r[k] for k in ['incident_id','source_version','priority']})
    for key,values in selected.items():
        latest=max(values,key=lambda r:(timestamp(r['source_updated_at']),r['source_version']))
        result['W2'].append({'incident_id':key,'service_id':latest['service_id'],'priority':latest['priority'],'event_count':len(history[key])})
    for key,values in history.items():
        previous=None
        for rank,r in enumerate(sorted(values,key=lambda r:(timestamp(r['event_time']),r['source_sequence'])),1):
            now=timestamp(r['event_time'])
            result['W3'].append({'incident_id':key,'event_id':r['event_id'],'event_rank':rank,'elapsed_seconds':(now-previous).total_seconds() if previous else None});previous=now
    return result


def equal(expected,actual):
    return sorted(map(canonical,expected))==sorted(map(canonical,actual))
