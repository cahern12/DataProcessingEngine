"""Versioned deterministic 30/60/90-delivery-day synthetic histories."""
import hashlib
from datetime import timedelta
from pathlib import Path
from .common import DATASETS,canonical,iso,read_json,timestamp,write_json
from .ingestion import load_batch
from .synthetic import generate,load_config,incident,BatchWriter


def generate_sequence(destination,days=30,count=1000,seed=42,config_path=None):
    if days not in (30,60,90) or count<1:raise ValueError('sequence needs 30/60/90 days and positive count')
    destination=Path(destination)
    if destination.exists():raise ValueError('destination exists')
    generate(destination/'base',count=count,seed=seed,config_path=config_path)
    original,base,_=load_batch(destination/'base/initial')
    config=load_config(config_path);config['seed']=seed
    refs={k:base[k] if k not in ('incident_snapshots','incident_events') else [] for k in DATASETS}
    metadata=[];parent=None;pending=[];previous=[]
    identity=hashlib.sha256(canonical([days,count,seed,config,'sequence.v2']).encode()).hexdigest()[:16]
    for day in range(days):
        root=destination/('day-%03d'%day);writer=BatchWriter(root,config['chunk_rows'])
        cutoff=timestamp(original['cutoff'])+timedelta(days=day)
        records={k:list(v) for k,v in (base if day==0 else refs).items()}
        scenarios=[]
        if day:
            # A quiet/outage delivery carries complete reference snapshots only.
            if day%7==0:scenarios.append('quiet')
            elif day%11==0:scenarios.append('source_outage')
            elif day%17==0:
                records['incident_events'].extend(previous);scenarios.append('duplicate_only')
            else:
                snapshots,events=incident(config,day%count)
                late={'incident_id':snapshots[0]['incident_id'],'event_id':'SEQ-%s-%03d'%(identity,day),
                      'event_type':'comment','event_time':iso(cutoff-timedelta(hours=2)),
                      'source_sequence':10000+day,'actor_id':'USR-0000'}
                records['incident_events'].extend(pending+[late]);pending=[];scenarios.append('normal_or_backfill')
                if day%5==0 and previous:
                    records['incident_events']+=previous;scenarios.append('duplicates')
                if day%13==0:
                    future=dict(late,event_id=late['event_id']+'-future',event_time=iso(cutoff+timedelta(hours=12)),source_sequence=20000+day)
                    records['incident_events'].append(future);scenarios.append('future_eligibility')
                if day%14==1 and day>=15:
                    correction=dict(snapshots[-1],source_version=3+day,
                        source_updated_at=iso(timestamp(snapshots[-1]['source_updated_at'])+timedelta(seconds=day)),service_id='SVC-001')
                    records['incident_snapshots'].append(correction);scenarios.append('weeks_late_dimension_correction')
                new_count=max(0,int(count*config['new_fraction']))
                day_config=dict(config,cutoff=iso(cutoff-timedelta(days=1)))
                for index in range(count+(day-1)*new_count,count+day*new_count):
                    new_snapshots,new_events=incident(day_config,index,new=True)
                    records['incident_snapshots'].extend(new_snapshots);records['incident_events'].extend(new_events)
                previous=[late]
            if day%11==0:
                snapshots,_=incident(config,day%count)
                pending.append({'incident_id':snapshots[0]['incident_id'],'event_id':'OUTAGE-%s-%03d'%(identity,day),
                                'event_type':'comment','event_time':iso(cutoff-timedelta(hours=3)),
                                'source_sequence':30000+day,'actor_id':'USR-0000'})
        for dataset in DATASETS:
            for record in records[dataset]:writer.add(dataset,record)
        batch_id='sequence-'+identity+'-%03d'%day
        manifest=dict(original,batch_id=batch_id,batch_kind='initial' if day==0 else 'delta',parent_batch_id=parent,
            cutoff=iso(cutoff),generator_version='sequence.v2',configuration={'delivery_days':days,'seed':seed,'incident_count':count,
                'scenarios':scenarios,'pending_backfill_records':len(pending),'synthetic_config':config},files=writer.finish())
        write_json(root/'manifest.json',manifest)
        metadata.append({'path':str(root.relative_to(destination)),'batch_id':batch_id,'cutoff':iso(cutoff),'scenarios':scenarios})
        parent=batch_id
    write_json(destination/'sequence.json',{'version':'v2','days':days,'batches':metadata,'initial_history_days':(timestamp(original['cutoff'])-timestamp(original['report_start'])).total_seconds()/86400,
                                        'pending_records_at_end':pending,'semantics':'30/60/90 consecutive delivery days; unchanged v1 business rules'})
    return {'days':days,'batches':len(metadata),'report':str(destination/'sequence.json')}
