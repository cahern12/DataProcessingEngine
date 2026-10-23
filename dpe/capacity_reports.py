"""Summarize sampled resources without presenting samples as exact peaks or bills."""
import json
import re
import statistics
from collections import defaultdict
from pathlib import Path
from .common import read_json,write_json,timestamp
from .costing import csv_rows


def byte_size(value):
    match=re.fullmatch(r'([0-9.]+)([A-Za-z]+)',value.strip())
    if not match:raise ValueError('unknown sampled memory unit: '+value)
    units={'B':1,'kB':1000,'KB':1000,'MB':1000**2,'GB':1000**3,'KiB':1024,'MiB':1024**2,'GiB':1024**3}
    return int(float(match[1])*units[match[2]])


def lines(path):return [json.loads(v) for v in Path(path).read_text().splitlines() if v.strip()] if Path(path).exists() else []


def summarize(workspace):
    workspace=Path(workspace);folder=workspace/'observations';stages=lines(folder/'stages.jsonl')
    samples=lines(folder/'container_samples.jsonl');producer=lines(folder/'producer_samples.jsonl')
    aggregate={};stage_rows=defaultdict(list);gaps=[];overhead=0;errors=[]
    def stage(at):
        eligible=[r['stage'] for r in stages if timestamp(r['at'])<=at]
        return eligible[-1] if eligible else 'admission'
    for index,sample in enumerate(samples):
        at=timestamp(sample['at']);name=stage(at)
        if index:gaps.append((at-timestamp(samples[index-1]['at'])).total_seconds())
        overhead+=sample.get('collector_seconds',0)
        if sample.get('status')!='captured':errors.append(sample);continue
        duration=(timestamp(samples[index+1]['at'])-at).total_seconds() if index+1<len(samples) else 0
        for row in sample.get('containers',[]):
            key=row['Name'];used,limit=map(byte_size,row['MemUsage'].split('/'));cpu=float(row['CPUPerc'].strip('%'))
            target=aggregate.setdefault(key,{'container':key,'peak_sampled_memory_bytes':0,'memory_limit_bytes':limit,'maximum_sampled_cpu_percent':0,'sampled_cpu_seconds':0,'samples':0})
            target['peak_sampled_memory_bytes']=max(target['peak_sampled_memory_bytes'],used)
            target['maximum_sampled_cpu_percent']=max(target['maximum_sampled_cpu_percent'],cpu)
            target['sampled_cpu_seconds']+=cpu/100*duration;target['samples']+=1
            stage_rows[(name,key)].append(used)
    before=read_json(folder/'runtime_before.json') if (folder/'runtime_before.json').exists() else {}
    after=read_json(folder/'runtime_after.json') if (folder/'runtime_after.json').exists() else {}
    oom=[r['name'] for r in after.get('containers',[]) if r['state'].get('OOMKilled')]
    inventories=read_json(folder/'storage_samples.json') if (folder/'storage_samples.json').exists() else []
    protected=[];committed=set();failed=set()
    for path in workspace.rglob('publications/*.json'):
        publication=read_json(path)
        for name,pin in publication['snapshots'].items():
            namespace=pin['table'].split('.')[1];committed.add(namespace)
            protected.append({'publication_id':publication['publication_id'],'product':name,**pin,'evidence':str(path)})
    for path in workspace.rglob('attempts/*.json'):
        attempt=read_json(path)
        if attempt.get('status')=='failed' and attempt.get('schema'):failed.add(attempt['schema'])
    groups=defaultdict(list)
    if inventories:
        for obj in read_json(folder/inventories[-1]['evidence']):
            parts=obj['key'].split('/');namespace=parts[0]
            category='raw' if obj['bucket']=='raw' else 'failed_attempt_namespace' if namespace in failed else 'committed_namespace' if namespace in committed else 'other_retained_namespace'
            kind='parquet' if obj['key'].endswith('.parquet') else 'metadata_or_raw'
            groups[(category,kind)].append(obj['bytes'])
    layout=[]
    for (category,kind),values in groups.items():
        ordered=sorted(values)
        layout.append({'category':category,'object_kind':kind,'objects':len(values),'bytes':sum(values),'minimum_bytes':min(values),
                       'median_bytes':statistics.median(values),'p95_bytes':ordered[min(len(ordered)-1,int(len(ordered)*.95))],'maximum_bytes':max(values)})
    result={'status':'observed' if samples and producer else 'incomplete','sample_count':len(samples),
        'containers':list(aggregate.values()),'oom_killed_containers':oom,'peak_sampled_producer_rss_bytes':max((r['rss_bytes'] for r in producer),default=None),
        'collector_wall_seconds_sum':overhead,'sampling_gap_seconds':{'median':statistics.median(gaps) if gaps else None,'maximum':max(gaps,default=None)},
        'runtime_before':before,'runtime_after':after,'storage_layout':layout,'protected_pins':protected,
        'storage_byte_hours_left_sample':sum(a['bytes']*(timestamp(b['at'])-timestamp(a['at'])).total_seconds()/3600 for a,b in zip(inventories,inventories[1:])),
        'errors':errors,'limits':['Sample maxima are not exact peaks; Docker memory values rounded by Docker stats.',
             'CPU seconds integrate sampled container CPU%; distinct from measured engine task/query CPU.',
             'Storage byte-hours use left-sample interpolation; retention is separately assumed in cloud models.',
             'Namespace classification does not prove all objects are referenced by Iceberg; no garbage collection authorized or performed.',
             'Per-process native/JVM heap breakdown, billable ADLS request counts, transfer meters and recurring hands-on labor unavailable.']}
    write_json(folder/'resource_summary.json',result)
    csv_rows(folder/'container_resources.csv',list(aggregate.values()));csv_rows(folder/'storage_layout.csv',layout)
    csv_rows(folder/'stage_memory.csv',[{'stage':stage,'container':container,'peak_sampled_memory_bytes':max(values),'samples':len(values)} for (stage,container),values in stage_rows.items()])
    return result


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser();parser.add_argument('--workspace',required=True)
    summarize(parser.parse_args().workspace)
