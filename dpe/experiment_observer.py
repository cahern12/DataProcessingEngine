"""Lifecycle observation; unsupported meters stay explicit and unpriced."""
import json
import os
import resource
import threading
import time
from datetime import datetime,timezone
from pathlib import Path
from urllib.request import Request,urlopen
from .common import ROOT,canonical,write_json


def rss():
    path=Path('/proc/self/status')
    if path.exists():
        fields={line.split(':')[0]:line.split(':',1)[1].strip() for line in path.read_text().splitlines() if ':' in line}
        return int(fields['VmRSS'].split()[0])*1024
    import sys
    value=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value if sys.platform=='darwin' else value*1024


class Observer:
    def __init__(self,workspace,criteria):
        self.workspace=Path(workspace).resolve();self.criteria=criteria;self.folder=self.workspace/'observations'
        self.folder.mkdir(parents=True,exist_ok=True);self.stop_event=threading.Event();self.worker=None;self.active=False
        self.endpoint=os.environ.get('DPE_RESOURCE_ENDPOINT');self.errors=[];self.stage='admission';self.peak=0;self.inventories=[]
    def remote(self,action):
        relative=str(self.folder.relative_to((ROOT/'data').resolve()))
        request=Request(self.endpoint.rstrip('/')+'/'+action,data=canonical({'workspace':relative,'interval_seconds':2}).encode(),headers={'Content-Type':'application/json'})
        with urlopen(request,timeout=45) as stream:return json.load(stream)
    def start(self):
        required=self.criteria.get('observer',{}).get('required',False)
        if required and not self.endpoint:raise ValueError('selected profile requires DPE_RESOURCE_ENDPOINT')
        if self.endpoint:
            info=self.remote('start');self.active=True
            write_json(self.folder/'runtime.json',info)
            if info['NCPU']!=self.criteria['resources']['local_cpus'] or info['MemTotal']<self.criteria['resources']['local_memory_gib']*1024**3*.94:
                raise ValueError('actual runtime does not match selected CPU/memory profile')
            if info.get('vm_memory',{}).get('MemAvailable',0)<self.criteria['resources'].get('vm_headroom_gib',0)*1024**3:raise ValueError('insufficient measured VM headroom for experiment admission')
            required_services=('trino','catalog','postgres','objectstore','airflow','telemetry','spark-gateway','benchmark-coordinator')
            if any(not any(r['name']=='dpe-local-'+name+'-1' and r['state']['Running'] and r['state'].get('Health',{}).get('Status')=='healthy' for r in info['containers']) for name in required_services):raise ValueError('capacity profile requires the complete healthy local stack')
            if any(r['state'].get('OOMKilled') for r in info['containers'] if r['state']['Running']):raise ValueError('runtime has an OOM-killed service')
        self.worker=threading.Thread(target=self.sample,daemon=True);self.worker.start()
    def sample(self):
        with (self.folder/'producer_samples.jsonl').open('w') as stream:
            while not self.stop_event.is_set():
                size=rss();self.peak=max(self.peak,size)
                row={'at':datetime.now(timezone.utc).isoformat(),'stage':self.stage,'rss_bytes':size}
                for key,path in [('cgroup_current_bytes','/sys/fs/cgroup/memory.current'),('cgroup_limit_bytes','/sys/fs/cgroup/memory.max')]:
                    try:row[key]=Path(path).read_text().strip()
                    except OSError:row[key]=None
                try:row['cgroup_memory_events']={k:int(v) for k,v in (line.split() for line in Path('/sys/fs/cgroup/memory.events').read_text().splitlines())}
                except OSError:row['cgroup_memory_events']=None
                stream.write(canonical(row)+'\n');stream.flush();self.stop_event.wait(2)
    def inventory(self):
        begin=time.monotonic()
        try:
            from .lakehouse import object_store
            store=object_store();objects=[];requests=0
            for bucket in ('raw','warehouse'):
                for page in store.get_paginator('list_objects_v2').paginate(Bucket=bucket):
                    requests+=1
                    objects.extend({'bucket':bucket,'key':r['Key'],'bytes':r['Size'],'last_modified':r['LastModified'].isoformat()} for r in page.get('Contents',[]))
            stamp=datetime.now(timezone.utc).isoformat()
            name='objects-%03d.json'%len(self.inventories)
            write_json(self.folder/name,objects)
            self.inventories.append({'at':stamp,'stage':self.stage,'objects':len(objects),'bytes':sum(r['bytes'] for r in objects),
                                     'list_requests':requests,'collector_seconds':time.monotonic()-begin,'evidence':name})
            from .lakehouse import atomic_json
            atomic_json(self.folder/'storage_samples.json',self.inventories)
        except Exception as error:self.errors.append('storage inventory: '+str(error))
    def mark(self,name):
        self.stage=name
        if self.endpoint and (name.startswith('full-') or name in ('shared_preparation','independent_read_expectations')):self.inventory()
        with (self.folder/'stages.jsonl').open('a') as stream:stream.write(canonical({'at':datetime.now(timezone.utc).isoformat(),'stage':name,'rss_bytes':rss()})+'\n')
    def stop(self):
        if self.active:self.inventory()
        write_json(self.folder/'storage_samples.json',self.inventories)
        self.stop_event.set()
        if self.worker:self.worker.join(timeout=5)
        if self.active:
            try:self.remote('stop')
            except Exception as error:self.errors.append(str(error))
        write_json(self.folder/'observer_summary.json',{'peak_sampled_producer_rss_bytes':self.peak,'errors':self.errors,
                   'unsupported':['provider_billed_capacity','process_native_heap_breakdown','Azure_storage_request_counts'],
                   'boundary':'Sampled memory is not exact peak RSS; local quantities do not establish cloud billing'})
