"""Public-retail planning scenarios; deliberately separate from local usage and bills."""
import copy
import hashlib
import statistics
from datetime import datetime,timedelta,timezone
from decimal import Decimal
from pathlib import Path

from .common import ROOT,canonical,read_json,write_json,timestamp,digest
from .costing import calculate,csv_rows,decimal,number,charge
from .experiment import frozen
from .pricing import select

GIB=Decimal(2**30)
MONTH_HOURS=Decimal(720)  # Explicit 30-day planning normalization, not invoice proration.


def catalog(snapshot):
    """Exact product/SKU/meter identities, including every progressive storage tier."""
    snapshot=Path(snapshot);result=[]
    for folder in ('vm','storage','aks','network','nat'):
        metadata=read_json(snapshot/folder/'snapshot.json')
        for page in metadata['pages']:
            if digest(snapshot/folder/page['path'])!=page['sha256']:raise ValueError('price page checksum mismatch')
        records=read_json(snapshot/folder/'records.json')
        if hashlib.sha256(canonical(records).encode()).hexdigest()!=metadata['records_sha256']:raise ValueError('price record checksum mismatch')
    definitions=[
        ('node','vm','Virtual Machines Dasv5 Series','Standard_D4as_v5','D4as v5','hour',1),
        ('disk','storage','Standard SSD Managed Disks','E10 LRS','E10 LRS Disk','month',1),
        ('disk_io','storage','Standard SSD Managed Disks','E10 LRS','E10 LRS Disk Operations','operation',10000),
        ('write','storage','Azure Data Lake Storage Gen2 Hierarchical Namespace','Hot LRS','Hot Write Operations','operation',10000),
        ('read','storage','Azure Data Lake Storage Gen2 Hierarchical Namespace','Hot LRS','Hot Read Operations','operation',10000),
        ('other','storage','Azure Data Lake Storage Gen2 Hierarchical Namespace','Hot LRS','Hot Other Operations','operation',10000),
        ('index','storage','Azure Data Lake Storage Gen2 Hierarchical Namespace','Hot LRS','Hot LRS Index','gib_month',1),
        ('aks','aks','Azure Kubernetes Service','Standard','Standard Uptime SLA','hour',1),
        ('ip','network','IP Addresses','Standard','Standard IPv4 Static Public IP','hour',1),
        ('nat','nat','NAT Gateway','Standard','Standard Gateway','hour',1),
        ('nat_data','nat','NAT Gateway','Standard','Standard Data Processed','gb',1)]
    for key,folder,product,sku,meter,unit,divisor in definitions:
        records=read_json(snapshot/folder/'records.json')
        region='Global' if folder=='nat' else 'eastus'
        row=select(records,productName=product,skuName=sku,meterName=meter,armRegionName=region,type='Consumption',tierMinimumUnits=0)
        expected_unit={'hour':'1 Hour','month':'1/Month','operation':'10K','gib_month':'1 GB/Month','gb':'1 GB'}[unit]
        if row['unitOfMeasure']!=expected_unit:raise ValueError('unexpected provider unit: '+key)
        result.append({'id':key,'unit':unit,'tiers':[{'start':'0','price':number(decimal(row['retailPrice'])/decimal(divisor))}],
                       'provider_rows':[row],'source_ref':str(snapshot/folder/'snapshot.json')})
    records=read_json(snapshot/'storage/records.json')
    rows=[r for r in records if r.get('productName')=='Azure Data Lake Storage Gen2 Hierarchical Namespace' and
          r.get('skuName')=='Hot LRS' and r.get('meterName')=='Hot LRS Data Stored' and r.get('armRegionName')=='eastus' and r.get('type')=='Consumption']
    if len(rows)!=3 or len({r['tierMinimumUnits'] for r in rows})!=3:raise ValueError('unexpected capacity tiers')
    rows.sort(key=lambda r:r['tierMinimumUnits'])
    result.append({'id':'capacity','unit':'gib_month','tiers':[{'start':str(r['tierMinimumUnits']),'price':str(r['retailPrice'])} for r in rows],
                   'provider_rows':rows,'source_ref':str(snapshot/'storage/snapshot.json')})
    retrieved=timestamp(read_json(snapshot/'vm/snapshot.json')['retrieved_at'])
    valuation_end=(retrieved+timedelta(days=2)).replace(hour=0,minute=0,second=0,microsecond=0)
    return {'basis':'modeled_cloud','currency':'USD','effective_from':(retrieved-timedelta(days=1)).replace(hour=0,minute=0,second=0,microsecond=0).isoformat(),'effective_to':valuation_end.isoformat(),
            'rates':result,'scope':'Frozen public retail planning rates; Global NAT meters apply to the East US scenario',
            'binary_capacity_unit_bytes':int(GIB),'accounting_month_hours':str(MONTH_HOURS)}


def weights(items):
    """Conserve exact decimal weights, including rounding remainder."""
    total=sum(items.values(),Decimal(0))
    if not items or total<=0:raise ValueError('positive allocation denominator required')
    result={k:decimal(int(v/total*10**18))/Decimal(10**18) for k,v in items.items()}
    last=next(reversed(result));result[last]+=1-sum(result.values())
    return result


def storage(workspace,runs):
    """Retained objects stay charged, including old histories and failed/orphan files."""
    folder=Path(workspace)/'observations';samples=read_json(folder/'storage_samples.json')
    if len(samples)<2:raise ValueError('two storage inventories required for measured retention baseline')
    before=read_json(folder/samples[0]['evidence']);after=read_json(folder/samples[-1]['evidence'])
    baseline={(r['bucket'],r['key']):r for r in before}
    current={(r['bucket'],r['key']):r for r in after}
    added=[r for k,r in current.items() if k not in baseline]
    assigned={r['logical_run_id']:{'bytes':0,'objects':0} for r in runs};unassigned=[]
    for obj in added:
        when=timestamp(obj['last_modified'])
        matches=[r for r in runs if timestamp(r['started_at'])<=when<=timestamp(r['finished_at'])]
        if len(matches)==1:
            entry=assigned[matches[0]['logical_run_id']];entry['bytes']+=obj['bytes'];entry['objects']+=1
        else:unassigned.append(obj)
    # Unassigned shared setup/collector objects are an explicit experiment overhead pool.
    return {'baseline_bytes':sum(r['bytes'] for r in before),'new_bytes':sum(r['bytes'] for r in added),
            'unassigned_bytes':sum(r['bytes'] for r in unassigned),'unassigned_objects':len(unassigned),
            'assigned':assigned,'final_objects':len(after),'new_objects':len(added),
            'deleted_objects':len(set(baseline)-set(current)),
            'measured_object_byte_hours':sum((decimal(a['bytes'])*(decimal((timestamp(b['at'])-timestamp(a['at'])).total_seconds())/3600)
                                            for a,b in zip(samples,samples[1:])),Decimal(0)),
            'boundary':'Global bucket inventories on isolated serial experiment; attribution by object last-modified interval. Existing retained histories remain standing. No cleanup performed.',
            'evidence':str(folder/'storage_samples.json')}


def model(workspace,snapshot,output,topology='vm',spark_retained=True,variant='base'):
    workspace=Path(workspace);output=Path(output)
    result=read_json(workspace/'run_results.json');criteria=frozen(workspace)
    if not result.get('runs'):raise ValueError('no measured attempts to model')
    if topology not in ('vm','aks'):raise ValueError('unknown topology')
    factors={'low':('0.75','0.8',3,'0.5'),'base':('1','1',7,'1'),'high':('1.5','1.2',14,'2')}
    runtime_factor,rate_factor,retention,request_factor=factors[variant]
    runtime_factor=decimal(runtime_factor);rate_factor=decimal(rate_factor);request_factor=decimal(request_factor)
    rates=catalog(snapshot)
    for rate in rates['rates']:
        for tier in rate['tiers']:tier['price']=number(decimal(tier['price'])*rate_factor)
    runs=[copy.deepcopy(r) for r in result['runs'] if spark_retained or r['engine']!='spark']
    if not runs:raise ValueError('scenario has no eligible measured attempts')
    usage_runs=[{k:r[k] for k in ('logical_run_id','workload_id','engine','size','workers','concurrency','status','logical_input_bytes')} for r in runs]
    for a,r in zip(usage_runs,runs):
        a['attempt_ids']=[r['attempt_id']]
        for key in ('variant','strategy','failure_injection'):
            if key in r:a[key]=r[key]
    start=min(r['started_at'] for r in result['runs'])
    start=min(start,result.get('specification',{}).get('started_at',start))
    last=max(timestamp(r['finished_at']) for r in result['runs'])
    lifecycle_seconds=max((last-timestamp(start)).total_seconds(),result.get('runtime_seconds',0))
    finish=(timestamp(start)+timedelta(seconds=lifecycle_seconds)).isoformat()
    hours=decimal(lifecycle_seconds)/3600*runtime_factor
    if hours<=0:raise ValueError('empty experiment interval')
    driver='equal attempt count' if variant=='low' else 'elapsed seconds × configured engine container ceiling' if variant=='high' else 'measured serial attempt elapsed seconds'
    def allocation_quantity(run):
        if variant=='low':return Decimal(1)
        elapsed=max(decimal(run['runtime_seconds']),Decimal('.000001'))
        if variant=='high':elapsed*=Decimal(4 if run['engine']=='trino' else 3 if run['engine']=='spark' else '1.5')
        return elapsed
    allocation=weights({r['logical_run_id']:allocation_quantity(r) for r in runs})
    window=workspace.name;pool='standing';usage={'version':'v1','cost_basis':'modeled_cloud','currency':'USD',
        'allocation_policy_version':'capacity-retail.v1','runs':usage_runs,'lines':[],
        'pools':[{'pool_id':pool,'window_id':window,'driver':driver+'; includes failed attempts; explicit allocation assumption',
                  'weights':{k:number(v) for k,v in allocation.items()}}],
        'accounting_window_hours':number(hours),'required_components':['trino','dbt','airflow','catalog','object_storage','container_infrastructure','monitoring','network']+(['spark'] if spark_retained else [])}
    assumptions={'version':'v1','cost_basis':'modeled_cloud','topology':topology,'spark_retained':spark_retained,'sensitivity':variant,
        'region':'eastus','currency':'USD','purchase_model':'Linux pay-as-you-go Consumption; no reservations, spot, license or negotiated discounts',
        'window_hours':number(hours),'runtime_factor':number(runtime_factor),'rate_factor':number(rate_factor),'retention_days':retention,
        'window_boundary':'Whole observed benchmark lifecycle, including generation/verification/preparation/collectors and gaps; Airflow queue before producer startup remains outside this diagnostic window',
        'request_factor':number(request_factor),'cache_policy':criteria['cache_policy'],
        'allocation_driver':driver,'sensitivity_interpretation':'Bundled assumption alternatives, not confidence bounds or guaranteed per-row minimum/maximum costs',
        'capacity_equivalence':'D4as_v5 shape is an assumption; local ARM elapsed time is not measured Azure x86 performance',
        'warm_capacity':'Fixed warm nodes. Attributed active compute is diagnostic within standing compute, never added a second time. Marginal compute cash is zero under this inventory.',
        'storage_mapping':'Measured object bytes retained for assumed days; ADLS operations assumed from object counts, not measured S3/Azure billable requests.',
        'storage_operation_mapping':{'writes_per_new_object':2,'reads_per_scanned_object':2,'other_operations_per_attempt':100,'disk_io_per_attempt':10000},
        'index_capacity_fraction':'0.01 assumed, not measured Azure HNS metadata',
        'network':'Same-region storage, no customer-facing load balancer, no cross-zone/region replication or outbound application payload. NAT bootstrap allowance is assumed.',
        'software':'Community/self-hosted licenses assumed zero incremental license fees; no managed Airflow/catalog/monitoring service purchased',
        'excluded':'Taxes, support contracts, engineering labor and production HA/DR. This is a local-POC-equivalent planning inventory, not a production quote.',
        'operations_labor':'Recurring hands-on hours unavailable; economic operations gate remains unassessed',
        'model_measurement_status':'Cloud rates real; capacity, retention, requests, allocation and local-to-cloud runtime mapping are assumptions',
        'actual_azure_cost_status':'unavailable; no Azure execution or bill'}
    def line(component,rate,quantity,resource,run=None,provenance='assumed',reason='',retry=False):
        key=str(len(usage['lines']))
        entry={'line_id':key,'charge_id':key,'resource_id':resource,'meter_id':rate,'window_id':window,'component':component,
               'category':'retail_scenario','classification':'marginal' if run else 'standing','quantity':number(decimal(quantity)),
               'unit':next(r['unit'] for r in rates['rates'] if r['id']==rate),'rate_id':rate,
               'started_at':start,'finished_at':finish,'source_ref':reason or str(workspace/'run_results.json'),
               'quantity_basis':provenance,'retry':retry}
        if run:entry['logical_run_id']=run['logical_run_id']
        else:entry['pool_id']=pool
        usage['lines'].append(entry)
    # One application node, shared by every engine/service. Splits sum to one node.
    trino_share=decimal(criteria['resources'].get('trino_container_gib',3))
    shares={'trino':trino_share,'spark':Decimal(3) if spark_retained else Decimal(0),'dbt':Decimal('1.5'),
            'airflow':Decimal('1.5'),'catalog':Decimal('1.75'),'monitoring':Decimal('.25'),'container_infrastructure':Decimal(8)-trino_share}
    shares=weights({k:v for k,v in shares.items() if v})
    inventory=[{'resource':'application-node','sku':'Standard_D4as_v5','count':1,'shape':'4 vCPU / 16 GiB; all self-hosted application components share it'},
               {'resource':'os-and-catalog-disks','sku':'E10 LRS 128 GiB','count':2,'reason':'one OS and one persistent catalog/log disk'},
               {'resource':'adls','sku':'Hot LRS HNS','count':1,'reason':'same-region raw and Iceberg objects; seven-day base retention'}]
    for component,share in shares.items():line(component,'node',hours*share,'application-node','',reason='one node charge split by assumed capacity shares '+canonical({k:number(v) for k,v in shares.items()}))
    node_count=1;disk_count=2
    if topology=='aks':
        # Current AKS system pools require at least two nodes and >=4 vCPU each.
        inventory.extend([{'resource':'system-pool','sku':'Standard_D4as_v5','count':2,'reason':'separate minimum two-node 4-vCPU system pool; local Kubernetes overhead not measured'},
                          {'resource':'system-pool-os-disks','sku':'E10 LRS 128 GiB','count':2},
                          {'resource':'control-plane','sku':'AKS Standard','count':1},
                          {'resource':'outbound','sku':'Standard NAT + IPv4','count':1}])
        line('container_infrastructure','node',hours*2,'system-pool');node_count+=2;disk_count+=2
        line('container_infrastructure','aks',hours,'aks-control-plane')
        line('network','nat',hours,'nat-gateway');line('network','nat_data',hours*Decimal('.01'),'nat-gateway',reason='assumed 0.01 GB bootstrap/maintenance egress per cluster hour; no measured cloud traffic')
    line('container_infrastructure','disk',hours/MONTH_HOURS*disk_count,'managed-disks')
    line('network','ip',hours,'public-ip')
    layout=storage(workspace,result['runs'])
    standing_bytes=layout['baseline_bytes']
    line('object_storage','capacity',decimal(standing_bytes)/GIB*hours/MONTH_HOURS,'adls-standing',provenance='measured bytes, assumed lifetime',reason=layout['evidence'])
    line('object_storage','capacity',decimal(layout['unassigned_bytes'])/GIB*decimal(retention)/30,'adls-shared-retention',provenance='measured bytes, assumed retention',reason=layout['evidence'])
    # Progressive tiers require one aggregated line per meter/resource/window.
    # Each workload is a hypothetical separate marginal retention increment;
    # all retained capacity below first tier is priced at the scenario's base tier.
    if decimal(standing_bytes+layout['new_bytes'])/GIB>51200:raise ValueError('joint storage tier allocation required above 50 TiB; refuse independent-tier pricing')
    read_objects=max(1,layout['new_objects'])
    for r in runs:
        assigned=layout['assigned'][r['logical_run_id']]
        retained=decimal(assigned['bytes'])/GIB*decimal(retention)/30
        line('object_storage','capacity',retained,'adls-retained-'+r['logical_run_id'],r,'measured bytes, assumed retention',layout['evidence'],r.get('retry',False))
        line('object_storage','index',retained*Decimal('.01'),'adls-index-'+r['logical_run_id'],r,reason='assumed HNS index bytes = 1% retained data bytes')
        line('object_storage','write',decimal(assigned['objects'])*2*request_factor,'adls-writes',r,reason='assumed two ADLS writes per new retained object; no S3-to-ADLS meter equivalence')
        line('object_storage','read',decimal(read_objects)*2*request_factor,'adls-reads',r,reason='assumed two reads per experiment-created object per attempt, including repeated scans')
        line('object_storage','other',100*request_factor,'adls-other',r,reason='assumed 100 list/metadata calls per attempt')
        line('container_infrastructure','disk_io',10000*request_factor,'managed-disks',r,reason='assumed 10,000 OS/catalog disk operations per attempt; not measured IOPS')
    computed=calculate(usage,rates)
    node_rate=next(r for r in rates['rates'] if r['id']=='node')['tiers'][0]['price']
    for row,r in zip(computed['workloads'],runs):
        row['attributed_active_compute_cost']=number(decimal(r['runtime_seconds'])/3600*runtime_factor*decimal(node_rate))
        row['active_compute_boundary']='One application-node elapsed-time allocation, already contained in standing node charge; includes service work, not extra cash'
        row['measurement_completeness']='incomplete: Azure execution, request counts, native memory and recurring operator labor unavailable'
        row['failed_attempt_cost_usd']=row['fully_allocated_cost'] if r['status']=='failed' else '0'
    for row in computed['technology']:
        source=next(r for r in runs if r['logical_run_id']==row['logical_run_id'])
        value=decimal(source['runtime_seconds'])/3600*runtime_factor*decimal(node_rate)*shares.get(row['component'],Decimal(0))
        row['attributed_active_compute_cost']=number(value)
        logical=decimal(source['logical_input_bytes'])/10**12
        row['fully_allocated_cost_per_logical_tb']=number(decimal(row['fully_allocated_cost'])/logical) if logical and row['fully_allocated_cost'] is not None else None
        row['quantity_provenance']='measured local elapsed/object bytes plus explicit scenario assumptions; resource ledger contains per-line basis'
        row.update({k:source[k] for k in ('variant','strategy','failure_injection') if k in source})
    monthly=(decimal(node_rate)*node_count + sum((charge(1,rate) for rate in rates['rates'] if rate['id'] in (['ip','aks','nat'] if topology=='aks' else ['ip'])),Decimal(0)))*MONTH_HOURS
    monthly+=charge(disk_count,next(r for r in rates['rates'] if r['id']=='disk'))
    # Standing storage is the measured existing byte stock; hypothetical future
    # Small-only demand retention is added to each run's marginal quantity.
    monthly+=charge(decimal(standing_bytes)/GIB,next(r for r in rates['rates'] if r['id']=='capacity'))
    if topology=='aks':monthly+=charge(MONTH_HOURS*Decimal('.01'),next(r for r in rates['rates'] if r['id']=='nat_data'))
    demand=[]
    for workload,engine,size in sorted({(r['workload_id'],r['engine'],r['size']) for r in runs}):
        selected=[r for r in computed['workloads'] if (r['workload_id'],r['engine'],r['size'])==(workload,engine,size) and r['status']=='passed']
        if not selected:continue
        marginal=statistics.median([decimal(r['marginal_cost']) for r in selected])
        runtime=statistics.median([decimal(r['runtime_seconds']) for r in runs if (r['workload_id'],r['engine'],r['size'])==(workload,engine,size) and r['status']=='passed'])*runtime_factor
        for frequency in criteria['workload_mix']['sensitivity_runs_per_day']:
            attempts=decimal(frequency)*30;full=marginal+monthly/attempts
            demand.append({'workload_id':workload,'engine':engine,'observed_size':size,'runs_per_day':frequency,
                'scenario':'hypothetical '+size+'-only repeated workload; excludes amortized shared input preparation/read Spark startup unless those are this workload',
                'marginal_per_run_usd':number(marginal),'standing_monthly_usd':number(monthly),'fully_allocated_per_run_usd':number(full),
                'total_monthly_usd':number(monthly+marginal*attempts),'capacity_utilization_fraction':number(runtime*decimal(frequency)/86400),
                'capacity_status':'infeasible serial demand' if runtime*decimal(frequency)>86400 else 'unverified hypothetical capacity',
                'marginal_target_status':'passed' if marginal<=decimal(criteria['business_limits']['marginal_cost_usd'].get(size,'0')) and size in criteria['business_limits']['marginal_cost_usd'] else 'not_assessed' if size not in criteria['business_limits']['marginal_cost_usd'] else 'failed',
                'fully_allocated_target_status':'passed' if full<=decimal(criteria['business_limits']['fully_allocated_cost_usd'].get(size,'0')) and size in criteria['business_limits']['fully_allocated_cost_usd'] else 'not_assessed' if size not in criteria['business_limits']['fully_allocated_cost_usd'] else 'failed'})
            logical_bytes=statistics.median([decimal(r['logical_input_bytes']) for r in selected])
            demand[-1]['fully_allocated_per_logical_tb_usd']=number(full/(logical_bytes/10**12)) if logical_bytes else None
            demand[-1]['unit_cost_target_status']='not_assessed: below minimum logical input gate' if logical_bytes<criteria['business_limits']['unit_cost_gate_minimum_logical_bytes'] else 'passed' if full/(logical_bytes/10**12)<=decimal(criteria['business_limits']['fully_allocated_cost_per_logical_tb_usd']) else 'failed'
            demand[-1]['actual_azure_gate_status']='not_assessed: modeled assumptions and local timings are not cloud execution/billing evidence'
    summary={'status':'complete_declared_planning_scenario','measurement_status':'incomplete','cost_basis':'modeled_cloud',
             'scenario':topology+'-'+('with-spark' if spark_retained else 'trino-only')+'-'+variant,
             'experiment_scope_total_usd':computed['scope_total'],'allocated_total_usd':computed['allocated_total'],
             'standing_monthly_usd':number(monthly),'standing_target_status':'passed' if monthly<=decimal(criteria['business_limits']['standing_monthly_cost_usd']) else 'failed',
             'original_mix_projection':{'status':'unavailable','reason':'Medium/Large measurements missing; retain 90/9/1 mix without extrapolating from Small'},
             'logical_tb_unit_bytes':10**12,'storage_gib_unit_bytes':int(GIB),
             'actual_azure_cost_status':'unavailable','assumptions':assumptions,'inventory':inventory,
             'storage':{k:number(v) if isinstance(v,Decimal) else v for k,v in layout.items()},
             'rate_snapshot_sha256':hashlib.sha256(canonical(rates).encode()).hexdigest()}
    summary['model_source_sha256']=digest(Path(__file__))
    write_json(output/'scenario.json',summary);write_json(output/'usage.json',usage);write_json(output/'rates.json',rates)
    write_json(output/'cost_report.json',computed)
    csv_rows(output/'workload_costs.csv',computed['workloads']);csv_rows(output/'technology_costs.csv',computed['technology'])
    csv_rows(output/'resource_ledger.csv',computed['ledger']);csv_rows(output/'demand_sensitivity.csv',demand)
    return summary


def campaign(workspace,snapshot,output):
    summaries=[]
    for topology in ('vm','aks'):
        for retained in (False,True):
            for variant in ('low','base','high'):
                name=topology+'-'+('with-spark' if retained else 'trino-only')+'-'+variant
                value=model(workspace,snapshot,Path(output)/name,topology,retained,variant)
                summaries.append({k:value[k] for k in ('scenario','standing_monthly_usd','standing_target_status','experiment_scope_total_usd','status','measurement_status')})
    csv_rows(Path(output)/'scenario_comparison.csv',summaries)
    write_json(Path(output)/'summary.json',{'scenarios':summaries,'scope':'Twelve explicit retail planning scenarios; not Azure execution, bill or full POC closure'})
    return summaries


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser();parser.add_argument('--workspace',required=True);parser.add_argument('--snapshot',required=True);parser.add_argument('--output',required=True)
    args=parser.parse_args();campaign(args.workspace,args.snapshot,args.output)
