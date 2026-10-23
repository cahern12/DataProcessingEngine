"""Spark-side SQL executor. Invoked by spark-submit inside the pinned image."""
import argparse
import json
import os
import sys
import time
import uuid
from datetime import datetime,timezone
from pathlib import Path
sys.path.insert(0,'/app')
from dpe.common import ROOT,canonical,read_json,write_json
from dpe.lakehouse import normalized,sql_revision
from dpe.rules import normalize

MODELS=['stg_snapshots','stg_events','stg_references','eligible_snapshots','eligible_events','incident_headers',
        'event_timeline','quarantine','incident_measures','gold_incident_resolution','incident_daily_contributions',
        'gold_service_daily_metrics','record_dispositions']


def session(event_directory,master='local[4]'):
    from pyspark.sql import SparkSession
    Path(event_directory).mkdir(parents=True,exist_ok=True)
    spark=(SparkSession.builder.appName('dpe-comparative').master(master)
        .config('spark.sql.iceberg.vectorization.enabled','false')
        .config('spark.sql.session.timeZone','UTC').config('spark.sql.ansi.enabled','false')
        .config('spark.sql.catalog.lake','org.apache.iceberg.spark.SparkCatalog')
        .config('spark.sql.catalog.lake.type','rest').config('spark.sql.catalog.lake.uri','http://catalog:19120/iceberg')
        .config('spark.sql.catalog.lake.io-impl','org.apache.iceberg.aws.s3.S3FileIO')
        .config('spark.sql.catalog.lake.s3.endpoint','http://objectstore:9000').config('spark.sql.catalog.lake.s3.path-style-access','true')
        .config('spark.sql.catalog.lake.s3.access-key-id','dpe-local').config('spark.sql.catalog.lake.s3.secret-access-key','local-dpe-only')
        .config('spark.sql.catalog.lake.client.region','us-east-1')
        .config('spark.sql.extensions','org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
        .config('spark.eventLog.logStageExecutorMetrics','true').config('spark.executor.metrics.pollingInterval','1000')
        .config('spark.eventLog.enabled','true').config('spark.eventLog.dir','file://'+str(Path(event_directory).resolve()))
        .getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    spark.udf.register('epoch_seconds',lambda x:x.timestamp() if x else None,'double')
    return spark


def normalized_record(dataset,payload):
    try:return canonical(normalize(dataset,json.loads(payload)))
    except (ValueError,TypeError):return payload


def valid_record(dataset,payload):
    try:normalize(dataset,json.loads(payload));return True
    except (ValueError,TypeError):return False


def setup(spark,schema,cases):
    from pyspark.sql import functions as F
    spark.sql('CREATE NAMESPACE lake.'+schema)
    rows=[]; contexts=[]
    for case in cases:
        contexts.append((case['name'],case['cutoff'],case['report_start']))
        for dataset,records in case.get('datasets',{}).items():
            for ordinal,record in enumerate(records):
                origins=case.get('provenance',{}).get(dataset,[])
                origin=origins[ordinal] if origins else {}
                rows.append((case['name'],dataset,ordinal,origin.get('batch_id',case['name']),origin.get('source_file','fixture'),origin.get('source_row',ordinal+1),canonical(record)))
    raw_schema='case_id string,dataset string,ordinal long,batch_id string,source_file string,source_row long,payload string'
    if any(c.get('raw_path') for c in cases):
        if len(cases)!=1 or rows:raise ValueError('mixed Spark source input modes')
        raw=spark.read.schema(raw_schema).json(cases[0]['raw_path'])
    else:raw=spark.createDataFrame(rows,raw_schema)
    raw.writeTo('lake.'+schema+'.raw_records').using('iceberg').create()
    spark.createDataFrame(contexts,'case_id string,cutoff string,report_start string').writeTo('lake.'+schema+'.batch_context').using('iceberg').create()
    spark.udf.register('normalized_record',normalized_record,'string');spark.udf.register('valid_record',valid_record,'boolean')
    raw=spark.table('lake.'+schema+'.raw_records')
    prepared=raw.withColumn('normalized_payload',F.call_udf('normalized_record','dataset','payload')).withColumn('valid',F.call_udf('valid_record','dataset','payload'))
    prepared.createOrReplaceTempView('prepared_records')
    context=spark.table('lake.'+schema+'.batch_context');context.createOrReplaceTempView('context')
    for kind in ('snapshots','events'):
        snapshot=kind=='snapshots'
        fields=['incident_id','source_version','source_updated_at','created_at','priority','status','service_id','group_id','requester_id'] if snapshot else ['incident_id','event_id','event_time','source_sequence','event_type','actor_id']
        integers={'source_version','source_sequence'}; dates={'source_updated_at','created_at','event_time'}
        expressions=["try_cast(get_json_object(normalized_payload,'$."+f+"') as "+('bigint' if f in integers else 'timestamp' if f in dates else 'string')+') '+f for f in fields]
        base="SELECT r.*,"+','.join(expressions)+",cast(c.cutoff as timestamp) cutoff,cast(c.report_start as timestamp) report_start FROM prepared_records r JOIN context c ON c.case_id=r.case_id WHERE dataset='incident_"+kind+"'"
        clock='source_updated_at' if snapshot else 'event_time';keys='incident_id,source_version' if snapshot else 'event_id'
        signature="to_json(struct("+','.join(fields)+"))"
        query="WITH parsed AS ("+base+"),signed AS (SELECT *,"+signature+" signature FROM parsed) SELECT *,row_number() OVER(PARTITION BY case_id,"+keys+" ORDER BY if(valid AND "+clock+"<=cutoff,0,1),ordinal) identity_rank,first_value(signature) OVER(PARTITION BY case_id,"+keys+" ORDER BY if(valid AND "+clock+"<=cutoff,0,1),ordinal) first_signature FROM signed"
        spark.sql('CREATE TABLE lake.'+schema+'.stg_'+kind+' USING iceberg AS '+query)
    spark.sql('''CREATE TABLE lake.'''+schema+'''.stg_references USING iceberg AS SELECT DISTINCT case_id,dataset,
        coalesce(trim(get_json_object(payload,'$.user_id')),trim(get_json_object(payload,'$.service_id')),
        trim(get_json_object(payload,'$.group_id')),trim(get_json_object(payload,'$.priority'))) reference_id,
        try_cast(get_json_object(payload,'$.response_minutes') as bigint) response_minutes,
        try_cast(get_json_object(payload,'$.resolution_minutes') as bigint) resolution_minutes
        FROM prepared_records WHERE dataset NOT IN ('incident_snapshots','incident_events')''')


def build(spark,schema,fail_at=None):
    from jinja2 import Environment,StrictUndefined
    variables={'business_rules':read_json(ROOT/'rules/business_rules.v1.json'),'targeted_daily':False}
    def utc(value):return 'try_cast('+value+' as timestamp)'
    def source(_,name):return 'lake.'+schema+'.'+name
    env=Environment(undefined=StrictUndefined)
    stats=[]
    for name in MODELS[3:]:
        start=time.perf_counter()
        sql=env.from_string((ROOT/'transformations/spark'/(name+'.sql')).read_text()).render(ref=lambda name:source('',name),source=source,var=lambda key,default=None:variables.get(key,default),utc=utc)
        spark.sql('CREATE TABLE lake.'+schema+'.'+name+' USING iceberg AS '+sql)
        stats.append({'model':name,'runtime_seconds':time.perf_counter()-start})
        if fail_at=='between_gold_writes' and name=='gold_incident_resolution': raise RuntimeError('injected failure: between_gold_writes')
    return stats


def collect(spark,schema,name):
    return [{k:normalized(v) for k,v in row.asDict().items()} for row in spark.table('lake.'+schema+'.'+name).collect()]


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--input',required=True);parser.add_argument('--output',required=True)
    parser.add_argument('--master',default='local[4]');parser.add_argument('--fail-at')
    args=parser.parse_args();spec=read_json(args.input);path=Path(args.output)
    schema=spec.get('schema','dpe_spark_'+uuid.uuid4().hex[:12]);report={'status':'running','schema':schema,'engine':'spark','implementation_revision':sql_revision(),'python_version':sys.version}
    start=time.perf_counter();spark=None
    try:
        spark=session(path.parent/'events',args.master);report.update(engine_version=spark.version,application_id=spark.sparkContext.applicationId,master=spark.sparkContext.master)
        report['java_version']=spark.sparkContext._jvm.java.lang.System.getProperty('java.version')
        report['settings']={'iceberg_vectorization':False,'shuffle_partitions':spark.conf.get('spark.sql.shuffle.partitions'),'time_zone':'UTC','driver_memory':spark.sparkContext.getConf().get('spark.driver.memory')}
        if spec.get('action')=='compat':
            spark.sql('CREATE NAMESPACE lake.'+schema)
            spark.sql('CREATE TABLE lake.'+schema+'.probe(id bigint,value string) USING iceberg')
            spark.sql("INSERT INTO lake."+schema+".probe VALUES(1,'spark')")
            report['rows']=collect(spark,schema,'probe')
            if spec.get('trino_table'):report['trino_rows']=spark.sql('SELECT * FROM '+spec['trino_table']).count()
        elif spec.get('action')=='queries':
            report['queries']={}
            for name,sql in spec['queries'].items():
                query_start=time.perf_counter();query_started=datetime.now(timezone.utc).isoformat()
                from dpe.experiment import watchdog
                with watchdog(spec.get('query_watchdog_seconds')):
                    data=spark.sql(sql)
                    rows=[{k:normalized(v) for k,v in row.asDict().items()} for row in (data.toLocalIterator() if spec.get('bounded_io') else data.collect())]
                report['queries'][name]={'rows':rows,'started_at':query_started,'finished_at':datetime.now(timezone.utc).isoformat(),'runtime_seconds':time.perf_counter()-query_start,'plan':data._jdf.queryExecution().executedPlan().toString()}
        else:
            setup(spark,schema,spec['cases']);report['models']=build(spark,schema,args.fail_at)
            report['outputs']={name:({'rows':spark.table('lake.'+schema+'.gold_'+name).count()} if spec.get('bounded_result') else collect(spark,schema,'gold_'+name)) for name in ('incident_resolution','service_daily_metrics')}
            report['quarantine']=collect(spark,schema,'quarantine')
            if spec.get('bounded_result'):
                report['reconciliation']=[dict(row.asDict()) for row in spark.sql('SELECT dataset,disposition,count(*) rows FROM lake.'+schema+'.record_dispositions GROUP BY 1,2').collect()]
            else:report['reconciliation']=collect(spark,schema,'record_dispositions')
            report['snapshots']={name:{'table':'lake.'+schema+'.gold_'+name,'snapshot_id':spark.sql('SELECT snapshot_id FROM lake.'+schema+'.gold_'+name+'.snapshots ORDER BY committed_at DESC').first()[0]} for name in report['outputs']}
        report['status']='passed'
    except Exception as error:
        report.update(status='failed',error=str(error));raise
    finally:
        report['runtime_seconds']=time.perf_counter()-start;write_json(path,report)
        if spark:spark.stop()

if __name__=='__main__':main()
