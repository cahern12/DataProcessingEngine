"""Bounded typed Parquet landing; SQL imports files without large literal ASTs."""
import os
from pathlib import Path
from .common import ROOT,canonical,digest,write_json

RAW_FIELDS=[('case_id','string'),('dataset','string'),('ordinal','int64'),('batch_id','string'),
            ('source_file','string'),('source_row','int64'),('payload','string')]


def write_parts(rows,folder,fields=RAW_FIELDS,chunk_rows=50000):
    import pyarrow as pa
    import pyarrow.parquet as pq
    if chunk_rows<1:raise ValueError('positive Parquet chunk size required')
    folder=Path(folder);folder.mkdir(parents=True,exist_ok=False)
    schema=pa.schema([(name,getattr(pa,kind)()) for name,kind in fields]);names={name for name,_ in fields};buffer=[];files=[]
    def flush():
        path=folder/('part-%05d.parquet'%len(files))
        table=pa.Table.from_pylist(buffer,schema=schema)
        pq.write_table(table,path,compression='snappy',row_group_size=min(10000,chunk_rows),version='2.6')
        check=pq.ParquetFile(path)
        if check.schema_arrow!=schema or check.metadata.num_rows!=len(buffer):raise ValueError('Parquet landing schema/row count mismatch')
        files.append({'path':str(path),'rows':len(buffer),'bytes':path.stat().st_size,'sha256':digest(path)})
    for row in rows:
        if set(row)!=names:raise ValueError('Parquet landing row fields differ from exact schema')
        buffer.append(row)
        if len(buffer)==chunk_rows:flush();buffer=[]
    if buffer:flush()
    write_json(folder/'landing.json',{'fields':fields,'files':files,'rows':sum(f['rows'] for f in files),
                                    'format':'Parquet 2.6 / Snappy; explicit schema verified before registration'})
    return files


def register(client,target,rows,fields=RAW_FIELDS):
    from .lakehouse import object_store,identifier,literal
    schema=identifier(target.split('.')[1]);table=identifier(target.split('.')[2]);prefix='ingestion/'+schema+'/'+table
    folder=ROOT/'data/parquet-landing'/schema/table
    files=write_parts(rows,folder,fields,int(os.environ.get('DPE_PARQUET_CHUNK_ROWS','50000')))
    store=object_store()
    for item in files:
        key=prefix+'/'+Path(item['path']).name
        store.upload_file(item['path'],'raw',key,ExtraArgs={'Metadata':{'sha256':item['sha256']}})
        item['object_location']='s3://raw/'+key
    if files:client.sql('ALTER TABLE '+target+' EXECUTE add_files(location => '+literal('s3://raw/'+prefix)+",format => 'PARQUET')")
    evidence={'mode':'parquet_add_files','files':files,'rows':sum(f['rows'] for f in files),'target':target,
              'boundary':'Imported immutable files; original raw source archives and provenance preserved; no SQL business-rule change'}
    client.ingestion=evidence;write_json(folder/'registration.json',evidence)
    return evidence


def raw_rows(cases):
    for case in cases:
        for name,records in case['datasets'].items():
            origins=iter(case.get('provenance',{}).get(name,[]))
            for ordinal,record in enumerate(records):
                origin=next(origins,{})
                yield {'case_id':case['name'],'dataset':name,'ordinal':ordinal,'batch_id':origin.get('batch_id',case['name']),
                       'source_file':origin.get('source_file','fixture'),'source_row':origin.get('source_row',ordinal+1),'payload':canonical(record)}
