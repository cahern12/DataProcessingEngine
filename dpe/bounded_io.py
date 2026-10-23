"""Repeatable file-backed source views; payloads and provenance stay in source order."""
import gc
import os
from contextlib import contextmanager
from pathlib import Path
from .common import DATASETS,read_json,safe_file
from .ingestion import iter_file,validate_batch


def enabled():return os.environ.get('DPE_BOUNDED_IO')=='1'


class Records:
    def __init__(self,files):self.files=files
    def __len__(self):return sum(item['rows'] for _,_,item in self.files)
    def __iter__(self):
        for root,header,item in self.files:
            yield from iter_file(safe_file(root,item['path']),item['format'],item['dataset'])


class Origins:
    def __init__(self,files):self.files=files
    def __iter__(self):
        for root,header,item in self.files:
            for n in range(item['rows']):yield {'batch_id':header['batch_id'],'source_file':item['path'],'source_row':n+1}


def sources(paths,verify=True):
    files={k:[] for k in DATASETS};last=None;checked=None
    for path in paths:
        path=Path(path);header=read_json(path/'manifest.json')
        checked=validate_batch(path) if verify else None
        if checked and len(path.name)==64 and path.name!=checked['manifest_sha256']:raise ValueError('archived manifest identity mismatch')
        for item in header['files']:files[item['dataset']].append((path,header,item))
        last=header
    return last,{k:Records(v) for k,v in files.items()},{k:Origins(v) for k,v in files.items()},checked


def release():
    gc.collect()
    try:
        import ctypes
        ctypes.CDLL(None).malloc_trim(0)
    except (AttributeError,OSError):pass


def spool(datasets,provenance,path):
    from .common import canonical,ROOT
    path=Path(path).resolve();path.parent.mkdir(parents=True,exist_ok=True)
    if not path.is_relative_to((ROOT/'data').resolve()):raise ValueError('Spark source spool must be under repository data')
    with path.open('w') as stream:
        for name,records in datasets.items():
            origins=iter(provenance.get(name,[]))
            for ordinal,record in enumerate(records):
                origin=next(origins,{})
                row={'case_id':'__batch__','dataset':name,'ordinal':ordinal,'batch_id':origin.get('batch_id','__batch__'),
                     'source_file':origin.get('source_file','fixture'),'source_row':origin.get('source_row',ordinal+1),'payload':canonical(record)}
                stream.write(canonical(row)+'\n')
    return str(path)


@contextmanager
def profile(criteria):
    selection=criteria.get('source_ingestion',{})
    settings={'DPE_BOUNDED_IO':'1' if selection.get('bounded_io') else '0',
              'DPE_INGEST_MODE':selection.get('mode','values'),
              'DPE_PARQUET_CHUNK_ROWS':str(selection.get('parquet_chunk_rows',50000)),
              'DPE_INSERT_ROWS':str(selection.get('records_per_insert',200)),
              'DPE_INSERT_BYTES':str(selection.get('maximum_insert_bytes',0))}
    old={k:os.environ.get(k) for k in settings};os.environ.update(settings)
    try:yield
    finally:
        for k,v in old.items():
            if v is None:os.environ.pop(k,None)
            else:os.environ[k]=v
