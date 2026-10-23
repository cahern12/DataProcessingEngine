"""Capacity preflight preserves tier identity; it does not claim engine execution."""
import time
from pathlib import Path
from .common import ROOT, digest, read_json, write_json
from .synthetic import generate


def assess(manifest, criteria):
    logical_bytes = sum(item['bytes'] for item in manifest['files'])
    budget = criteria['resources']['maximum_local_logical_input_bytes']
    return {'logical_input_bytes': logical_bytes, 'budget_bytes': budget,
            'processing_status': 'eligible' if logical_bytes <= budget else 'resource_limited',
            'reason': 'Conservative raw-input budget for full-history Python validation and concurrent local services; no engine capacity claim.'}


def calibrate(workspace, count=100000):
    workspace = Path(workspace)
    if workspace.exists():
        raise ValueError('calibration requires a fresh workspace')
    workspace.mkdir(parents=True)
    criteria = read_json(ROOT/'benchmarks/specs/acceptance.poc.v1.json')
    write_json(workspace/'frozen_criteria.json', criteria)
    started = time.perf_counter()
    generate(workspace/'source', count=count)
    manifest = workspace/'source/initial/manifest.json'
    header = read_json(manifest)
    result = dict(assess(header, criteria), count=count,
                  manifest_sha256=digest(manifest), generation_seconds=time.perf_counter()-started,
                  files=len(header['files']), rows_by_dataset={name: sum(f['rows'] for f in header['files'] if f['dataset']==name) for name in {f['dataset'] for f in header['files']}},
                  evidence=str(manifest), engine_execution='not_run',
                  compressed_bytes=None, compression_reason='source calibration only; no Iceberg write performed')
    write_json(workspace/'calibration.json', result)
    return result
