"""Trace local evidence without promoting it into an Azure POC verdict."""
import statistics
from pathlib import Path
from .common import ROOT, read_json, write_json, validate_schema
from .costing import csv_rows


def assessment(actual, limit, reason=None):
    if actual is None or limit is None:
        return {'status': 'not_assessed', 'actual': actual, 'limit': limit,
                'reason': reason or 'required measurement or limit unavailable'}
    return {'status': 'passed' if actual <= limit else 'failed', 'actual': actual, 'limit': limit}


def evaluate_runs(runs, limits):
    result = []
    for run in runs:
        cap = limits['business_limits']['runtime_seconds'].get(run['workload_id'], {}).get(run['size'])
        result.append({'attempt_id': run.get('attempt_id'), 'workload_id': run['workload_id'],
                       'engine': run['engine'], 'size': run['size'],
                       'correctness': 'passed' if run['status']=='passed' and run.get('quality_status')=='passed' else 'failed',
                       'completion': assessment(run.get('runtime_seconds'), cap, 'Smoke/custom tiers do not establish S/M/L runtime gates'),
                       'economics': {'status': 'not_assessed', 'reason': 'local usage is unpriced; separate monetary report required'}})
    return result


def required_cells(specs):
    ext = specs['matrix_extensions']
    for workload in specs['workloads']:
        for size in workload['sizes']:
            for engine in workload['required_engines']:
                variants = ext['W5_faults'] if workload['id']=='W5' else ['initial']
                if workload['id']=='W4':
                    variants += ext['W4_variants']
                for variant in variants:
                    yield dict(workload_id=workload['id'], size=size, engine=engine,
                               workers=1, concurrency=1, variant=variant, axis='baseline')
                if workload['id'] in ext['scale_workloads'] and size in ext['representative_sizes']:
                    for axis, levels in [('workers',ext['worker_levels']),('concurrency',ext['concurrency_levels'])]:
                        for level in levels:
                            cell=dict(workload_id=workload['id'], size=size, engine=engine,
                                      workers=1, concurrency=1, variant='initial', axis=axis)
                            cell[axis]=level
                            yield cell


def scorecard(output, runs=None, findings=None):
    output=Path(output);runs=runs or [];findings=findings or {}
    specs=read_json(ROOT/'benchmarks/specs/workloads.v1.json')
    coverage=read_json(ROOT/'benchmarks/specs/coverage.v1.json')
    limits=read_json(ROOT/'benchmarks/specs/acceptance.poc.v1.json')
    matrix=[]
    for cell in required_cells(specs):
        matches=[r for r in runs if all(r.get(k, 'initial' if k=='variant' else 1 if k in ('workers','concurrency') else None)==v for k,v in cell.items() if k!='axis')]
        # A repeated copy of an attempt is evidence once, not another repetition.
        unique={r.get('attempt_id',str(index)): r for index,r in enumerate(matches)}
        successful=[r for r in unique.values() if r['status']=='passed' and r.get('quality_status')=='passed']
        status='passed' if len(successful)>=limits['engineering_criteria']['repetitions'] else 'failed' if any(r['status']=='failed' for r in matches) else 'not_assessed'
        matrix.append(dict(cell,status=status,successful_repetitions=len(successful),
            reason=None if status=='passed' else 'Required controlled repetitions/variant/topology not executed; Smoke/custom evidence cannot fill this cell.',
            evidence=[r.get('evidence') for r in matches]))
    result={'version':'v1','hypotheses':[r for r in coverage['requirements'] if r['id'].startswith('H')],
            'gates':[r for r in coverage['requirements'] if r['id'].startswith('G')],
            'deliverables':[r for r in coverage['requirements'] if r['id'].startswith('D')],
            'economic_requirements':[r for r in coverage['requirements'] if r['id'].startswith('E')],
            'enterprise_requirements':coverage['enterprise_requirements'],'matrix':matrix,'local_findings':findings,
            'local_run_assessments':evaluate_runs(runs,limits),
            'business_limits':limits['business_limits'],'recommendation':'incomplete','cost_completeness':'incomplete',
            'reason':'Azure execution, scale/recovery matrix and reconciled economics remain open; local result is not a cloud gate verdict.'}
    validate_schema(result,read_json(ROOT/'contracts/benchmark/scorecard.v1.schema.json'))
    write_json(output/'poc_scorecard.json',result);csv_rows(output/'matrix.csv',matrix)
    text=['# POC evidence scorecard','','Recommendation: **incomplete**. Local experiments do not close Azure gates.','','| Requirement | Verdict | Reason |','|---|---|---|']
    text += [f"| {r['id']} — {r['name']} | {r['verdict']} | {r['reason']} |" for r in result['hypotheses']+result['gates']]
    text += ['','## Local findings','']+[f'- {k}: {v}' for k,v in findings.items()]
    text += ['','Every required baseline, variant, fault, worker and concurrency cell is retained in matrix.csv. Runtime/quality assessments are in poc_scorecard.json; monetary gates remain unassessed for unpriced local usage.']
    (output/'poc_scorecard.md').write_text('\n'.join(text)+'\n');return result


def performance(output,runs):
    groups={}
    for run in runs:
        key=(run['workload_id'],run['engine'],run['size'],run.get('strategy','full'),run.get('workers',1),run.get('concurrency',1))
        groups.setdefault(key,[]).append(run)
    rows=[]
    for key,values in sorted(groups.items()):
        good=[r['runtime_seconds'] for r in values if r['status']=='passed']
        rows.append(dict(zip(('workload_id','engine','size','strategy','workers','concurrency'),key),
            repetitions=len(values),successful_repetitions=len(good),median_seconds=statistics.median(good) if good else None,
            min_seconds=min(good) if good else None,max_seconds=max(good) if good else None,
            ranking_status='eligible_local_only' if len(good)>=3 else 'insufficient_repetitions',
            cost_basis='local_measured_usage',marginal_cost=None,fully_allocated_cost=None))
    csv_rows(Path(output)/'cost_performance.csv',rows)
    return rows
