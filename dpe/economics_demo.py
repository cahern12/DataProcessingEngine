"""Hand-computable cost reporting demonstration, explicitly synthetic prices."""
from pathlib import Path
from .common import ROOT,read_json,write_json
from .costing import report


def demo(workspace):
    workspace=Path(workspace)
    if workspace.exists():raise ValueError('workspace exists')
    components=['trino','spark','dbt','airflow','catalog','object_storage','container_infrastructure','monitoring','network']
    usage={'version':'v1','currency':'USD','cost_basis':'modeled_cloud','allocation_policy_version':'synthetic-fixture.v1',
        'warning':'Synthetic quantities and prices for arithmetic demonstration only; not measured Azure costs.',
        'required_components':[],
        'runs':[{'logical_run_id':e,'workload_id':'W4','engine':e,'logical_input_bytes':10**12,'scenario':'arithmetic_fixture'} for e in ('trino','spark')],
        'pools':[{'pool_id':'standing','window_id':'fixture-hour','driver':'equal fixture demand, not inferred utilization','weights':{'trino':'0.5','spark':'0.5'}}], 'lines':[]}
    for i,component in enumerate(components):
        line={'line_id':component,'resource_id':'fixture-'+component,'meter_id':'fictional-node-hour','charge_id':component,
            'window_id':'fixture-hour','component':component,'category':'compute','quantity':'1','unit':'node_hour',
            'rate_id':'compute_hour','started_at':'2026-01-01T00:00:00Z','finished_at':'2026-01-01T01:00:00Z',
            'source_ref':'independent arithmetic fixture; no actual resource usage'}
        if component in ('trino','spark'):line.update(classification='marginal',logical_run_id=component)
        else:line.update(classification='standing',pool_id='standing')
        usage['lines'].append(line)
    write_json(workspace/'usage.json',usage)
    result=report(workspace/'usage.json',workspace/'reports',ROOT/'benchmarks/pricing/synthetic-fixture.v1.json')
    # 9 one-hour components x $2 = $18. Each engine $2 + half of $14 shared = $9.
    actual=read_json(workspace/'reports/cost_report.json')
    from decimal import Decimal
    if Decimal(actual['scope_total'])!=18 or any(Decimal(r['fully_allocated_cost'])!=9 for r in actual['workloads']):raise AssertionError('arithmetic fixture totals differ')
    return result
