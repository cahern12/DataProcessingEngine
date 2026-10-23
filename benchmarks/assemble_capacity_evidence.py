"""Assemble local capacity evidence, retaining configuration and cost boundaries."""
import argparse
import json
import statistics
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from dpe.common import ROOT,read_json,write_json,digest
from dpe.benchmark_reports import scorecard,performance
from dpe.capacity_reports import summarize
from dpe.cloud_model import campaign
from dpe.costing import csv_rows


def assemble(output,small,custom,representative=None,targeted=None,concurrency=None):
    output=Path(output);output.mkdir(parents=True,exist_ok=True)
    main=read_json(Path(small)/'run_results.json');paired=read_json(Path(custom)/'run_results.json')
    if main['status']!='passed' or paired['status']!='passed':raise ValueError('completed custom and Small comparisons required')
    if main['specification']['revision']!=paired['specification']['revision']:raise ValueError('paired comparisons have different implementations')
    if main['specification']['criteria_sha256']!=paired['specification']['criteria_sha256']:raise ValueError('paired comparisons have different frozen profiles')
    paths=list(dict.fromkeys(['data/capacity-custom-10000','data/capacity-custom-10000-trino4g','data/capacity-small-unmodified',
           'data/capacity-custom-bounded-10000','data/capacity-small-bounded-100000',custom,small]))
    if representative:paths.append(representative)
    if targeted:paths.append(targeted)
    phases=[];cost_rows=[];technology=[];scenarios=[]
    for workspace in paths:
        root=Path(workspace);result=read_json(root/'run_results.json')
        observation=summarize(root)
        phase={'workspace':str(root),'status':result['status'],'error':result.get('error'),'performance_status':result.get('performance_status'),
               'count':result['specification']['count'],'criteria_sha256':result['specification']['criteria_sha256'],
               'revision':result['specification']['revision'],'run_results_sha256':digest(root/'run_results.json'),
               'peak_sampled_producer_rss_bytes':observation['peak_sampled_producer_rss_bytes'],'oom_killed_containers':observation['oom_killed_containers'],
               'resource_evidence':str(root/'observations/resource_summary.json'),'runs':len(result['runs'])}
        phases.append(phase)
        if not (root/'observations/storage_samples.json').exists():
            phase['modeled_cost_status']='incomplete: durable storage inventory unavailable after producer kill';continue
        try:campaign(root,ROOT/'benchmarks/pricing/azure-eastus-2026-10-22',root/'modeled_cloud_final')
        except Exception as error:phase['modeled_cost_status']='incomplete: '+str(error);continue
        for path in sorted((root/'modeled_cloud_final').glob('*/scenario.json')):
            scenario=read_json(path);report=read_json(path.parent/'cost_report.json');run_index={r['logical_run_id']:r for r in result['runs']}
            scenarios.append({'experiment':str(root),'scenario':scenario['scenario'],'standing_monthly_usd':scenario['standing_monthly_usd'],
                              'standing_target_status':scenario['standing_target_status'],'scope_total_usd':scenario['experiment_scope_total_usd'],
                              'model_status':scenario['status'],'measurement_status':scenario['measurement_status'],'evidence':str(path)})
            for row in report['workloads']:
                source=run_index[row['logical_run_id']]
                cost_rows.append(dict(row,experiment=str(root),scenario=scenario['scenario'],runtime_seconds=source['runtime_seconds'],
                    correctness_status=source.get('quality_status'),variant=source.get('variant','initial'),strategy=source.get('strategy','full'),
                    economic_basis='modeled_cloud; measured local elapsed and bytes, explicit deployment/request assumptions',evidence=str(path.parent/'cost_report.json')))
            technology.extend(dict(row,experiment=str(root),scenario=scenario['scenario']) for row in report['technology'])
        phase['modeled_cost_status']='complete_declared_planning_scenarios; cloud measurements incomplete'
    findings={'capacity':'Custom and Small exact paired comparisons completed; see separate performance assessments.',
        'retained_failures':'3-GiB Trino OOM and original Small producer OOM remain failed.',
        'configuration_boundaries':'Only tuned Small repetitions populate this scorecard. Other resources, source distributions, variants and strategies remain separate.',
        'local_costs':'Unpriced measured usage retained. Separately labeled public-retail scenarios include assumptions and are not Azure bills.',
        'phase_summary':str(output/'summary.json'),'modeled_costs':str(output/'modeled_cost_performance.csv')}
    checks=[]
    for workspace in (representative,targeted):
        if not workspace:continue
        root=Path(workspace);entry={'workspace':str(root),'status':read_json(root/'representative.json')['status']}
        if (root/'restart.json').exists():entry['restart']=read_json(root/'restart.json')
        else:entry['restart']={'status':'not_assessed'}
        checks.append(entry)
    findings['delivery_recovery']=checks
    if concurrency:findings['concurrency']=read_json(Path(concurrency)/'concurrency.json')
    write_json(output/'specification.json',main['specification'])
    score=scorecard(output,main['runs'],findings,main['specification']['criteria']);measured=performance(output,main['runs'])
    csv_rows(output/'modeled_cost_performance.csv',cost_rows);csv_rows(output/'technology_costs.csv',technology);csv_rows(output/'scenario_comparison.csv',scenarios)
    summary={'date':'2026-10-22','scope':'local capacity and retail planning; original Azure POC incomplete','recommendation':'incomplete',
        'phases':phases,'primary_small_performance':measured,'primary_small_performance_status':main.get('performance_status'),
        'matrix_cells':len(score['matrix']),'matrix_status_counts':{s:sum(c['coverage_status']==s for c in score['matrix']) for s in ('passed','failed','not_assessed')},
        'matrix_performance_status_counts':{s:sum(c['performance_status']==s for c in score['matrix']) for s in ('passed','failed','not_assessed')},
        'delivery_recovery':checks,'cost_basis':'local_measured_usage + separate modeled_cloud',
        'azure_measured_costs':'unavailable','original_mix_projection':'unavailable: Medium/Large measurements missing',
        'concurrency':findings.get('concurrency'),'small_scenarios':[r for r in scenarios if r['experiment']==str(small)]}
    write_json(output/'summary.json',summary)
    document=['# Capacity campaign results','','Original POC recommendation: **incomplete**. These are local experiments and public-retail planning scenarios.','',
              '| Experiment | Technical status | Performance | Producer sampled peak MiB |','|---|---|---|---:|']
    for row in phases:document.append(f"| {row['workspace']} | {row['status']} | {row['performance_status'] or 'not assessed'} | {round(row['peak_sampled_producer_rss_bytes']/2**20,1) if row['peak_sampled_producer_rss_bytes'] else 'unavailable'} |")
    document+=['','## Tuned Small workloads','','| Workload | Engine | Repetitions | Median seconds | Range seconds |','|---|---|---:|---:|---|']
    for row in measured:
        if row['workload_id'] not in ('W1','W2','W3','W4'):continue
        document.append(f"| {row['workload_id']} | {row['engine']} | {row['successful_repetitions']} | {row['median_seconds']:.2f} | {row['min_seconds']:.2f}–{row['max_seconds']:.2f} |")
    document+=['','## Small public-retail scenarios','','Fixed warm shared capacity; additional cash compute for a run is zero under this assumption. Active compute allocation is already inside standing cost.','',
              '| Scenario | Standing USD/month | Standing target |','|---|---:|---|']
    for row in summary['small_scenarios']:document.append(f"| {row['scenario']} | {float(row['standing_monthly_usd']):.2f} | {row['standing_target_status']} |")
    document+=['','Per-attempt workload costs are in `modeled_cost_performance.csv`; child technology costs are in `technology_costs.csv`. Each scenario directory retains quantity/rate/resource ledgers, provenance, allocation assumptions and 6/24/96-per-day demand sensitivities.',
              '','Local measured usage remains unpriced. ADLS requests, Azure performance/billing and recurring operating labor are unavailable. Retail scenario assumptions do not close their measurement gates. Source configurations are not combined for rankings.',
              '',f"Primary required-cell coverage: {summary['matrix_status_counts']}. Single Small delivery cases and custom observations do not fill unexecuted required repetitions/topologies.",
              '','Read `poc_scorecard.md`, `matrix.csv`, per-phase `observations/resource_summary.json`, and the chronological execution document for limits, failures, restart evidence and remaining work.']
    (output/'capacity_results.md').write_text('\n'.join(document)+'\n')
    return summary


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--out',default='data/capacity-evidence-final')
    parser.add_argument('--small',default='data/capacity-small-bounded-100000');parser.add_argument('--custom',default='data/capacity-custom-bounded-10000')
    parser.add_argument('--representative');parser.add_argument('--targeted');parser.add_argument('--concurrency')
    args=parser.parse_args();print(json.dumps(assemble(args.out,args.small,args.custom,args.representative,args.targeted,args.concurrency),indent=2))
