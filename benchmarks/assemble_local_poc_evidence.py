"""Assemble retained local phases without promoting them to original Azure gates."""
import argparse
import json
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from dpe.common import read_json,write_json,digest
from dpe.benchmark_reports import scorecard,performance


def assemble(output,verification,comparisons,calibration,restart=None,failed_comparisons=(),concurrency=None):
    output=Path(output);runs=[];phases=[]
    checks=read_json(Path(verification)/'verification.json')
    if checks['status']!='passed':raise ValueError('acceptance failed')
    for workspace in comparisons:
        path=Path(workspace)/'run_results.json';result=read_json(path)
        if result['status']!='passed':raise ValueError('comparison failed: '+str(path))
        runs.extend(result['runs'])
        phases.append({'path':str(path),'sha256':digest(path),'revision':result['specification']['revision'],
                       'criteria_sha256':result['specification']['criteria_sha256'],'count':result['specification']['count'],
                       'repetitions':result['specification']['repeats'],'orchestration':result['specification']['orchestration']})
    capacity=read_json(Path(calibration)/'calibration.json')
    findings={'fixtures':checks['spark_fixtures']['fixture_count'],
              'publication_failures':'both failpoints, retry, replay and old pins passed for both engines',
              'capacity_preflight':capacity,'comparisons':phases,
              'economics':'local measured usage is unpriced; synthetic arithmetic is separate',
              'original_poc':'Azure, required sizes/topologies/faults and reconciled billing remain incomplete'}
    failed=[]
    for workspace in failed_comparisons:
        path=Path(workspace)/'run_results.json';result=read_json(path)
        if result['status']!='failed':raise ValueError('expected retained failed comparison')
        failed.append({'path':str(path),'sha256':digest(path),'status':'failed','error':result['error'],
                       'count':result['specification']['count'],'revision':result['specification']['revision'],
                       'attempts':len(result['runs']),'ranking_status':'ineligible: incomplete comparison'})
    findings['failed_comparisons']=failed
    concurrency_result=read_json(Path(concurrency)/'concurrency.json') if concurrency else None
    if concurrency_result:
        if concurrency_result['status']!='passed':raise ValueError('concurrency diagnostic failed')
        findings['exploratory_concurrency']={'path':str(Path(concurrency)/'concurrency.json'),'summary':concurrency_result['summary'],
                                           'gate_status':concurrency_result['gate_status']}
    if restart:
        findings['restart']=read_json(Path(restart)/'restart.json')
        if findings['restart']['status']!='passed':raise ValueError('restart check failed')
    result=scorecard(output,runs,findings);performance(output,runs)
    if capacity['count']==100000 and capacity['processing_status']=='resource_limited':
        for cell in result['matrix']:
            if cell['size']=='small' and cell['successful_repetitions']==0:
                cell.update(execution_status='resource_limited',reason='Initial input exceeds frozen local budget; preflight only, no engine execution.',capacity_evidence=str(Path(calibration)/'calibration.json'))
    write_json(output/'poc_scorecard.json',result)
    from dpe.costing import csv_rows
    csv_rows(output/'matrix.csv',result['matrix'])
    summary={'scope':'local only; original Azure POC incomplete','recommendation':result['recommendation'],
             'acceptance':{'path':str(Path(verification)/'verification.json'),'sha256':digest(Path(verification)/'verification.json'),'revision':checks['revision'],'status':checks['status']},
             'comparisons':phases,'failed_comparisons':failed,'concurrency':findings.get('exploratory_concurrency'),'capacity':capacity,'matrix_cells':len(result['matrix']),
             'matrix_status_counts':{s:sum(c['status']==s for c in result['matrix']) for s in ('passed','failed','not_assessed')},
             'restart':findings.get('restart'),'performance':performance(output,runs),'cost_basis':'local_measured_usage','monetary_costs':'unavailable'}
    write_json(output/'summary.json',summary)
    return summary

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--out',required=True);parser.add_argument('--verification',required=True)
    parser.add_argument('--comparison',action='append',required=True);parser.add_argument('--calibration',required=True);parser.add_argument('--restart')
    parser.add_argument('--failed-comparison',action='append',default=[]);parser.add_argument('--concurrency')
    args=parser.parse_args();print(json.dumps(assemble(args.out,args.verification,args.comparison,args.calibration,args.restart,args.failed_comparison,args.concurrency),indent=2))
