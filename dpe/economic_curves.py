"""Demand scenarios preserve missing tier prices and standing accounting windows."""
import statistics
from decimal import Decimal
from .costing import decimal, number


def demand_sensitivity(report, usage, criteria):
    mix=criteria['workload_mix'];days=decimal(mix['accounting_days'])
    weights={k:decimal(v) for k,v in mix['size_fractions'].items()}
    if sum(weights.values())!=1 or any(v<0 for v in weights.values()):
        raise ValueError('invalid workload mix')
    standing=[r for r in report['ledger'] if r['classification']=='standing']
    hours=usage.get('accounting_window_hours')
    monthly_standing=None
    if standing and hours is not None and decimal(hours)>0 and all(r['amount'] is not None for r in standing):
        monthly_standing=sum((decimal(r['amount']) for r in standing),Decimal(0))*days*24/decimal(hours)
    groups=sorted({(r.get('workload_id','unknown'),r.get('engine','unknown')) for r in report['workloads']})
    result=[]
    for workload,engine in groups:
        averages={}
        for size in weights:
            values=[decimal(r['marginal_cost']) for r in report['workloads'] if r.get('workload_id')==workload and r.get('engine')==engine and r.get('size')==size and r['marginal_cost'] is not None and r['cost_completeness']=='complete']
            averages[size]=statistics.median(values) if values else None
        weighted=None if any(v is None for v in averages.values()) else sum((weights[k]*averages[k] for k in weights),Decimal(0))
        for frequency in mix['sensitivity_runs_per_day']:
            count=days*decimal(frequency)
            marginal=weighted*count if weighted is not None else None
            full=marginal+monthly_standing if marginal is not None and monthly_standing is not None else None
            result.append({'workload_id':workload,'engine':engine,'cost_basis':report['cost_basis'],
                           'currency':report['currency'],'runs_per_day':frequency,'accounting_days':number(days),
                           'marginal_monthly_cost':number(marginal),'standing_monthly_cost':number(monthly_standing),
                           'total_monthly_cost':number(full),'fully_allocated_per_run':number(full/count) if full is not None else None,
                           'status':'complete_scenario' if full is not None else 'incomplete',
                           'assumption':'Alternative workload demand on unchanged platform capacity; no autoscaling or cache forecast.',
                           'reason':None if full is not None else 'Complete S/M/L marginal costs and a priced standing accounting window are required'})
    return result
