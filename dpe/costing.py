"""Decimal cost ledger with conserved, explicit shared/idle allocations.

Usage is imported; local CPU time is never silently converted to a cloud bill.
"""
import csv
import hashlib
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path

from .common import ROOT, canonical, digest, read_json, timestamp, write_json, validate_schema

COMPONENTS = ('trino','spark','dbt','airflow','catalog','object_storage','container_infrastructure','monitoring','network','reference')
BASES = ('local_measured_usage','modeled_cloud','azure_provisional','azure_reconciled')


def decimal(value):
    if isinstance(value,bool): raise ValueError('boolean is not a quantity')
    try: result=Decimal(str(value))
    except InvalidOperation as error: raise ValueError('invalid decimal') from error
    if not result.is_finite(): raise ValueError('non-finite monetary quantity')
    return result


def number(value):
    return format(value,'f') if value is not None else None


def charge(quantity, rate):
    quantity=decimal(quantity)
    if quantity<0: raise ValueError('negative usage')
    tiers=rate['tiers']
    starts=[decimal(t['start']) for t in tiers]
    if not starts or starts[0]!=0 or any(b<=a for a,b in zip(starts,starts[1:])): raise ValueError('invalid rate tiers')
    prices=[decimal(t['price']) for t in tiers]
    if any(p<0 for p in prices): raise ValueError('negative rate')
    return sum((max(Decimal(0),min(quantity,starts[i+1] if i+1<len(starts) else quantity)-start)*prices[i]
                for i,start in enumerate(starts)),Decimal(0))


def calculate(usage,rates=None):
    # Usage quantities and exact 18-place pool weights can exceed the default
    # 28-digit context when multiplied. Preserve finite monetary products and
    # sums before comparing the independently grouped ledger/allocation totals.
    with localcontext() as context:
        context.prec=128
        return _calculate(usage,rates)


def _calculate(usage, rates=None):
    """One charge identity, one component; pools allocate exactly once per window."""
    basis=usage['cost_basis']
    if basis not in BASES: raise ValueError('unsupported cost basis')
    if rates and (rates['currency']!=usage['currency'] or rates['basis']!=basis): raise ValueError('price currency/basis mismatch')
    if basis=='local_measured_usage' and rates: raise ValueError('local measured usage requires a separately labeled modeled scenario')
    if set(usage.get('required_components',[]))-set(COMPONENTS): raise ValueError('unknown required component')
    runs={r['logical_run_id']:r for r in usage['runs']}
    if len(runs)!=len(usage['runs']): raise ValueError('duplicate logical run')
    rates_by_id={r['id']:r for r in rates['rates']} if rates else {}
    if rates and len(rates_by_id)!=len(rates['rates']): raise ValueError('duplicate rate identity')
    pools={p['pool_id']:p for p in usage.get('pools',[])}
    if len(pools)!=len(usage.get('pools',[])): raise ValueError('duplicate pool')
    seen=set(); tiered_windows=set(); ledger=[]; allocations=[]; unallocated=[]
    for raw in usage['lines']:
        line=dict(raw)
        for key in ('quantity','unit','rate_id'):line.setdefault(key,None)
        validate_schema(line,read_json(ROOT/'contracts/benchmark/cost_line.v1.schema.json'))
        if timestamp(line['finished_at'])<timestamp(line['started_at']):raise ValueError('negative interval')
        if line['quantity'] is not None and decimal(line['quantity'])<0:raise ValueError('negative usage')
        identity=line['line_id']
        billing_identity=(line['resource_id'],line['meter_id'],line['window_id'],line['charge_id'])
        if identity in seen or billing_identity in seen: raise ValueError('duplicate underlying charge')
        seen.update([identity,billing_identity])
        if line['component'] not in COMPONENTS: raise ValueError('unknown component')
        if line['classification'] not in ('marginal','standing'): raise ValueError('invalid cost classification')
        if not line.get('source_ref'): raise ValueError('usage provenance required')
        amount=None; reason=None
        if line.get('billed_amount') is not None:
            if basis not in ('azure_provisional','azure_reconciled'): raise ValueError('billed amount requires Azure provenance')
            if line.get('billing_basis') not in ('actual','amortized'): raise ValueError('billing basis required')
            amount=decimal(line['billed_amount'])
            if amount<0 and line.get('charge_type')!='credit': raise ValueError('negative charge must be explicit credit')
        else:
            rate=rates_by_id.get(line.get('rate_id'))
            if not rate: reason='rate unavailable'
            elif line.get('quantity') is None: reason='quantity unavailable'
            else:
                if rate['unit']!=line['unit']: raise ValueError('quantity/rate unit mismatch; convert explicitly')
                if len(rate['tiers'])>1:
                    tier_window=(line['resource_id'],line['meter_id'],line['window_id'],rate['id'])
                    if tier_window in tiered_windows:raise ValueError('tiered usage must be aggregated per resource/meter/window, then allocated')
                    tiered_windows.add(tier_window)
                when=timestamp(line['started_at'])
                if when<timestamp(rates['effective_from']) or timestamp(line['finished_at'])>timestamp(rates['effective_to']):
                    raise ValueError('usage outside rate effective interval')
                if timestamp(line['finished_at'])<when: raise ValueError('negative interval')
                amount=charge(line['quantity'],rate)
        line.update(amount=number(amount),unavailable_reason=reason or line.get('unavailable_reason'))
        ledger.append(line)
        if line['classification']=='marginal':
            if line.get('logical_run_id') not in runs: raise ValueError('unknown workload on marginal line')
            allocations.append(dict(line,allocated_amount=number(amount),allocation_kind='marginal'))
        else:
            pool=pools.get(line.get('pool_id'))
            if not pool: raise ValueError('standing pool missing')
            if pool['window_id']!=line['window_id'] or not pool.get('driver'): raise ValueError('pool window/driver missing')
            weights={key:decimal(v) for key,v in pool['weights'].items()}
            if set(weights)-set(runs) or any(v<0 for v in weights.values()): raise ValueError('invalid pool weights')
            if weights and sum(weights.values())!=1: raise ValueError('fully allocated pool weights must sum to one')
            if not weights:
                unallocated.append(dict(line,reason='idle-only pool has no eligible workload')); continue
            # Last share receives exact decimal remainder; never lose currency in division.
            remainder=amount
            for index,(key,weight) in enumerate(weights.items()):
                share=None if amount is None else remainder if index==len(weights)-1 else amount*weight
                if amount is not None: remainder-=share
                allocations.append(dict(line,logical_run_id=key,allocated_amount=number(share),allocation_kind='standing',allocation_driver=pool['driver'],weight=number(weight)))
    if len({r['billing_basis'] for r in ledger if r.get('billed_amount') is not None})>1:raise ValueError('mixed billing bases')
    rows=[]; technology=[]
    def total(items):
        values=[item['allocated_amount'] for item in items]
        return None if any(v is None for v in values) else sum((decimal(v) for v in values),Decimal(0))
    for key,run in runs.items():
        selected=[r for r in allocations if r['logical_run_id']==key]
        marginal=total([r for r in selected if r['allocation_kind']=='marginal'])
        standing=total([r for r in selected if r['allocation_kind']=='standing'])
        complete=bool(selected) and all(r['allocated_amount'] is not None for r in selected)
        full=None if not complete else marginal+standing
        logical=decimal(run['logical_input_bytes'])/Decimal(10**12)
        if logical<0: raise ValueError('negative logical input')
        active=total([r for r in selected if r.get('active_capacity')])
        retries=total([r for r in selected if r.get('retry')])
        row=dict(run,currency=usage['currency'],cost_basis=basis,marginal_cost=number(marginal) if selected else None,
            attributed_active_compute_cost=number(active) if selected else None,
            allocated_platform_cost=number(standing) if selected else None,fully_allocated_cost=number(full),
            marginal_cost_per_logical_tb=number(marginal/logical) if logical and marginal is not None and selected else None,
            fully_allocated_cost_per_logical_tb=number(full/logical) if logical and full is not None else None,
            unit_cost_reason=None if logical else 'not_applicable: zero logical input',retry_cost=number(retries) if selected else None,
            cost_completeness='complete' if complete else 'incomplete',allocation_policy_version=usage['allocation_policy_version'])
        rows.append(row)
        for component in COMPONENTS:
            items=[r for r in selected if r['component']==component]
            technology.append({'logical_run_id':key,'workload_id':run.get('workload_id'),'engine':run.get('engine'),'component':component,'currency':usage['currency'],'cost_basis':basis,
                'marginal_cost':number(total([r for r in items if r['allocation_kind']=='marginal'])) if items else None,
                'allocated_platform_cost':number(total([r for r in items if r['allocation_kind']=='standing'])) if items else None,
                'fully_allocated_cost':number(total(items)) if items else None,
                'status':'not_applicable' if not items and component not in usage.get('required_components',[]) else 'incomplete' if not items or total(items) is None else 'complete'})
    for row in rows:
        if any(r['status']=='incomplete' for r in technology if r['logical_run_id']==row['logical_run_id']):
            row.update(cost_completeness='incomplete',fully_allocated_cost=None,fully_allocated_cost_per_logical_tb=None)
    ledger_complete=all(r['amount'] is not None for r in ledger)
    scope_total=sum((decimal(r['amount']) for r in ledger),Decimal(0)) if ledger_complete else None
    allocated=sum((decimal(r['allocated_amount']) for r in allocations),Decimal(0)) if ledger_complete else None
    remainder=sum((decimal(r['amount']) for r in unallocated),Decimal(0)) if ledger_complete else None
    if scope_total is not None and allocated+remainder!=scope_total: raise AssertionError('allocation did not conserve ledger')
    missing_components={r['component'] for r in technology if r['status']=='incomplete'}
    report={'version':'v1','cost_basis':basis,'currency':usage['currency'],'scope_total':number(scope_total),
        'allocated_total':number(allocated),'idle_only_total':number(remainder),
        'cost_completeness':'complete' if ledger and ledger_complete and not unallocated and not missing_components else 'incomplete',
        'ledger':ledger,'allocations':allocations,'workloads':rows,'technology':technology,'unallocated':unallocated,
        'rate_snapshot_sha256':hashlib.sha256(canonical(rates).encode()).hexdigest() if rates else None,
        'usage_sha256':hashlib.sha256(canonical(usage).encode()).hexdigest(),
        'reconciliation':{'status':'not_assessed','reason':'no settled billing scope supplied'}}
    billing=usage.get('billing_scope')
    if billing:
        if billing['currency']!=usage['currency'] or billing['basis'] not in ('actual','amortized'): raise ValueError('billing scope mismatch')
        if not billing.get('source_ref'): raise ValueError('billing scope provenance missing')
        if any(r.get('billed_amount') is not None and r['billing_basis']!=billing['basis'] for r in ledger): raise ValueError('mixed billing bases')
        expected=decimal(billing['total']); tolerance=decimal(billing.get('tolerance','0.02'))
        if tolerance<0 or tolerance>1: raise ValueError('invalid reconciliation tolerance')
        difference=abs(scope_total-expected) if scope_total is not None else None
        passed=difference is not None and (difference==0 if expected==0 else difference/abs(expected)<=tolerance)
        report['reconciliation']={'status':'passed' if passed else 'failed','scope':billing,'ledger_total':number(scope_total),'absolute_residual':number(difference)}
    if basis=='azure_reconciled' and report['reconciliation']['status']!='passed': raise ValueError('reconciled label requires successful billing reconciliation')
    return report


def csv_rows(path,rows):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    fields=list(dict.fromkeys(k for row in rows for k in row))
    with path.open('w',newline='') as stream:
        writer=csv.DictWriter(stream,fields);writer.writeheader()
        writer.writerows({k:canonical(v) if isinstance(v,(dict,list)) else v for k,v in row.items()} for row in rows)


def report(usage_path,output,rates_path=None):
    usage=read_json(usage_path);rates=read_json(rates_path) if rates_path else None
    result=calculate(usage,rates);output=Path(output)
    result['usage_file_sha256']=digest(usage_path)
    write_json(output/'usage_snapshot.json',usage)
    if rates:write_json(output/'rate_snapshot.json',rates)
    write_json(output/'cost_report.json',result)
    (output/'cost_ledger.jsonl').write_text(''.join(canonical(line)+'\n' for line in result['ledger']))
    csv_rows(output/'workload_costs.csv',result['workloads']);csv_rows(output/'technology_costs.csv',result['technology'])
    csv_rows(output/'platform_costs.csv',[r for r in result['ledger'] if r['classification']=='standing'])
    write_json(output/'billing_reconciliation.json',result['reconciliation'])
    from .economic_curves import demand_sensitivity
    csv_rows(output/'demand_sensitivity.csv',demand_sensitivity(result,usage,read_json(ROOT/'benchmarks/specs/acceptance.poc.v1.json')))
    text=['# Workload economics','',f"Cost basis: `{result['cost_basis']}`. Currency: {result['currency']}. Completeness: {result['cost_completeness']}.",'','| Workload | Engine | Marginal | Allocated | Fully allocated |','|---|---|---:|---:|---:|']
    text += [f"| {r.get('workload_id',r['logical_run_id'])} | {r.get('engine','unknown')} | {r['marginal_cost'] if r['marginal_cost'] is not None else 'unavailable'} | {r['allocated_platform_cost'] if r['allocated_platform_cost'] is not None else 'unavailable'} | {r['fully_allocated_cost'] if r['fully_allocated_cost'] is not None else 'unavailable'} |" for r in result['workloads']]
    text += ['','Component costs are in technology_costs.csv. Modeled values are scenarios, not Azure bills. Unknown costs remain unavailable.']
    (output/'economics.md').write_text('\n'.join(text)+'\n')
    return {'cost_basis':result['cost_basis'],'cost_completeness':result['cost_completeness'],'scope_total':result['scope_total'],'report':str(output/'cost_report.json')}
