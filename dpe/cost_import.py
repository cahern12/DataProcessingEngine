"""Explicitly mapped cost-detail CSV import; never infer per-query billing."""
import csv
import hashlib
from pathlib import Path
from .common import canonical,digest,read_json,write_json
from .costing import COMPONENTS


def import_details(csv_path,mapping_path,output):
    mapping=read_json(mapping_path)
    if mapping['billing_basis'] not in ('actual','amortized'):raise ValueError('select one billing basis')
    fields=mapping['columns'];seen=set();lines=[];source_hash=digest(csv_path)
    with Path(csv_path).open(newline='') as stream:
        for number,row in enumerate(csv.DictReader(stream),1):
            resource=row[fields['resource_id']]
            if resource not in mapping['resources']:raise ValueError('unmapped resource: '+resource)
            assignment=mapping['resources'][resource]
            if assignment['component'] not in COMPONENTS:raise ValueError('unknown component')
            identity=row[fields['charge_id']]
            if not identity or identity in seen:raise ValueError('duplicate/missing provider charge identity')
            seen.add(identity)
            if row[fields['currency']]!=mapping['currency']:raise ValueError('mixed currency')
            line=dict(assignment,line_id=identity,charge_id=identity,resource_id=resource,meter_id=row[fields['meter_id']],
                window_id=mapping['window_id'],billed_amount=row[fields['amount']],billing_basis=mapping['billing_basis'],
                quantity=row.get(fields.get('quantity','')),unit=row.get(fields.get('unit','')),
                started_at=mapping['started_at'],finished_at=mapping['finished_at'],
                source_ref=str(Path(csv_path).resolve())+'#row='+str(number),source_sha256=source_hash)
            lines.append(line)
    usage={'version':'v1','cost_basis':'azure_provisional','currency':mapping['currency'],
        'allocation_policy_version':mapping['allocation_policy_version'],'runs':mapping['runs'],'pools':mapping.get('pools',[]),
        'required_components':mapping.get('required_components',[]),'lines':lines,
        'import_provenance':{'usage_sha256':digest(csv_path),'mapping_sha256':digest(mapping_path),'billing_basis':mapping['billing_basis']}}
    if mapping.get('billing_scope'):usage['billing_scope']=mapping['billing_scope']
    write_json(output,usage)
    return {'status':'imported','cost_basis':'azure_provisional','lines':len(lines),'output':str(output)}
