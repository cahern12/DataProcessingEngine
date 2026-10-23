"""Immutable public Azure retail-price retrieval; no subscription or deployment."""
import hashlib
import json
from datetime import datetime,timezone
from pathlib import Path
from urllib.parse import urlencode,urlparse
from urllib.request import urlopen
from .common import canonical,write_json

ENDPOINT='https://prices.azure.com/api/retail/prices'


def retrieve(output,filters,currency='USD',fetch=None):
    output=Path(output)
    if output.exists():raise ValueError('price snapshot requires a fresh directory')
    output.mkdir(parents=True)
    url=ENDPOINT+'?'+urlencode({'api-version':'2023-01-01-preview','currencyCode':"'"+currency+"'",'$filter':filters})
    pages=[];records=[];seen=set()
    while url:
        parsed=urlparse(url)
        if parsed.scheme!='https' or parsed.hostname!='prices.azure.com':raise ValueError('untrusted price pagination URL')
        if url in seen:raise ValueError('price pagination loop')
        seen.add(url)
        if fetch:body=fetch(url)
        else:
            with urlopen(url,timeout=60) as response:body=response.read()
        value=json.loads(body);name='page-%03d.json'%len(pages)
        (output/name).write_bytes(body)
        pages.append({'url':url,'path':name,'sha256':hashlib.sha256(body).hexdigest()})
        records.extend(value['Items']);url=value.get('NextPageLink')
    metadata={'provider':'Microsoft Azure public retail','currency':currency,'filter':filters,
              'retrieved_at':datetime.now(timezone.utc).isoformat(),'api_version':'2023-01-01-preview',
              'pages':pages,'records':len(records),'records_sha256':hashlib.sha256(canonical(records).encode()).hexdigest(),
              'warning':'Public retail scenario inputs; no negotiated invoice, cloud execution or billing reconciliation'}
    write_json(output/'snapshot.json',metadata);write_json(output/'records.json',records)
    return records


def select(records,**fields):
    matches=[r for r in records if all(r.get(k)==v for k,v in fields.items())]
    if len(matches)!=1:raise ValueError('retail meter selection must be unique: '+str(fields)+' matches='+str(len(matches)))
    result=matches[0]
    if result['currencyCode']!='USD' or result['type']!='Consumption':raise ValueError('unsupported retail accounting basis')
    return result
