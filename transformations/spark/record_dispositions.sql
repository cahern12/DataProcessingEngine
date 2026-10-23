with incidents as (
  select case_id,dataset,ordinal,incident_id,valid,source_updated_at source_time,cutoff,identity_rank,signature,first_signature
  from {{ ref('stg_snapshots') }}
  union all
  select case_id,dataset,ordinal,incident_id,valid,event_time,cutoff,identity_rank,signature,first_signature
  from {{ ref('stg_events') }}
), first_error as (
  select case_id,incident_id,min(rule_id) rule_id,min(reason) reason from {{ ref('quarantine') }} group by 1,2
)
select i.case_id,i.dataset,i.ordinal,i.incident_id,
  case when i.valid and i.source_time>i.cutoff then 'deferred'
    when i.valid and identity_rank>1 and signature=first_signature then 'duplicate'
    when q.rule_id is not null then 'quarantined' else 'accepted' end disposition,
  case when i.valid and i.source_time>i.cutoff then cast(null as string)
    when i.valid and identity_rank>1 and signature=first_signature then 'BR-002' else q.rule_id end rule_id,
  q.reason
from incidents i left join first_error q on q.case_id=i.case_id and q.incident_id=i.incident_id
union all
select case_id,dataset,ordinal,cast(null as string),
  if(row_number() over(partition by case_id,dataset,payload order by ordinal)>1,'duplicate','accepted'),
  cast(null as string),cast(null as string)
from {{ source('bronze','raw_records') }} where dataset not in ('incident_snapshots','incident_events')
