with parsed as (
  select r.*, {{ field('incident_id') }} incident_id, {{ field('event_id') }} event_id,
    {{ utc(field('event_time')) }} event_time,
    try_cast({{ field('source_sequence') }} as bigint) source_sequence,
    {{ field('event_type') }} event_type, {{ field('actor_id') }} actor_id,
    coalesce({{ record_valid('incident_events') }},false)
      and {{ field('event_type') }} in ('response','hold_start','hold_end','resolve','reopen','close','comment') valid
  from {{ source('bronze','raw_records') }} r where dataset='incident_events'
), signatures as (
  select p.*, json_format(cast(row(event_id,incident_id,cast(event_time as varchar),source_sequence,event_type,actor_id) as json)) signature,
    {{ utc('c.cutoff') }} cutoff
  from parsed p join {{ source('bronze','batch_context') }} c on c.case_id=p.case_id
)
select *, row_number() over(partition by case_id,event_id order by if(valid and event_time<=cutoff,0,1),ordinal) identity_rank,
  first_value(signature) over(partition by case_id,event_id order by if(valid and event_time<=cutoff,0,1),ordinal) first_signature
from signatures
