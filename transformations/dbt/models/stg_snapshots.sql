with parsed as (
  select r.*, {{ field('incident_id') }} incident_id,
    try_cast({{ field('source_version') }} as bigint) source_version,
    {{ utc(field('source_updated_at')) }} source_updated_at,
    {{ utc(field('created_at')) }} created_at,
    {{ mapped('priority', var('business_rules')['priority_mapping']) }} priority,
    {{ mapped('status', var('business_rules')['status_mapping']) }} status,
    {{ field('service_id') }} service_id, {{ field('group_id') }} group_id,
    {{ field('requester_id') }} requester_id,
    coalesce({{ record_valid('incident_snapshots') }}, false) schema_valid
  from {{ source('bronze','raw_records') }} r where dataset='incident_snapshots'
), signatures as (
  select p.*, schema_valid and priority is not null and status is not null valid,
    json_format(cast(row(incident_id, source_version, cast(source_updated_at as varchar),
      cast(created_at as varchar), priority, status, service_id, group_id, requester_id) as json)) signature,
    {{ utc('c.cutoff') }} cutoff, {{ utc('c.report_start') }} report_start
  from parsed p join {{ source('bronze','batch_context') }} c on c.case_id=p.case_id
)
select *, row_number() over(partition by case_id,incident_id,source_version order by if(valid and source_updated_at<=cutoff,0,1),ordinal) identity_rank,
  first_value(signature) over(partition by case_id,incident_id,source_version order by if(valid and source_updated_at<=cutoff,0,1),ordinal) first_signature
from signatures
