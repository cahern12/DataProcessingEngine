select distinct case_id, dataset,
  coalesce({{ field('user_id') }},{{ field('service_id') }},{{ field('group_id') }},{{ field('priority') }}) reference_id,
  try_cast({{ field('response_minutes') }} as bigint) response_minutes,
  try_cast({{ field('resolution_minutes') }} as bigint) resolution_minutes
from {{ source('bronze','raw_records') }}
where dataset not in ('incident_snapshots','incident_events')
