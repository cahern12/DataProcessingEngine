select case_id,incident_id,created_at,priority_at_creation,current_priority,state,service_id,group_id,
  normalized_requester requester_id,quality_flags,first_response_at,resolved_at,
  if(first_response_at is not null,round(response_seconds/60.0,6)) response_minutes,
  if(resolved_at is not null,round(resolution_seconds/60.0,6)) resolution_minutes,
  case when response_seconds>response_target*60 then 'breached'
       when first_response_at is not null then 'passed' else 'pending' end response_sla,
  case when resolution_seconds>resolution_target*60 then 'breached'
       when resolved_at is not null then 'passed' else 'pending' end resolution_sla
from {{ ref('incident_measures') }}
