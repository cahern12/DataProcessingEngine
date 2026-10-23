select case_id,date,service_id,priority,
  sum(created_count) created_count,sum(resolved_count) resolved_count,sum(backlog_count) backlog_count,
  sum(response_passed) response_passed,sum(response_breached) response_breached,
  sum(resolution_passed) resolution_passed,sum(resolution_breached) resolution_breached,
  round(cast(sum(resolution_passed) as double)/nullif(sum(resolution_passed)+sum(resolution_breached),0),6) resolution_pass_rate,
  round(sum(resolution_seconds)/nullif(sum(resolved_count),0)/60.0,6) avg_resolution_minutes
from {{ ref('incident_daily_contributions') }}
{% if var('targeted_daily', false) %}
where (date,service_id,priority) in (select date,service_id,priority from {{ source('bronze','affected_daily_groups') }})
{% endif %}
group by 1,2,3,4
