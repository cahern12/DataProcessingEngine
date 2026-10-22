select 'incident_resolution' dataset,case_id,incident_id key from {{ ref('gold_incident_resolution') }}
group by case_id,incident_id having count(*)>1
union all
select 'service_daily_metrics',case_id,json_format(cast(row(date,service_id,priority) as json)) from {{ ref('gold_service_daily_metrics') }}
group by case_id,date,service_id,priority having count(*)>1
