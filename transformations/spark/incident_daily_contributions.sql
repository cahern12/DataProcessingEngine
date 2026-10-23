with creations as (
  select case_id,incident_id,cast(created_at as date) date,creation_service service_id,priority_at_creation priority,
    count(*) created_count from {{ ref('incident_measures') }}
  where created_at>=report_start and created_at<cutoff group by 1,2,3,4,5
), resolution_dimensions as (
  select i.case_id,i.incident_id,i.resolved_at,i.resolution_seconds,
    max_by(s.service_id,struct(s.source_updated_at,s.source_version)) service_id,
    max_by(s.priority,struct(s.source_updated_at,s.source_version)) priority
  from {{ ref('incident_measures') }} i
  join {{ ref('eligible_snapshots') }} s on s.case_id=i.case_id and s.incident_id=i.incident_id
    and s.source_updated_at<=i.resolved_at
  where i.resolved_at>=i.report_start and i.resolved_at<i.cutoff
  group by 1,2,3,4
), resolutions as (
  select d.case_id,d.incident_id,cast(d.resolved_at as date) date,d.service_id,d.priority,
    count(*) resolved_count,sum(d.resolution_seconds) resolution_seconds,
    count_if(i.response_sla='passed') response_passed,count_if(i.response_sla='breached') response_breached,
    count_if(i.resolution_sla='passed') resolution_passed,count_if(i.resolution_sla='breached') resolution_breached
  from resolution_dimensions d join {{ ref('gold_incident_resolution') }} i on i.case_id=d.case_id and i.incident_id=d.incident_id
  group by 1,2,3,4,5
), days as (
  select i.*,d.date,least(cast(d.date+interval '1' day as timestamp),i.cutoff) day_end
  from {{ ref('incident_measures') }} i
  lateral view explode(sequence(cast(i.report_start as date),cast(i.cutoff as date),interval '1' day)) d as date
  where cast(d.date as timestamp)<i.cutoff
), day_states as (
  select d.case_id,d.incident_id,d.date,d.day_end,
    coalesce(max_by(e.state_after,struct(e.event_time,e.source_sequence)) filter(where e.event_time is not null),'new') state
  from days d left join {{ ref('event_timeline') }} e on e.case_id=d.case_id and e.incident_id=d.incident_id and e.event_time<d.day_end
  where d.created_at<d.day_end group by 1,2,3,4
), backlog_dimensions as (
  select d.case_id,d.incident_id,d.date,
    coalesce(max_by(s.service_id,struct(s.source_updated_at,s.source_version)) filter(where s.source_updated_at<d.day_end),
      min_by(s.service_id,struct(s.source_updated_at,s.source_version))) service_id,
    coalesce(max_by(s.priority,struct(s.source_updated_at,s.source_version)) filter(where s.source_updated_at<d.day_end),
      min_by(s.priority,struct(s.source_updated_at,s.source_version))) priority
  from day_states d join {{ ref('eligible_snapshots') }} s on s.case_id=d.case_id and s.incident_id=d.incident_id
  where d.state not in ('resolved','closed') group by 1,2,3
), backlog as (
  select case_id,incident_id,date,service_id,priority,count(*) backlog_count from backlog_dimensions group by 1,2,3,4,5
), keys as (
  select case_id,incident_id,date,service_id,priority from creations union
  select case_id,incident_id,date,service_id,priority from resolutions union
  select case_id,incident_id,date,service_id,priority from backlog
)
select k.*,coalesce(c.created_count,0) created_count,coalesce(r.resolved_count,0) resolved_count,
  coalesce(b.backlog_count,0) backlog_count,coalesce(r.response_passed,0) response_passed,
  coalesce(r.response_breached,0) response_breached,coalesce(r.resolution_passed,0) resolution_passed,
  coalesce(r.resolution_breached,0) resolution_breached,
  coalesce(r.resolution_seconds,0.0) resolution_seconds
from keys k left join creations c on c.case_id=k.case_id and c.incident_id=k.incident_id and c.date=k.date and c.service_id=k.service_id and c.priority=k.priority
left join resolutions r on r.case_id=k.case_id and r.incident_id=k.incident_id and r.date=k.date and r.service_id=k.service_id and r.priority=k.priority
left join backlog b on b.case_id=k.case_id and b.incident_id=k.incident_id and b.date=k.date and b.service_id=k.service_id and b.priority=k.priority
