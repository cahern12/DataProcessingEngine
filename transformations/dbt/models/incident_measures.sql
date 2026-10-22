with stats as (
  select h.*,
    coalesce(max_by(e.state_after,row(e.event_time,e.source_sequence)) filter(where e.event_time is not null),'new') state,
    min(e.event_time) filter(where e.event_type='response' and u.reference_id is not null) first_response_at,
    max(e.event_time) filter(where e.event_type='resolve') latest_resolve,
    coalesce(bool_or(e.event_type='response' and u.reference_id is null),false) unknown_actor
  from {{ ref('incident_headers') }} h
  left join {{ ref('event_timeline') }} e on e.case_id=h.case_id and e.incident_id=h.incident_id
  left join {{ ref('stg_references') }} u on u.case_id=e.case_id and u.dataset='users' and u.reference_id=e.actor_id
  where not exists(select 1 from {{ ref('quarantine') }} q where q.case_id=h.case_id and q.incident_id=h.incident_id)
  group by h.case_id,h.incident_id,h.priority_at_creation,h.creation_service,h.created_at,
    h.current_priority,h.service_id,h.group_id,h.requester_id,h.cutoff,h.report_start
), finals as (
  select *,case when state in ('resolved','closed') then latest_resolve end resolved_at from stats
), hold_edges as (
  select case_id,incident_id,event_type,event_time,
    lead(event_time) over(partition by case_id,incident_id order by event_time,source_sequence) hold_end
  from {{ ref('event_timeline') }} where event_type in ('hold_start','hold_end')
), durations as (
  select f.*,coalesce(sum(greatest(0.0,
    to_unixtime(least(coalesce(he.hold_end,f.cutoff),coalesce(f.resolved_at,f.cutoff)))-to_unixtime(he.event_time))),0.0) held_seconds
  from finals f left join hold_edges he on he.case_id=f.case_id and he.incident_id=f.incident_id
    and he.event_type='hold_start' and he.event_time<coalesce(f.resolved_at,f.cutoff)
  group by f.case_id,f.incident_id,f.priority_at_creation,f.creation_service,f.created_at,
    f.current_priority,f.service_id,f.group_id,f.requester_id,f.cutoff,f.report_start,f.state,
    f.first_response_at,f.latest_resolve,f.unknown_actor,f.resolved_at
)
select d.*,
  coalesce(u.reference_id,'unknown_user') normalized_requester,
  array_sort(concat(if(u.reference_id is null,array['unknown_requester'],cast(array[] as array(varchar))),
    if(unknown_actor,array['unknown_actor'],cast(array[] as array(varchar))))) quality_flags,
  to_unixtime(coalesce(first_response_at,cutoff))-to_unixtime(created_at) response_seconds,
  to_unixtime(coalesce(resolved_at,cutoff))-to_unixtime(created_at)-held_seconds resolution_seconds,
  sla.response_minutes response_target,sla.resolution_minutes resolution_target
from durations d
left join {{ ref('stg_references') }} u on u.case_id=d.case_id and u.dataset='users' and u.reference_id=d.requester_id
join {{ ref('stg_references') }} sla on sla.case_id=d.case_id and sla.dataset='sla_targets' and sla.reference_id=d.priority_at_creation
