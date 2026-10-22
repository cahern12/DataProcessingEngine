with errors as (
  select case_id,incident_id,'BR-001' rule_id,'invalid source record' reason from {{ ref('stg_snapshots') }} where not valid
  union all
  select case_id,incident_id,'BR-001','invalid source record' from {{ ref('stg_events') }} where not valid
  union all
  select case_id,incident_id,'BR-002','conflicting snapshot identity' from {{ ref('stg_snapshots') }}
    where valid and source_updated_at<=cutoff group by case_id,incident_id,source_version having count(distinct signature)>1
  union all
  select e.case_id,e.incident_id,'BR-002','conflicting event identity' from {{ ref('stg_events') }} e
    join (select case_id,event_id from {{ ref('stg_events') }} where valid and event_time<=cutoff
      group by 1,2 having count(distinct signature)>1) c on c.case_id=e.case_id and c.event_id=e.event_id
  union all
  select s.case_id,s.incident_id,'BR-003','missing service/group reference' from {{ ref('eligible_snapshots') }} s
    left join {{ ref('stg_references') }} svc on svc.case_id=s.case_id and svc.dataset='services' and svc.reference_id=s.service_id
    left join {{ ref('stg_references') }} grp on grp.case_id=s.case_id and grp.dataset='assignment_groups' and grp.reference_id=s.group_id
    where svc.reference_id is null or grp.reference_id is null
  union all
  select e.case_id,e.incident_id,'BR-003','event has no eligible snapshot' from {{ ref('eligible_events') }} e
    left join {{ ref('incident_headers') }} h on h.case_id=e.case_id and h.incident_id=e.incident_id where h.incident_id is null
  union all
  select s.case_id,s.incident_id,'BR-004','snapshot precedes incident creation' from {{ ref('eligible_snapshots') }} s
    join {{ ref('incident_headers') }} h on h.case_id=s.case_id and h.incident_id=s.incident_id where s.source_updated_at<h.created_at or h.created_at>h.cutoff
  union all
  select e.case_id,e.incident_id,'BR-004','event precedes creation' from {{ ref('event_timeline') }} e
    join {{ ref('incident_headers') }} h on h.case_id=e.case_id and h.incident_id=e.incident_id where e.event_time<h.created_at
  union all
  select case_id,incident_id,'BR-004','ambiguous event order' from {{ ref('event_timeline') }}
    group by case_id,incident_id,event_time,source_sequence having count(*)>1
  union all
  select case_id,incident_id,
    case when event_type in ('hold_start','hold_end') or prior_state='on_hold' then 'BR-005' else 'BR-004' end,
    'invalid event transition'
  from {{ ref('event_timeline') }} where not (
    {% for kind, states in var('business_rules')['event_transitions'].items() %}
      (event_type='{{ kind }}' and prior_state in ({% for state in states %}'{{ state }}'{% if not loop.last %},{% endif %}{% endfor %}))
      {% if not loop.last %} or {% endif %}
    {% endfor %})
  union all
  select s.case_id,s.incident_id,'BR-004','snapshot state disagrees with events' from {{ ref('eligible_snapshots') }} s
    left join {{ ref('event_timeline') }} e on e.case_id=s.case_id and e.incident_id=s.incident_id and e.event_time<=s.source_updated_at
    group by s.case_id,s.incident_id,s.ordinal,s.status
    having s.status<>coalesce(max_by(e.state_after,row(e.event_time,e.source_sequence)) filter(where e.event_time is not null),'new')
)
select distinct case_id,incident_id,rule_id,reason from errors
