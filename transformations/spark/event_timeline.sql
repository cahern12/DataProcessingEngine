with states as (
  select *, case event_type
    when 'response' then 'in_progress' when 'hold_start' then 'on_hold'
    when 'hold_end' then 'in_progress' when 'resolve' then 'resolved'
    when 'reopen' then 'in_progress' when 'close' then 'closed' end event_state
  from {{ ref('eligible_events') }}
), previous as (
  select *,coalesce(last_value(event_state) ignore nulls over(
    partition by case_id,incident_id order by event_time,source_sequence
    rows between unbounded preceding and 1 preceding),'new') prior_state
  from states
)
select *, coalesce(event_state,prior_state) state_after from previous
