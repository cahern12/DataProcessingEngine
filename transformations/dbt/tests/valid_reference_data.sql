with invalid as (
  {% for dataset in ['users','assignment_groups','services','sla_targets'] %}
  select case_id,'invalid {{ dataset }} schema' error
  from {{ source('bronze','raw_records') }} where dataset='{{ dataset }}' and not coalesce({{ record_valid(dataset) }},false)
  union all
  {% endfor %}
  select c.case_id,'incomplete or changed v1 SLA targets'
  from {{ source('bronze','batch_context') }} c
  left join {{ ref('stg_references') }} r on r.case_id=c.case_id and r.dataset='sla_targets'
  group by c.case_id having count(distinct case
    {% for priority, targets in var('sla_targets').items() %}
      when reference_id='{{ priority }}' and response_minutes={{ targets[0] }} and resolution_minutes={{ targets[1] }} then reference_id
    {% endfor %} end)<>4
  union all
  select case_id,'wrong SLA rule version' from {{ source('bronze','raw_records') }}
    where dataset='sla_targets' and {{ field('rule_version') }}<>'v1'
  {% for dataset,key in [('users','user_id'),('assignment_groups','group_id'),('services','service_id'),('sla_targets','priority')] %}
  union all
  select case_id,'conflicting {{ dataset }} reference'
  from {{ source('bronze','raw_records') }} where dataset='{{ dataset }}'
  group by case_id,{{ field(key) }} having count(distinct json_format(cast(row(
    {% for name in var('row_schemas')[dataset]['properties'] %}{{ field(name) }}{% if not loop.last %},{% endif %}{% endfor %}
  ) as json)))>1
  {% endfor %}
)
select * from invalid
