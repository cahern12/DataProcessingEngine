{{ config(tags=['compatibility']) }}
select id, value from lake.{{ var('raw_schema', target.schema) }}.probe
