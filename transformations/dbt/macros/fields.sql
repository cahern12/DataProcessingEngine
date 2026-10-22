{% macro field(name, payload='payload') -%}
trim(json_extract_scalar({{ payload }}, '$.{{ name }}'))
{%- endmacro %}

{% macro utc(expr) -%}
case when regexp_like({{ expr }}, '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?(Z|[+-]\d{2}:\d{2})$')
then at_timezone(try(cast(from_iso8601_timestamp_nanos({{ expr }}) as timestamp(6) with time zone)), 'UTC') end
{%- endmacro %}

{% macro record_valid(dataset) -%}
{% set schema = var('row_schemas')[dataset] %}
(
  array_sort(map_keys(cast(json_parse(payload) as map(varchar,json)))) = array_sort(array[
  {% for name in schema['properties'] %}'{{ name }}'{% if not loop.last %},{% endif %}{% endfor %}])
  {% for name, spec in schema['properties'].items() %}
    {% set types = spec['type'] if spec['type'] is not string else [spec['type']] %}
    and (
      {% if 'null' in types %} json_format(json_extract(payload, '$.{{ name }}')) = 'null' or {% endif %}
      {% if 'string' in types %}
        (substr(json_format(json_extract(payload, '$.{{ name }}')), 1, 1) = '"'
        {% if spec.get('minLength') %}and length({{ field(name) }}) >= {{ spec['minLength'] }}{% endif %}
        {% if spec.get('format') == 'date-time' %}and {{ utc(field(name)) }} is not null{% endif %})
      {% elif 'integer' in types %}
        (regexp_like(json_format(json_extract(payload, '$.{{ name }}')), '^-?\d+$')
        and try_cast({{ field(name) }} as bigint) >= {{ spec.get('minimum', 0) }})
      {% endif %}
    )
    {% if spec.get('enum') %}
      and {{ field(name) }} in ({% for value in spec['enum'] %}'{{ value }}'{% if not loop.last %},{% endif %}{% endfor %})
    {% endif %}
  {% endfor %}
)
{%- endmacro %}

{% macro mapped(name, mapping) -%}
case {{ field(name) }} {% for source, target in mapping.items() %}when '{{ source }}' then '{{ target }}' {% endfor %}end
{%- endmacro %}
