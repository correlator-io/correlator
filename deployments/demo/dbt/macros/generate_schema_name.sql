{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
        Override dbt's default schema naming behavior.

        Default behavior: {{ target_schema }}_{{ custom_schema_name }}
        Our behavior: Use custom_schema_name directly if provided

        This ensures:
        - +schema: staging -> schema "staging" (not "public_staging")
        - +schema: marts -> schema "marts" (not "public_marts")
        - No custom schema -> use target schema (public)
    #}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
