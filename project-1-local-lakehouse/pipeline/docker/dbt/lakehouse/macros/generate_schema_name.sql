{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

{#
  Override dbt's default schema naming behaviour.

  By default dbt concatenates target.schema (profiles.yml) + custom schema
  (dbt_project.yml) with an underscore, e.g. "main" + "bronze" → "main_bronze".

  We want clean Polaris namespace names (bronze, silver, gold) so models land at
  polaris.bronze.stg_events rather than polaris.main_bronze.stg_events.

  The fix is simple: if a custom schema is set, use it exactly as written.

  Ref: dbt docs — "How does dbt generate a model's schema name"
  https://docs.getdbt.com/docs/build/custom-schemas
#}

