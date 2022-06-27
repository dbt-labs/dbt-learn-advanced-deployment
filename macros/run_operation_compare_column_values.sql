{% macro check_columns() %}

{%- set columns_to_compare=adapter.get_columns_in_relation(ref('fct_web_events'))  -%}

{% set old_etl_relation_query %}
    select * from "ANALYTICS"."ADVANCED_DEPLOYMENT_PRODUCTION"."FCT_WEB_EVENTS"
{% endset %}

{% set new_etl_relation_query %}
    select * from {{ ref('fct_web_events') }}
{% endset %}

{% if execute %}
    {% for column in columns_to_compare %}
        {{ log('Comparing column "' ~ column.name ~'"', info=True) }}

        {% set audit_query = audit_helper.compare_column_values(
            a_query=old_etl_relation_query,
            b_query=new_etl_relation_query,
            primary_key="page_view_id",
            column_to_compare=column.name
        ) %}

        {% set audit_results = run_query(audit_query) %}
        {% do audit_results.print_table() %}
        {{ log("", info=True) }}

    {% endfor %}
{% endif %}

{% endmacro %}