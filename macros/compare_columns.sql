{% macro compare_columns() %}

{%- set columns_to_compare=adapter.get_columns_in_relation(ref('fct_orders'))  -%}

{% set old_etl_relation_query %}
    select * from "ANALYTICS"."ADVANCED_DEPLOYMENT_PRODUCTION"."FCT_ORDERS"
{% endset %}

{% set new_etl_relation_query %}
    select * from {{ ref('fct_orders') }}
{% endset %}

{% if execute %}
    {% for column in columns_to_compare %}
        {{ log('Comparing column "' ~ column.name ~'"', info=True) }}

        {% set audit_query = audit_helper.compare_column_values(
            a_query=old_etl_relation_query,
            b_query=new_etl_relation_query,
            primary_key="order_id",
            column_to_compare=column.name
        ) %}

        {% set audit_results = run_query(audit_query) %}

        {% do log(audit_results.column_names, info=True) %}
        {% for row in audit_results.rows %}
            {% do log(row.values(), info=True) %}
        {% endfor %}

    {% endfor %}
    
{% endif %}

{% endmacro %}
