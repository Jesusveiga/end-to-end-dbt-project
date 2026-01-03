{% macro get_order_statuses() %}

    {% set status_query %}
        select distinct status from {{ ref('stg_app__orders') }}
        order by 1
    {% endset %}

    {% set results = run_query(status_query) %}

    {% if execute %}
    {# Extraemos los valores de la primera columna del resultado #}
    {% set results_list = results.columns[0].values() %}
    {% else %}
    {% set results_list = [] %}
    {% endif %}

    {{ return(results_list) }}

{% endmacro %}