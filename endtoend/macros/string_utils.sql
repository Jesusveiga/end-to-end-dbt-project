-- macros/utils.sql
{% macro string_utils(column_name) %}
    REGEXP_REPLACE(TRIM({{ column_name }}), '\s+', '' )
{% endmacro %}