{% macro masking() %}
regexp_replace(email, '(^[^@]{2})[^@]+(@.*$)', '\\1****\\2')
{% endmacro %}

 