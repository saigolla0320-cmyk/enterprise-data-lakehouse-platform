{% macro safe_divide(numerator, denominator) %}
    /*
        Null-safe division. Returns NULL instead of erroring on divide-by-zero.
        Usage: {{ safe_divide('revenue', 'customer_count') }}
    */
    case
        when {{ denominator }} = 0 or {{ denominator }} is null
        then null
        else {{ numerator }} / {{ denominator }}
    end
{% endmacro %}


{% macro generate_surrogate_key(field_list) %}
    /*
        Deterministic surrogate key from a list of fields.
        Usage: {{ generate_surrogate_key(['customer_id', 'transaction_date']) }}
    */
    md5(
        {%- for field in field_list %}
        coalesce(cast({{ field }} as varchar), '_null_')
        {%- if not loop.last %} || '-' || {% endif -%}
        {% endfor %}
    )
{% endmacro %}


{% macro cents_to_dollars(column_name, precision=2) %}
    round(cast({{ column_name }} as numeric) / 100.0, {{ precision }})
{% endmacro %}


{% macro grant_select_to_reporting() %}
    /*
        Post-hook macro: grants SELECT on built models to the reporting role.
        Wired via +post-hook in dbt_project.yml for prod target.
    */
    {% if target.name == 'prod' %}
        grant select on {{ this }} to role REPORTER
    {% endif %}
{% endmacro %}
