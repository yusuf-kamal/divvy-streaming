{% macro retain_bronze(retain_days=3) %}

    {% set sql %}
        EXEC bronze.retain_station_status @retain_days = {{ retain_days }};
    {% endset %}

    {% do run_query(sql) %}
    {{ log("bronze.station_status trimmed to last " ~ retain_days ~ " day(s)", info=True) }}

{% endmacro %}