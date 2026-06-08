{% macro describe_events() %}

    {% set results = run_query(
        "DESCRIBE polaris.silver.events_sample"
    ) %}

    {% do results.print_table() %}

{% endmacro %}