{#
  Load ONE run date ({{ ds }}) from S3 into the three RAW tables.
  Same logic as snowflake/03_load_one_day.sql, but the date comes from
  Airflow instead of being typed in, and a Jinja loop writes the
  DELETE + COPY pair once per stream instead of copy-pasting it 3 times.

  Re-run safe: each run deletes its own day first, then reloads it, all
  inside one transaction (all-or-nothing).
#}
{% set streams = ['case_fulfillment_events', 'equipment_telemetry', 'system_exceptions_rejects'] %}
BEGIN;
{% for s in streams %}
DELETE FROM TELEMETRY_DB.RAW.{{ s | upper }} WHERE RUN_DATE = '{{ ds }}';

COPY INTO TELEMETRY_DB.RAW.{{ s | upper }} (RAW_DATA, RUN_DATE, SOURCE_FILE)
  FROM (SELECT $1, '{{ ds }}'::DATE, METADATA$FILENAME
        FROM @TELEMETRY_DB.RAW.S3_RAW_STAGE/{{ s }}/dt={{ ds }}/)
  FORCE = TRUE;
{% endfor %}
COMMIT;
