{#
  Data-quality gate for one run date. SQLCheckOperator fails the task if
  ANY value in this row is false/0/NULL.

  Why it exists: if a file is missing, COPY INTO doesn't error. It just
  reports "0 files processed" and the task goes green with nothing loaded.
  This check turns that silent gap into a red task.

  Exceptions aren't checked for > 0: a quiet day with zero rejects is
  legitimate (it happened once in the original September data).
#}
SELECT
  (SELECT COUNT(*) FROM TELEMETRY_DB.RAW.CASE_FULFILLMENT_EVENTS WHERE RUN_DATE = '{{ ds }}') > 0
    AS fulfillment_loaded,
  (SELECT COUNT(*) FROM TELEMETRY_DB.RAW.EQUIPMENT_TELEMETRY     WHERE RUN_DATE = '{{ ds }}') = 185760
    AS telemetry_complete  -- 43 assets x 4,320 heartbeats, always exact
