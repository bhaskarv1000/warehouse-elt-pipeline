-- =====================================================================
-- 04  Peek inside the raw JSON. RAW_DATA is one VARIANT column, but
--     Snowflake can reach into it with a colon: RAW_DATA:case_id
--     ::STRING / ::TIMESTAMP casts the value to a real type.
--     (This is a preview of what dbt staging models will do in Phase 5.)
-- =====================================================================
USE ROLE TELEMETRY_DBT_ROLE;
USE WAREHOUSE TELEMETRY_WH;
USE SCHEMA TELEMETRY_DB.RAW;

SELECT
  RAW_DATA:case_id::STRING             AS case_id,
  RAW_DATA:event_type::STRING          AS event_type,
  RAW_DATA:event_timestamp::TIMESTAMP  AS event_timestamp,
  RAW_DATA:order_id::STRING            AS order_id,
  RUN_DATE,
  SOURCE_FILE,
  LOADED_AT
FROM CASE_FULFILLMENT_EVENTS
WHERE RUN_DATE = '2026-09-01'
ORDER BY case_id, event_timestamp
LIMIT 10;
