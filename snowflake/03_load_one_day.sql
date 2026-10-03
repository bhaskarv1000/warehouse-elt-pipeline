-- =====================================================================
-- 03  Load ONE run date from S3 into the RAW tables, by hand.
--     This is exactly what the Airflow task will do every night; running
--     it manually first proves the SQL works before automating it.
--
--     To load a different day: Cmd+F, replace every 2026-09-01.
--     Run with Cmd+Shift+Enter (Run All).
-- =====================================================================
USE ROLE TELEMETRY_DBT_ROLE;
USE WAREHOUSE TELEMETRY_WH;
USE SCHEMA TELEMETRY_DB.RAW;

-- One transaction: either the whole day is replaced, or nothing changes.
-- If a COPY fails halfway, the DELETEs are rolled back too, so a table is
-- never left with that day half-missing.
BEGIN;

-- Step 1: remove whatever this run date loaded before (if anything).
-- This is what makes re-running a day REPLACE it instead of doubling it.
DELETE FROM CASE_FULFILLMENT_EVENTS   WHERE RUN_DATE = '2026-09-01';
DELETE FROM EQUIPMENT_TELEMETRY       WHERE RUN_DATE = '2026-09-01';
DELETE FROM SYSTEM_EXCEPTIONS_REJECTS WHERE RUN_DATE = '2026-09-01';

-- Step 2: load that day's file for each stream.
--   $1                 = the whole JSON row -> RAW_DATA
--   '2026-09-01'       = which run it belongs to -> RUN_DATE
--   METADATA$FILENAME  = which S3 file it came from -> SOURCE_FILE
--   LOADED_AT fills itself in (column default).
-- FORCE = TRUE: Snowflake normally skips files it has loaded before. We
-- want a re-run to reload the (possibly regenerated) file, and Step 1
-- already cleared the old rows, so forcing is safe.
COPY INTO CASE_FULFILLMENT_EVENTS (RAW_DATA, RUN_DATE, SOURCE_FILE)
  FROM (SELECT $1, '2026-09-01'::DATE, METADATA$FILENAME
        FROM @S3_RAW_STAGE/case_fulfillment_events/dt=2026-09-01/)
  FORCE = TRUE;

COPY INTO EQUIPMENT_TELEMETRY (RAW_DATA, RUN_DATE, SOURCE_FILE)
  FROM (SELECT $1, '2026-09-01'::DATE, METADATA$FILENAME
        FROM @S3_RAW_STAGE/equipment_telemetry/dt=2026-09-01/)
  FORCE = TRUE;

COPY INTO SYSTEM_EXCEPTIONS_REJECTS (RAW_DATA, RUN_DATE, SOURCE_FILE)
  FROM (SELECT $1, '2026-09-01'::DATE, METADATA$FILENAME
        FROM @S3_RAW_STAGE/system_exceptions_rejects/dt=2026-09-01/)
  FORCE = TRUE;

COMMIT;

-- Check 1: row counts per table for this day.
-- Expected: fulfillment 6,665 | telemetry 185,760 | exceptions ~20-40
SELECT 'case_fulfillment_events' AS tbl, COUNT(*) AS rows_loaded
  FROM CASE_FULFILLMENT_EVENTS WHERE RUN_DATE = '2026-09-01'
UNION ALL
SELECT 'equipment_telemetry', COUNT(*)
  FROM EQUIPMENT_TELEMETRY WHERE RUN_DATE = '2026-09-01'
UNION ALL
SELECT 'system_exceptions_rejects', COUNT(*)
  FROM SYSTEM_EXCEPTIONS_REJECTS WHERE RUN_DATE = '2026-09-01';
