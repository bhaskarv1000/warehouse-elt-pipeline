-- =====================================================================
-- 02  Raw landing objects: file format, stage, and one table per stream.
--     Run as the project role (not ACCOUNTADMIN), after 01 is done and
--     the AWS trust policy has been updated.
-- =====================================================================
USE ROLE TELEMETRY_DBT_ROLE;
USE WAREHOUSE TELEMETRY_WH;
USE SCHEMA TELEMETRY_DB.RAW;

-- "Each line of these files is one JSON object" (NDJSON).
CREATE FILE FORMAT IF NOT EXISTS NDJSON_FORMAT
  TYPE = JSON
  COMMENT = 'Newline-delimited JSON, one row per line';

-- A named shortcut to s3://.../raw/ so COPY statements can just say
-- @S3_RAW_STAGE/<stream>/dt=<date>/ instead of the full bucket URL.
CREATE STAGE IF NOT EXISTS S3_RAW_STAGE
  URL = 's3://bhaskarv1000-warehouse-elt-raw/raw/'
  STORAGE_INTEGRATION = S3_WAREHOUSE_ELT_INT
  FILE_FORMAT = NDJSON_FORMAT
  COMMENT = 'Daily run-date partitions landed by the Airflow DAG';

-- Raw tables: the whole JSON row goes into RAW_DATA untouched. The
-- other three columns record WHERE and WHEN each row was loaded, which
-- is what makes "delete this run_date, then reload it" possible.
-- Typing, renaming and cleaning happen later in dbt (STAGING), not here.
CREATE TABLE IF NOT EXISTS CASE_FULFILLMENT_EVENTS (
  RAW_DATA     VARIANT,
  RUN_DATE     DATE,
  SOURCE_FILE  VARCHAR,
  LOADED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS EQUIPMENT_TELEMETRY (
  RAW_DATA     VARIANT,
  RUN_DATE     DATE,
  SOURCE_FILE  VARCHAR,
  LOADED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS SYSTEM_EXCEPTIONS_REJECTS (
  RAW_DATA     VARIANT,
  RUN_DATE     DATE,
  SOURCE_FILE  VARCHAR,
  LOADED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Smoke test: if the AWS handshake worked, this lists S3 files.
-- Expect three .jsonl files for 2026-09-01.
LIST @S3_RAW_STAGE PATTERN = '.*dt=2026-09-01.*';
