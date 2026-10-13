-- =====================================================================
-- 07  A dedicated Snowflake user for dbt, signing in with a key pair.
--
-- Why: the account now requires MFA for human users, so dbt can no longer
-- sign in with your password. A TYPE = SERVICE user has no password and
-- signs in with a key only. Same pattern as AIRFLOW_LOADER (script 05).
-- Run as ACCOUNTADMIN.
-- =====================================================================
USE ROLE ACCOUNTADMIN;

CREATE USER IF NOT EXISTS DBT_RUNNER
  TYPE = SERVICE
  DEFAULT_ROLE = TELEMETRY_DBT_ROLE
  DEFAULT_WAREHOUSE = TELEMETRY_WH
  DEFAULT_NAMESPACE = TELEMETRY_DB.STAGING
  COMMENT = 'dbt robot user: builds models in TELEMETRY_DB';

-- Paste the PUBLIC key body (no BEGIN/END lines, one line).
ALTER USER DBT_RUNNER SET RSA_PUBLIC_KEY = '<PASTE_PUBLIC_KEY_HERE>';

GRANT ROLE TELEMETRY_DBT_ROLE TO USER DBT_RUNNER;
