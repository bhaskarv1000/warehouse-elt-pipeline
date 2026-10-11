-- =====================================================================
-- 06  Cost guard rails: a monthly credit cap + fast warehouse auto-suspend.
--
-- Why: the account is pay-as-you-go now. Compute is the only cost that can
-- run away (storage for this project is cents). A resource monitor is a
-- circuit breaker: when the month's credits hit the quota, Snowflake
-- suspends the warehouses so nothing can keep billing.
--
-- Run as ACCOUNTADMIN (resource monitors need admin rights).
-- Attached at the ACCOUNT level so it covers every warehouse, including
-- TELEMETRY_WH (dbt / Airflow) and the default COMPUTE_WH.
-- =====================================================================
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE RESOURCE MONITOR WAREHOUSE_ELT_MONTHLY_CAP
  WITH CREDIT_QUOTA = 5
       FREQUENCY = MONTHLY
       START_TIMESTAMP = IMMEDIATELY
  TRIGGERS
    ON 75  PERCENT DO NOTIFY                 -- warning at 3.75 credits
    ON 100 PERCENT DO SUSPEND                -- let running queries finish, then stop
    ON 110 PERCENT DO SUSPEND_IMMEDIATE;     -- hard stop, cancel running queries

ALTER ACCOUNT SET RESOURCE_MONITOR = WAREHOUSE_ELT_MONTHLY_CAP;

-- Stop billing quickly once a warehouse goes idle (seconds).
ALTER WAREHOUSE TELEMETRY_WH SET AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;
ALTER WAREHOUSE COMPUTE_WH   SET AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

-- ---- Verify (should show the monitor, quota 5, monthly) ----
SHOW RESOURCE MONITORS;
SHOW WAREHOUSES;   -- check the auto_suspend column = 60 and size = X-Small
