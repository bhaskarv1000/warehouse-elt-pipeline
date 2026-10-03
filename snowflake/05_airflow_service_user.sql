-- =====================================================================
-- 05  A dedicated Snowflake user for Airflow, signing in with a key pair.
--
-- Why not your own login? Your user is a HUMAN user (password + MFA).
-- Snowflake blocks password-only sign-in for scripts, and a robot
-- shouldn't share your identity anyway. TYPE = SERVICE marks this as a
-- robot account: no password at all, it can only sign in with a key.
--
-- Same idea as the AWS uploader user: one robot, one job, minimal access.
-- Run as ACCOUNTADMIN (creating users needs admin rights).
-- =====================================================================
USE ROLE ACCOUNTADMIN;

CREATE USER IF NOT EXISTS AIRFLOW_LOADER
  TYPE = SERVICE
  DEFAULT_ROLE = TELEMETRY_DBT_ROLE
  DEFAULT_WAREHOUSE = TELEMETRY_WH
  DEFAULT_NAMESPACE = TELEMETRY_DB.RAW
  COMMENT = 'Airflow robot user: loads S3 files into TELEMETRY_DB.RAW';

-- Paste the PUBLIC key here: the lines BETWEEN
-- "-----BEGIN PUBLIC KEY-----" and "-----END PUBLIC KEY-----",
-- joined into one line, without those two header lines.
-- (The public key is safe to share. The PRIVATE key never leaves your Mac
-- except into Airflow's encrypted connection.)
ALTER USER AIRFLOW_LOADER SET RSA_PUBLIC_KEY = '<PASTE_PUBLIC_KEY_HERE>';

GRANT ROLE TELEMETRY_DBT_ROLE TO USER AIRFLOW_LOADER;

-- Airflow needs your account identifier ("orgname-accountname").
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS account_identifier;
