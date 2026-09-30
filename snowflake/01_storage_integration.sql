-- =====================================================================
-- 01  Storage integration: lets Snowflake read the S3 landing zone
--     WITHOUT any access keys. Run as ACCOUNTADMIN (only an admin can
--     create integrations).
--
-- How the trust works:
--   Snowflake has its own AWS identity. We tell AWS "that identity may
--   put on the snowflake-warehouse-elt-reader role, but only if it
--   presents this secret external ID". The role can only READ raw/.
--
-- Order (the AWS role has to exist before this script runs):
--   1. AWS: create policy + role (see snowflake/aws/)       -> copy Role ARN
--   2. THIS SCRIPT: paste the Role ARN below, run it
--   3. DESC INTEGRATION                                    -> copy 2 values
--   4. AWS: paste those 2 values into the role's trust policy
-- =====================================================================
USE ROLE ACCOUNTADMIN;

CREATE STORAGE INTEGRATION IF NOT EXISTS S3_WAREHOUSE_ELT_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<YOUR_AWS_ACCOUNT_ID>:role/snowflake-warehouse-elt-reader'
  STORAGE_ALLOWED_LOCATIONS = ('s3://bhaskarv1000-warehouse-elt-raw/raw/')
  COMMENT = 'Read-only access to the warehouse-elt S3 landing zone';

-- Let the project role build stages on top of this integration.
GRANT USAGE ON INTEGRATION S3_WAREHOUSE_ELT_INT TO ROLE TELEMETRY_DBT_ROLE;

-- Copy these two values into the AWS role's trust policy
-- (snowflake/aws/snowflake_trust_policy.template.json):
--   STORAGE_AWS_IAM_USER_ARN   -> "Principal": { "AWS": ... }
--   STORAGE_AWS_EXTERNAL_ID    -> "sts:ExternalId": ...
DESC INTEGRATION S3_WAREHOUSE_ELT_INT;
