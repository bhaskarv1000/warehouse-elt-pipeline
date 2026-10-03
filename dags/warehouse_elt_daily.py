"""
Daily warehouse ELT pipeline: generate one day of synthetic warehouse
data, land it in S3, then load it into Snowflake.

    generate_fulfillment ──> generate_exceptions ──┐
                                                   ├──> upload_to_s3 ──> load_to_snowflake ──> check_raw_counts
    generate_telemetry ────────────────────────────┘

- generate_exceptions waits for generate_fulfillment because it reads
  that day's case_ids (rejects must reference real cases).
- generate_telemetry has no dependency on either, so Airflow runs it
  in parallel with them.
- upload_to_s3 waits for all three, so a day is only uploaded once it's
  complete, never half-generated.
- load_to_snowflake runs dags/sql/load_raw_day.sql: delete that day's
  RAW rows, then COPY INTO from S3, in one transaction (re-run safe).
- check_raw_counts runs dags/sql/check_raw_day.sql and fails the run if
  the load silently brought in nothing (COPY doesn't error on 0 files).

Which day does a run process?
    {{ ds }} is the run's logical date. With CronDataIntervalTimetable,
    the run for Oct 1 covers the interval Oct 1 00:00 -> Oct 2 00:00,
    fires at the END of that interval (just after midnight Oct 2), and
    has ds = 2026-10-01. In other words: each run processes the day that
    just finished, which is the classic batch-ELT convention.
"""
import sys
from datetime import timedelta
import os

import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLExecuteQueryOperator,
)
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable

# Where docker-compose mounts the repo folders inside the containers.
AIRFLOW_HOME = "/opt/airflow"
GENERATOR_DIR = f"{AIRFLOW_HOME}/data_generator"
S3_LOADER_DIR = f"{AIRFLOW_HOME}/s3_loader"

# Airflow Connection holding the warehouse-elt-uploader IAM user's keys.
# Credentials live (encrypted by the Fernet key) in Airflow's metadata DB,
# not in this file and not in the container image.
AWS_CONN_ID = "aws_warehouse_elt"

# Airflow Connection for the AIRFLOW_LOADER Snowflake service user
# (key-pair sign-in, no password). Same idea: secret lives in Airflow.
SNOWFLAKE_CONN_ID = "snowflake_warehouse_elt"

default_args = {
    "owner": "bhaskar",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="warehouse_elt_daily",
    description="Generate one day of warehouse data and land it in S3",
    schedule=CronDataIntervalTimetable("@daily", timezone="UTC"),
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    # Don't auto-create a run for every missed day since start_date when the
    # DAG is unpaused. Historical days get loaded deliberately, with an
    # explicit backfill command, not as a side effect of flipping a toggle.
    catchup=False,
    # Cap parallel days during a backfill: each day's telemetry is ~185K
    # rows / ~55 MB, and this is all running on one laptop.
    max_active_runs=3,
    default_args=default_args,
    tags=["warehouse-elt"],
)
def warehouse_elt_daily():
    # "{{ ds }}" is a Jinja template: Airflow swaps in the run's date
    # (e.g. 2026-10-01) right before the command runs.
    generate_fulfillment = BashOperator(
        task_id="generate_fulfillment",
        bash_command=f"python {GENERATOR_DIR}/generate_fulfillment_events.py" + " --date {{ ds }}",
    )

    generate_telemetry = BashOperator(
        task_id="generate_telemetry",
        bash_command=f"python {GENERATOR_DIR}/generate_equipment_telemetry.py" + " --date {{ ds }}",
    )

    generate_exceptions = BashOperator(
        task_id="generate_exceptions",
        bash_command=f"python {GENERATOR_DIR}/generate_system_exceptions_rejects.py" + " --date {{ ds }}",
    )

    @task
    def upload_to_s3(ds: str | None = None) -> list[str]:
        # Reuse the exact same upload logic as the local CLI; only the way
        # the S3 client gets its credentials differs (Airflow Connection
        # here vs. ~/.aws profile locally).
        sys.path.insert(0, S3_LOADER_DIR)
        from upload_to_s3 import upload_day

        bucket = os.environ["WAREHOUSE_ELT_BUCKET"]  # from .env via docker-compose
        s3_client = S3Hook(aws_conn_id=AWS_CONN_ID).get_conn()
        return upload_day(ds, s3_client, bucket)

    # SQL lives in dags/sql/ as Jinja templates; Airflow fills in {{ ds }}
    # before sending it to Snowflake.
    load_to_snowflake = SQLExecuteQueryOperator(
        task_id="load_to_snowflake",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="sql/load_raw_day.sql",
        split_statements=True,   # the file holds several statements
        do_xcom_push=False,      # don't store COPY's result tables in Airflow
    )

    check_raw_counts = SQLCheckOperator(
        task_id="check_raw_counts",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="sql/check_raw_day.sql",
    )

    generate_fulfillment >> generate_exceptions
    uploaded = upload_to_s3()
    [generate_exceptions, generate_telemetry] >> uploaded
    uploaded >> load_to_snowflake >> check_raw_counts


warehouse_elt_daily()
