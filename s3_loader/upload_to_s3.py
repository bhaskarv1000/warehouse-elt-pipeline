"""
Lands one run date's generated files in S3.

Layout (defined once in data_generator/output_paths.py):
    output/<stream>/dt=<run_date>/<stream>.jsonl
      -> s3://<bucket>/raw/<stream>/dt=<run_date>/<stream>.jsonl

Partitioned by RUN date, not event date. The earlier version of this
script re-split rows by the date in each event's timestamp. That worked
for a one-shot 30-day backfill, but breaks for daily runs: a case
inducted on Oct 1 can ship on Oct 2, so the Oct 1 run would write rows
into dt=2026-10-02, and the Oct 2 run would then overwrite that same
object and silently drop them. With run-date partitions each run owns
exactly one object per stream, and re-running a day is a clean
overwrite (idempotent). Event time is still on every row; dbt handles it.

No format conversion happens here any more either: every generator now
writes NDJSON directly, so this script just copies files up. (The
event_timestamp vs timestamp field-name mismatch between streams is
still captured as-is; reconciling it is dbt staging work, Phase 5.)

Two ways to call it:
  - CLI, locally:  python s3_loader/upload_to_s3.py --date 2026-10-01
    (credentials from the ~/.aws profile named in WAREHOUSE_ELT_AWS_PROFILE)
  - From Airflow:  upload_day(run_date, s3_client, bucket), where the DAG
    builds s3_client from an Airflow AWS Connection instead of a local
    profile, so no credentials live in the code or the container image.
"""
import argparse
import os
import sys
from pathlib import Path

# Reuse the generators' path definitions so the layout lives in one place.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "data_generator"))
from output_paths import STREAMS, partition_path, s3_key  # noqa: E402


def upload_day(run_date: str, s3_client, bucket: str) -> list[str]:
    """Upload all three streams' partitions for one run date. Fails loudly
    if any file is missing — a half-uploaded day is worse than a failed task,
    because Airflow will retry a failed task but won't notice a partial one."""
    missing = [str(partition_path(s, run_date)) for s in STREAMS
               if not partition_path(s, run_date).exists()]
    if missing:
        raise FileNotFoundError(
            f"Missing generated output for {run_date}: {missing}. "
            f"Run the generators for this date first."
        )

    uploaded = []
    for stream in STREAMS:
        local = partition_path(stream, run_date)
        key = s3_key(stream, run_date)
        s3_client.upload_file(str(local), bucket, key)
        size_kb = local.stat().st_size / 1024
        print(f"  uploaded s3://{bucket}/{key} ({size_kb:,.0f} KB)")
        uploaded.append(key)
    return uploaded


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload one run date's output to S3.")
    parser.add_argument("--date", required=True, help="Run date, YYYY-MM-DD")
    args = parser.parse_args()

    bucket = os.environ.get("WAREHOUSE_ELT_BUCKET", "")
    if not bucket:
        raise SystemExit(
            "Set WAREHOUSE_ELT_BUCKET to your S3 bucket name before running, "
            "e.g. export WAREHOUSE_ELT_BUCKET=your-bucket-name"
        )

    import boto3  # imported here so upload_day() is usable without it at import time
    profile = os.environ.get("WAREHOUSE_ELT_AWS_PROFILE", "warehouse-elt")
    s3 = boto3.Session(profile_name=profile).client("s3")

    upload_day(args.date, s3, bucket)


if __name__ == "__main__":
    main()
