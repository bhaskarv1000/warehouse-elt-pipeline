"""
Lands the generated raw event files in S3, split into daily partitions
by event date, so the S3 layout matches exactly what the Phase 3 Airflow
DAG will produce for every future day (generate one day -> upload one
partition -> COPY INTO Snowflake). Running this against the Sept 1-30
backfill now means the partitioning logic gets built once, not once for
the backfill and again later for daily runs.

Two things this script normalizes on the way in, worth knowing:

1. Format: the generators write case_fulfillment_events as a pretty
   JSON array, but equipment_telemetry and system_exceptions_rejects as
   newline-delimited JSON (NDJSON). Every partition uploaded here is
   NDJSON regardless of the source format -- that's what Snowflake's
   COPY INTO expects most naturally, and it keeps the three streams
   consistent in the landing zone even though they aren't consistent
   on disk.
2. Field names: case_fulfillment_events dates its rows with
   "event_timestamp"; the other two streams use "timestamp". That
   mismatch is real (see STREAMS below), not a bug -- it's the kind of
   inconsistency a landing zone just captures as-is; reconciling field
   names is transformation work that belongs in dbt staging models
   (Phase 5), not here.

Only NDJSON is uploaded -- the CSV outputs are a convenience for
eyeballing data locally and were never meant to be a second copy of the
same rows sitting in S3.
"""
import json
import os
from collections import defaultdict

import boto3

# --- Config ---
BUCKET_NAME = os.environ.get("WAREHOUSE_ELT_BUCKET", "")
AWS_PROFILE = os.environ.get("WAREHOUSE_ELT_AWS_PROFILE", "warehouse-elt")
OUTPUT_DIR = "output"

# One entry per event stream: where the generator left its file, whether
# that file is one JSON array or NDJSON, and which field holds the date
# each row gets partitioned on.
STREAMS = {
    "case_fulfillment_events": {
        "source_file": f"{OUTPUT_DIR}/case_fulfillment_events.json",
        "source_format": "json_array",
        "date_field": "event_timestamp",
    },
    "equipment_telemetry": {
        "source_file": f"{OUTPUT_DIR}/equipment_telemetry.jsonl",
        "source_format": "jsonl",
        "date_field": "timestamp",
    },
    "system_exceptions_rejects": {
        "source_file": f"{OUTPUT_DIR}/system_exceptions_rejects.jsonl",
        "source_format": "jsonl",
        "date_field": "timestamp",
    },
}


def read_rows(path: str, source_format: str) -> list[dict]:
    with open(path) as f:
        if source_format == "json_array":
            return json.load(f)
        return [json.loads(line) for line in f if line.strip()]


def group_by_date(rows: list[dict], date_field: str) -> dict[str, list[dict]]:
    by_date = defaultdict(list)
    for row in rows:
        event_date = row[date_field][:10]  # "2026-09-09T08:36:31" -> "2026-09-09"
        by_date[event_date].append(row)
    return by_date


def upload_partition(s3, stream_name: str, event_date: str, rows: list[dict]) -> str:
    body = "\n".join(json.dumps(row) for row in rows)
    key = f"raw/{stream_name}/dt={event_date}/{stream_name}.jsonl"
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=body.encode("utf-8"))
    return key


def main() -> None:
    if not BUCKET_NAME:
        raise SystemExit(
            "Set WAREHOUSE_ELT_BUCKET to your S3 bucket name before running, "
            "e.g. export WAREHOUSE_ELT_BUCKET=your-bucket-name"
        )

    session = boto3.Session(profile_name=AWS_PROFILE)
    s3 = session.client("s3")

    for stream_name, cfg in STREAMS.items():
        if not os.path.exists(cfg["source_file"]):
            print(f"Skipping {stream_name}: {cfg['source_file']} not found (regenerate it first)")
            continue

        rows = read_rows(cfg["source_file"], cfg["source_format"])
        by_date = group_by_date(rows, cfg["date_field"])

        for event_date in sorted(by_date):
            key = upload_partition(s3, stream_name, event_date, by_date[event_date])
            print(f"  uploaded s3://{BUCKET_NAME}/{key} ({len(by_date[event_date])} rows)")

        print(f"{stream_name}: {len(by_date)} daily partitions, {len(rows)} rows total\n")


if __name__ == "__main__":
    main()
