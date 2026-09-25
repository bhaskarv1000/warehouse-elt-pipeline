"""
One place that defines where a generator writes a given day's output,
so all three generators, the S3 uploader, and the Airflow DAG agree on
the layout without each one hardcoding its own path.

Local layout mirrors the S3 landing zone exactly:
    output/<stream>/dt=<YYYY-MM-DD>/<stream>.jsonl
                     -> s3://<bucket>/raw/<stream>/dt=<YYYY-MM-DD>/<stream>.jsonl

`dt` is the RUN date (the --date the generator was called with, i.e. the
Airflow logical date), not the date each event happened. A case inducted
late on Oct 1 can ship on Oct 2, but it still lives in the dt=2026-10-01
partition, because that's the run that produced it. That way each daily
run owns exactly one partition per stream, and re-running a day just
overwrites that one partition instead of clobbering a neighbour's rows.
The true event time is still on every row; sorting by it is dbt's job.

The output root defaults to <repo>/output, resolved from this file's
location (not the current working directory), so it behaves the same
whether you run a script from the repo root, from data_generator/, or
from inside the Airflow container. Override with WAREHOUSE_ELT_OUTPUT_DIR.
"""
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_ROOT = Path(os.environ.get("WAREHOUSE_ELT_OUTPUT_DIR", REPO_ROOT / "output"))

STREAMS = (
    "case_fulfillment_events",
    "equipment_telemetry",
    "system_exceptions_rejects",
)


def partition_path(stream: str, run_date: str) -> Path:
    """Local path for one stream's output for one run date (YYYY-MM-DD)."""
    return OUTPUT_ROOT / stream / f"dt={run_date}" / f"{stream}.jsonl"


def s3_key(stream: str, run_date: str) -> str:
    """Matching S3 object key for the same partition."""
    return f"raw/{stream}/dt={run_date}/{stream}.jsonl"


def write_jsonl(rows: list[dict], path: Path) -> None:
    """Newline-delimited JSON, one row per line — the single format every
    stream lands in. (No more JSON arrays or CSV copies: NDJSON is what
    Snowflake's COPY INTO reads most naturally, and `head -n 5` on the
    file is enough for eyeballing.)"""
    import json
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
