"""
Generates synthetic system_exceptions_rejects data for the warehouse
ELT pipeline project.

Grain: one row per discrete fault/reject incident (not a periodic
stream like telemetry, not a per-case sequence like fulfillment).

Depends on case_fulfillment_events already existing — this script reads
real case_id values from that output so rejects reference cases that
actually exist elsewhere in the dataset, instead of inventing IDs that
would never join to anything. That's also a preview of what an Airflow
DAG will encode later as a task dependency: generate cases, then
generate rejects.

Generates ONE day at a time (--date). It reads the fulfillment output
for that SAME date, so it must run after the fulfillment generator for
that day — in the DAG that's an explicit task dependency:
generate_fulfillment >> generate_exceptions.
"""
import argparse
import json
import random
from datetime import datetime, timedelta

from asset_registry import CONVEYOR_IDS, BOT_IDS, SCANNER_IDS, conveyor_zone, bot_zone, scanner_zone
from output_paths import partition_path, write_jsonl

STREAM = "system_exceptions_rejects"
FULFILLMENT_STREAM = "case_fulfillment_events"

TARGET_REJECT_RATE = 0.02  # ~2% of cases produce a reject incident somewhere

# Same anomaly window as equipment_telemetry's LIFT-03 event — knock-on
# jams near the overheating lift, so the story is consistent across
# streams instead of each table inventing its own unrelated incident.
# Absolute date: only injected when --date is 2026-09-08.
ANOMALY_START = datetime(2026, 9, 8, 14, 0)
ANOMALY_END = ANOMALY_START + timedelta(hours=8)
ANOMALY_EXTRA_JAMS = 20

REJECT_CATEGORIES = [
    ("VISION_NO_READ", 35),
    ("DIMENSION_OVERHANG", 20),
    ("WEIGHT_MISMATCH", 15),
    ("ROBOTIC_MISPICK", 15),
    ("CONVEYOR_JAM", 15),
]

REJECT_CODES = {
    "VISION_NO_READ": ["ERR_CAM_302", "ERR_CAM_318", "ERR_LABEL_UNREADABLE"],
    "DIMENSION_OVERHANG": ["ERR_DIM_OVER_TOLERANCE", "ERR_DIM_UNDER_TOLERANCE"],
    "WEIGHT_MISMATCH": ["ERR_WEIGHT_HIGH", "ERR_WEIGHT_LOW"],
    "ROBOTIC_MISPICK": ["ERR_BOT_GRIP_FAIL", "ERR_BOT_DROP"],
    "CONVEYOR_JAM": ["ERR_CNV_JAM", "ERR_CNV_STALL"],
}

SEVERITY_WEIGHTS = [("WARNING", 50), ("ERROR", 40), ("ESTOP_STOP", 10)]

ACTION_BY_SEVERITY = {
    "WARNING": "AUTO_RETRY_PASS",
    "ERROR": "REROUTED_TO_MANUAL_REWORK",
    "ESTOP_STOP": "PURGED_TO_REJECT_LANE",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate one day of system_exceptions_rejects data."
    )
    parser.add_argument(
        "--date", required=True,
        help="Target date to generate, format YYYY-MM-DD (the Airflow run date). "
             "The fulfillment output for this same date must already exist.",
    )
    return parser.parse_args()


def load_case_ids(run_date: str) -> list[str]:
    path = partition_path(FULFILLMENT_STREAM, run_date)
    if not path.exists():
        raise SystemExit(
            f"No fulfillment output for {run_date} at {path} — "
            f"run generate_fulfillment_events.py --date {run_date} first."
        )
    with open(path) as f:
        return sorted({json.loads(line)["case_id"] for line in f if line.strip()})


def random_timestamp(day: datetime) -> datetime:
    hour = random.randint(6, 21) if random.random() < 0.8 else random.choice([0, 1, 2, 3, 4, 5, 22, 23])
    return day.replace(hour=hour, minute=random.randint(0, 59), second=random.randint(0, 59))


def pick_weighted(options: list[tuple[str, int]]) -> str:
    values, weights = zip(*options)
    return random.choices(values, weights=weights, k=1)[0]


def build_reject(
    category: str, case_ids: list[str], day: datetime, forced_asset: str | None = None
) -> dict:
    severity = pick_weighted(SEVERITY_WEIGHTS)

    if category == "CONVEYOR_JAM":
        asset_id = forced_asset or random.choice(CONVEYOR_IDS)
        zone_id = conveyor_zone(asset_id)
    elif category == "ROBOTIC_MISPICK":
        asset_id = random.choice(BOT_IDS)
        zone_id = bot_zone(asset_id)
    else:
        asset_id = random.choice(SCANNER_IDS)
        zone_id = scanner_zone(asset_id)

    # VISION_NO_READ means the barcode genuinely couldn't be read —
    # there's no honest way to know which case it was.
    case_id = None if category == "VISION_NO_READ" else random.choice(case_ids)

    return {
        "exception_id": None,  # assigned in main() once rows are sorted
        "timestamp": random_timestamp(day).isoformat(),
        "asset_id": asset_id,
        "zone_id": zone_id,
        "case_id": case_id,
        "reject_category": category,
        "reject_code": random.choice(REJECT_CODES[category]),
        "severity": severity,
        "action_taken": ACTION_BY_SEVERITY[severity],
    }


def build_anomaly_jam(case_ids: list[str], day: datetime) -> dict:
    row = build_reject("CONVEYOR_JAM", case_ids=case_ids, day=day, forced_asset=random.choice(
        [c for c in CONVEYOR_IDS if conveyor_zone(c) == "Z3"]
    ))
    anomaly_span = (ANOMALY_END - ANOMALY_START).total_seconds()
    row["timestamp"] = (ANOMALY_START + timedelta(seconds=random.uniform(0, anomaly_span))).isoformat()
    row["severity"] = "ESTOP_STOP"
    row["action_taken"] = ACTION_BY_SEVERITY["ESTOP_STOP"]
    return row


def main():
    args = parse_args()
    day = datetime.strptime(args.date, "%Y-%m-%d")

    case_ids = load_case_ids(args.date)
    num_rejects = round(len(case_ids) * TARGET_REJECT_RATE)

    rows = []
    for _ in range(num_rejects):
        category = pick_weighted(REJECT_CATEGORIES)
        rows.append(build_reject(category, case_ids, day))

    is_anomaly_day = day.date() == ANOMALY_START.date()
    if is_anomaly_day:
        for _ in range(ANOMALY_EXTRA_JAMS):
            rows.append(build_anomaly_jam(case_ids, day))

    rows.sort(key=lambda r: r["timestamp"])

    # Sequential, date-stamped IDs instead of random 8-digit numbers: a
    # random ID can collide (within a day or across days), and a
    # sequential one is also deterministic on re-runs of the same day.
    date_tag = day.strftime("%Y%m%d")
    for i, row in enumerate(rows):
        row["exception_id"] = f"exc_{date_tag}_{i:04d}"

    out_path = partition_path(STREAM, args.date)
    write_jsonl(rows, out_path)

    extra = f" ({ANOMALY_EXTRA_JAMS} from the LIFT-03 anomaly window)" if is_anomaly_day else ""
    print(f"Generated {len(rows)} reject incidents for {args.date}{extra}")
    print(f"Wrote to {out_path}")


if __name__ == "__main__":
    main()
