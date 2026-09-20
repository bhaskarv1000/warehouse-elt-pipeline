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
"""
import csv
import json
import random
from datetime import datetime, timedelta

from asset_registry import CONVEYOR_IDS, BOT_IDS, SCANNER_IDS, conveyor_zone, bot_zone, scanner_zone

START_DATE = datetime(2026, 9, 1)
END_DATE = datetime(2026, 9, 30)
OUTPUT_DIR = "output"
FULFILLMENT_CSV = f"{OUTPUT_DIR}/case_fulfillment_events.csv"

TARGET_REJECT_RATE = 0.02  # ~2% of cases produce a reject incident somewhere

# Same anomaly window as equipment_telemetry's LIFT-03 event — knock-on
# jams near the overheating lift, so the story is consistent across
# streams instead of each table inventing its own unrelated incident.
ANOMALY_START = START_DATE + timedelta(days=7, hours=14)
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


def load_case_ids(path: str) -> list[str]:
    with open(path) as f:
        reader = csv.DictReader(f)
        return sorted({row["case_id"] for row in reader})


def random_timestamp() -> datetime:
    day_offset = random.randint(0, (END_DATE - START_DATE).days - 1)
    day = START_DATE + timedelta(days=day_offset)
    hour = random.randint(6, 21) if random.random() < 0.8 else random.choice([0, 1, 2, 3, 4, 5, 22, 23])
    return day.replace(hour=hour, minute=random.randint(0, 59), second=random.randint(0, 59))


def pick_weighted(options: list[tuple[str, int]]) -> str:
    values, weights = zip(*options)
    return random.choices(values, weights=weights, k=1)[0]


def build_reject(category: str, case_ids: list[str], forced_asset: str | None = None) -> dict:
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
        "exception_id": f"exc_{random.randint(10_000_000, 99_999_999)}",
        "timestamp": random_timestamp().isoformat(),
        "asset_id": asset_id,
        "zone_id": zone_id,
        "case_id": case_id,
        "reject_category": category,
        "reject_code": random.choice(REJECT_CODES[category]),
        "severity": severity,
        "action_taken": ACTION_BY_SEVERITY[severity],
    }


def build_anomaly_jam(case_ids: list[str]) -> dict:
    row = build_reject("CONVEYOR_JAM", case_ids=case_ids, forced_asset=random.choice(
        [c for c in CONVEYOR_IDS if conveyor_zone(c) == "Z3"]
    ))
    anomaly_span = (ANOMALY_END - ANOMALY_START).total_seconds()
    row["timestamp"] = (ANOMALY_START + timedelta(seconds=random.uniform(0, anomaly_span))).isoformat()
    row["severity"] = "ESTOP_STOP"
    row["action_taken"] = ACTION_BY_SEVERITY["ESTOP_STOP"]
    return row


def write_jsonl(rows: list[dict], path: str) -> None:
    with open(path, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def write_csv(rows: list[dict], path: str) -> None:
    if not rows:
        return
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def main():
    import os
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    case_ids = load_case_ids(FULFILLMENT_CSV)
    num_rejects = round(len(case_ids) * TARGET_REJECT_RATE)

    rows = []
    for _ in range(num_rejects):
        category = pick_weighted(REJECT_CATEGORIES)
        rows.append(build_reject(category, case_ids))

    for _ in range(ANOMALY_EXTRA_JAMS):
        rows.append(build_anomaly_jam(case_ids))

    rows.sort(key=lambda r: r["timestamp"])

    write_jsonl(rows, f"{OUTPUT_DIR}/system_exceptions_rejects.jsonl")
    write_csv(rows, f"{OUTPUT_DIR}/system_exceptions_rejects.csv")

    print(f"Generated {len(rows)} reject incidents ({ANOMALY_EXTRA_JAMS} from the anomaly window)")


if __name__ == "__main__":
    main()
