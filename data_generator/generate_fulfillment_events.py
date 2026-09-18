"""
Generates synthetic case_fulfillment_events data for the warehouse ELT
pipeline project.

Grain: one row per case per state transition. A single case produces
5 rows tracing its journey from induction to shipping.
"""
import csv
import json
import random
from datetime import datetime, timedelta

# --- Config ---
NUM_CASES = 40_000
START_DATE = datetime(2026, 9, 1)
END_DATE = datetime(2026, 9, 30)
OUTPUT_DIR = "output"

STATE_SEQUENCE = [
    "CASE_INDUCTED",
    "STORED_IN_BUFFER",
    "RETRIEVED_FROM_BUFFER",
    "PALLET_BUILT",
    "SHIPPED",
]

# Realistic gap ranges (in seconds) between consecutive states.
# Dwell time in the buffer is the big one — a case might sit for
# minutes or the better part of a day before it's picked for an order.
TRANSITION_GAPS_SECONDS = {
    ("CASE_INDUCTED", "STORED_IN_BUFFER"): (20, 90),
    ("STORED_IN_BUFFER", "RETRIEVED_FROM_BUFFER"): (300, 64_800),   # 5 min - 18 hr
    ("RETRIEVED_FROM_BUFFER", "PALLET_BUILT"): (60, 300),
    ("PALLET_BUILT", "SHIPPED"): (300, 14_400),                     # 5 min - 4 hr
}

INDUCT_STATIONS = [f"INDUCT-{i:02d}" for i in range(1, 6)]
BUFFER_LOCATIONS = [f"BUFFER-{a}{n:02d}" for a in "ABCD" for n in range(1, 21)]
STAGING_DOCKS = [f"STAGING-DOCK-{i}" for i in range(1, 13)]

BOT_IDS = [f"BOT-{i:03d}" for i in range(1, 21)]
CONVEYOR_IDS = [f"CNV-Z{z}-{n:02d}" for z in range(1, 4) for n in range(1, 6)]
PALLETIZER_IDS = [f"PLZ-{i:02d}" for i in range(1, 4)]

SKU_IDS = [f"SKU-{random.randint(10000, 99999)}" for _ in range(300)]


def random_start_time() -> datetime:
    """Pick a random induction timestamp within the date range, weighted
    toward daytime hours (06:00-22:00) since inbound volume is heavier
    during shifts than overnight, even on a system that runs 24/7."""
    day_offset = random.randint(0, (END_DATE - START_DATE).days - 1)
    day = START_DATE + timedelta(days=day_offset)

    if random.random() < 0.8:
        hour = random.randint(6, 21)
    else:
        hour = random.choice([0, 1, 2, 3, 4, 5, 22, 23])

    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return day.replace(hour=hour, minute=minute, second=second)


def next_timestamp(current_ts: datetime, from_state: str, to_state: str) -> datetime:
    lo, hi = TRANSITION_GAPS_SECONDS[(from_state, to_state)]
    gap = random.randint(lo, hi)
    return current_ts + timedelta(seconds=gap)


def generate_order_assignments(num_cases: int) -> list[tuple[str, str]]:
    """Build a pool of orders, each with 1-4 line items, and return a
    shuffled list of (order_id, order_line_id) pairs — one per case —
    so that multiple cases can legitimately belong to the same order,
    instead of every case getting its own one-off order."""
    assignments = []
    order_counter = 1
    while len(assignments) < num_cases:
        order_id = f"ORD-{order_counter:06d}"
        num_lines = random.randint(1, 4)
        for line_num in range(1, num_lines + 1):
            assignments.append((order_id, f"{order_id}-L{line_num}"))
            if len(assignments) >= num_cases:
                break
        order_counter += 1
    random.shuffle(assignments)
    return assignments[:num_cases]


def build_case_events(case_index: int, order_id: str, order_line_id: str) -> list[dict]:
    case_id = f"CASE-{case_index:08d}"
    sku_id = random.choice(SKU_IDS)

    buffer_loc = random.choice(BUFFER_LOCATIONS)
    induct_station = random.choice(INDUCT_STATIONS)
    palletizer = random.choice(PALLETIZER_IDS)
    staging_dock = random.choice(STAGING_DOCKS)
    bot_id = random.choice(BOT_IDS)
    conveyor_id = random.choice(CONVEYOR_IDS)

    ts = random_start_time()
    events = []

    for i, state in enumerate(STATE_SEQUENCE):
        if i > 0:
            ts = next_timestamp(ts, STATE_SEQUENCE[i - 1], state)

        # order_id/order_line_id are null until the case is actually
        # retrieved for an order — mirrors the fact that inventory
        # sitting in storage isn't attached to a customer order yet.
        has_order_context = state in (
            "RETRIEVED_FROM_BUFFER", "PALLET_BUILT", "SHIPPED"
        )

        if state == "CASE_INDUCTED":
            source, target, asset = induct_station, buffer_loc, conveyor_id
        elif state == "STORED_IN_BUFFER":
            source, target, asset = buffer_loc, buffer_loc, bot_id
        elif state == "RETRIEVED_FROM_BUFFER":
            source, target, asset = buffer_loc, palletizer, bot_id
        elif state == "PALLET_BUILT":
            source, target, asset = palletizer, staging_dock, palletizer
        else:  # SHIPPED
            source, target, asset = staging_dock, None, None

        events.append({
            "event_id": f"evt_{case_index:08d}_{i}",
            "event_timestamp": ts.isoformat(),
            "case_id": case_id,
            "order_id": order_id if has_order_context else None,
            "order_line_id": order_line_id if has_order_context else None,
            "sku_id": sku_id,
            "event_type": state,
            "source_location_id": source,
            "target_location_id": target,
            "assigned_asset_id": asset,
        })

    return events


def write_json(rows: list[dict], path: str) -> None:
    with open(path, "w") as f:
        json.dump(rows, f, indent=2)


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

    order_assignments = generate_order_assignments(NUM_CASES)

    all_rows = []
    for i in range(NUM_CASES):
        order_id, order_line_id = order_assignments[i]
        all_rows.extend(build_case_events(i, order_id, order_line_id))

    write_json(all_rows, f"{OUTPUT_DIR}/case_fulfillment_events.json")
    write_csv(all_rows, f"{OUTPUT_DIR}/case_fulfillment_events.csv")

    print(f"Generated {len(all_rows)} events across {NUM_CASES} cases")
    print(f"Wrote to {OUTPUT_DIR}/case_fulfillment_events.{{json,csv}}")


if __name__ == "__main__":
    main()
