"""
Generates synthetic case_fulfillment_events data for the warehouse ELT
pipeline project.

Grain: one row per case per state transition. A single case produces
5 rows tracing its journey from induction to shipping.

Generates ONE day at a time (--date), not a date range. The original
version hardcoded a Sept 1-30 backfill; that's fine for a one-off
historical load, but Phase 3's Airflow DAG needs to call this once per
day going forward, so "which day" has to be a parameter, not a constant.
"""
import argparse
import random
from datetime import datetime, timedelta

from asset_registry import BOT_IDS, CONVEYOR_IDS, PALLETIZER_IDS
from output_paths import partition_path, write_jsonl

# --- Config ---
# 40,000 cases over the original 30-day backfill averaged ~1,333/day;
# keep that same daily volume so a single day here looks consistent
# with a day pulled out of the earlier backfill.
DEFAULT_NUM_CASES = 1_333
STREAM = "case_fulfillment_events"

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

SKU_IDS = [f"SKU-{random.randint(10000, 99999)}" for _ in range(300)]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate one day of case_fulfillment_events data."
    )
    parser.add_argument(
        "--date", required=True,
        help="Target date to generate, format YYYY-MM-DD (this becomes the "
             "case induction day, matching what an Airflow DAG's execution "
             "date would pass in)",
    )
    parser.add_argument(
        "--num-cases", type=int, default=DEFAULT_NUM_CASES,
        help=f"Number of cases to generate for this day (default {DEFAULT_NUM_CASES})",
    )
    return parser.parse_args()


def random_start_time(target_date: datetime) -> datetime:
    """Pick a random induction timestamp within target_date, weighted
    toward daytime hours (06:00-22:00) since inbound volume is heavier
    during shifts than overnight, even on a system that runs 24/7."""
    if random.random() < 0.8:
        hour = random.randint(6, 21)
    else:
        hour = random.choice([0, 1, 2, 3, 4, 5, 22, 23])

    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return target_date.replace(hour=hour, minute=minute, second=second)


def next_timestamp(current_ts: datetime, from_state: str, to_state: str) -> datetime:
    lo, hi = TRANSITION_GAPS_SECONDS[(from_state, to_state)]
    gap = random.randint(lo, hi)
    return current_ts + timedelta(seconds=gap)


def generate_order_assignments(num_cases: int, target_date: datetime) -> list[tuple[str, str]]:
    """Build a pool of orders, each with 1-4 line items, and return a
    shuffled list of (order_id, order_line_id) pairs — one per case —
    so that multiple cases can legitimately belong to the same order,
    instead of every case getting its own one-off order.

    Date is baked into order_id for the same reason as case_id: the
    counter restarts at 1 every run, so without it every day would
    reuse ORD-000001, ORD-000002, ... and orders from different days
    would silently merge when joined on order_id."""
    date_tag = target_date.strftime("%Y%m%d")
    assignments = []
    order_counter = 1
    while len(assignments) < num_cases:
        order_id = f"ORD-{date_tag}-{order_counter:06d}"
        num_lines = random.randint(1, 4)
        for line_num in range(1, num_lines + 1):
            assignments.append((order_id, f"{order_id}-L{line_num}"))
            if len(assignments) >= num_cases:
                break
        order_counter += 1
    random.shuffle(assignments)
    return assignments[:num_cases]


def build_case_events(
    case_index: int, order_id: str, order_line_id: str, target_date: datetime
) -> list[dict]:
    # Date baked into the ID, not just the timestamp: case_index resets to 0
    # every run, so without the date prefix, day 2's CASE-00000000 would
    # collide with day 1's — harmless for a single backfill, silently wrong
    # once this runs once per day for real.
    date_tag = target_date.strftime("%Y%m%d")
    case_id = f"CASE-{date_tag}-{case_index:06d}"
    sku_id = random.choice(SKU_IDS)

    buffer_loc = random.choice(BUFFER_LOCATIONS)
    induct_station = random.choice(INDUCT_STATIONS)
    palletizer = random.choice(PALLETIZER_IDS)
    staging_dock = random.choice(STAGING_DOCKS)
    bot_id = random.choice(BOT_IDS)
    conveyor_id = random.choice(CONVEYOR_IDS)

    ts = random_start_time(target_date)
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
            "event_id": f"evt_{date_tag}_{case_index:06d}_{i}",
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


def main():
    args = parse_args()
    target_date = datetime.strptime(args.date, "%Y-%m-%d")
    num_cases = args.num_cases

    order_assignments = generate_order_assignments(num_cases, target_date)

    all_rows = []
    for i in range(num_cases):
        order_id, order_line_id = order_assignments[i]
        all_rows.extend(build_case_events(i, order_id, order_line_id, target_date))

    out_path = partition_path(STREAM, args.date)
    write_jsonl(all_rows, out_path)

    print(f"Generated {len(all_rows)} events across {num_cases} cases for {args.date}")
    print(f"Wrote to {out_path}")


if __name__ == "__main__":
    main()
