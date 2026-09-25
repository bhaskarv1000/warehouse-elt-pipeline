"""
Generates synthetic equipment_telemetry data for the warehouse ELT
pipeline project.

Grain: one row per asset per heartbeat interval (~20s). Unlike
case_fulfillment_events (a fixed sequence per case), this is an
indefinite stream: every asset reports its state on a clock for the
whole date range, whether or not anything "interesting" is happening.

Design: each asset runs its own state machine. It picks an
operational_state, holds it for a randomized duration, then
transitions to a new state — a machine doesn't flicker between
RUNNING and FAULTED every 20 seconds, it stays in a state for minutes
at a time. Readings emitted during that hold drift smoothly toward the
state's target temperature/vibration rather than jumping randomly.

No case_id or order_id here, by design — mirrors how real PLC/bot
firmware only knows its own motion and health, not what business
object it's touching. That link only exists via asset_id + time,
resolved downstream (later phase), not baked into this table.

Generates ONE day at a time (--date), same as the fulfillment generator,
so the Airflow DAG can call it once per daily run.

Known simplification: each run starts every asset fresh (IDLE, ambient
temperature, random battery) at 00:00. Carrying end-of-day state over
from the previous day would be more realistic, but it would make day D
depend on day D-1 having run, which breaks the "any day can be re-run
on its own" property the DAG relies on. So the midnight reset is a
deliberate trade: independence over continuity.
"""
import argparse
import random
from datetime import datetime, timedelta

from asset_registry import (
    BOT_IDS, CONVEYOR_IDS, LIFT_IDS, PALLETIZER_IDS,
    conveyor_zone, lift_zone, bot_zone,
)
from output_paths import partition_path, write_jsonl

# --- Config ---
STREAM = "equipment_telemetry"
HEARTBEAT_SECONDS = 20

# Deliberate anomaly: LIFT-03 runs hot for one shift on 2026-09-08,
# elevating its fault rate. Pinned to an absolute date (not "day 8 of the
# run") now that each run is a single day — it only fires when that date
# is generated. This is a natural byproduct of the state-duration model
# below, not bolted-on special logic — it just biases the same mechanics.
ANOMALY_ASSET_ID = "LIFT-03"
ANOMALY_START = datetime(2026, 9, 8, 14, 0)   # shift 2 start
ANOMALY_END = ANOMALY_START + timedelta(hours=8)

STATES = ["RUNNING", "IDLE", "BLOCKED", "STARVED", "FAULTED"]
BOT_ONLY_STATE = "CHARGING"

# (lo_minutes, hi_minutes) an asset holds a state before transitioning
STATE_DURATION_MINUTES = {
    "RUNNING": (5, 45),
    "IDLE": (1, 10),
    "BLOCKED": (1, 5),
    "STARVED": (1, 5),
    "FAULTED": (5, 30),
    "CHARGING": (20, 60),
}

# Weighted next-state choices. Not every transition is equally likely —
# RUNNING mostly goes back to IDLE, rarely straight to FAULTED.
TRANSITIONS = {
    "RUNNING": [("IDLE", 45), ("BLOCKED", 15), ("STARVED", 15), ("FAULTED", 5), ("CHARGING", 20)],
    "IDLE": [("RUNNING", 70), ("BLOCKED", 10), ("STARVED", 10), ("FAULTED", 5), ("CHARGING", 5)],
    "BLOCKED": [("RUNNING", 60), ("IDLE", 30), ("FAULTED", 10)],
    "STARVED": [("RUNNING", 60), ("IDLE", 35), ("FAULTED", 5)],
    "FAULTED": [("RUNNING", 80), ("IDLE", 20)],
    "CHARGING": [("RUNNING", 90), ("IDLE", 10)],
}

TARGET_TEMP_C = {
    "RUNNING": 60, "IDLE": 38, "BLOCKED": 40,
    "STARVED": 40, "FAULTED": 85, "CHARGING": 35,
}
VIBRATION_BASELINE = {
    "RUNNING": 0.35, "IDLE": 0.05, "BLOCKED": 0.08,
    "STARVED": 0.08, "FAULTED": 1.3, "CHARGING": 0.05,
}

AMBIENT_TEMP_C = 22.0
ANOMALY_TEMP_BOOST_C = 25.0


def build_roster() -> list[dict]:
    """One row per physical asset: id, type, and a fixed home zone.
    Reuses the exact ID pools from the fulfillment generator so this
    is the same physical fleet, not a lookalike set of fake IDs."""
    roster = []
    for bot_id in BOT_IDS:
        roster.append({"asset_id": bot_id, "asset_type": "AMR_BOT", "zone_id": bot_zone(bot_id)})
    for cnv_id in CONVEYOR_IDS:
        roster.append({"asset_id": cnv_id, "asset_type": "CONVEYOR", "zone_id": conveyor_zone(cnv_id)})
    for lift_id in LIFT_IDS:
        roster.append({"asset_id": lift_id, "asset_type": "LIFT", "zone_id": lift_zone(lift_id)})
    for plz_id in PALLETIZER_IDS:
        roster.append({"asset_id": plz_id, "asset_type": "PALLETIZER", "zone_id": "PALLETIZING"})
    return roster


def pick_next_state(current_state: str, asset_type: str, battery_pct: float | None) -> str:
    # Bots that are low on charge head to CHARGING regardless of the
    # weighted transition table — mirrors a real fleet management rule.
    if asset_type == "AMR_BOT" and battery_pct is not None and battery_pct < 15 and current_state != "CHARGING":
        return "CHARGING"

    options = TRANSITIONS[current_state]
    if asset_type != "AMR_BOT":
        options = [(s, w) for s, w in options if s != "CHARGING"]

    states, weights = zip(*options)
    return random.choices(states, weights=weights, k=1)[0]


def state_duration(state: str) -> timedelta:
    lo, hi = STATE_DURATION_MINUTES[state]
    return timedelta(minutes=random.uniform(lo, hi))


def step_temp(current_temp: float, target_temp: float) -> float:
    # Drift partway toward the target each tick instead of jumping —
    # this is what makes a temperature *curve* instead of noise.
    current_temp += (target_temp - current_temp) * 0.15 + random.uniform(-0.4, 0.4)
    return round(current_temp, 1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate one day of equipment_telemetry data."
    )
    parser.add_argument(
        "--date", required=True,
        help="Target date to generate, format YYYY-MM-DD (the Airflow run date)",
    )
    return parser.parse_args()


def simulate_asset(asset: dict, day_start: datetime) -> list[dict]:
    asset_id = asset["asset_id"]
    asset_type = asset["asset_type"]
    zone_id = asset["zone_id"]
    is_bot = asset_type == "AMR_BOT"

    current_state = "IDLE"
    current_temp = AMBIENT_TEMP_C
    battery_pct = round(random.uniform(40, 100), 1) if is_bot else None

    day_end = day_start + timedelta(days=1)
    date_tag = day_start.strftime("%Y%m%d")

    ts = day_start
    state_ends_at = ts + state_duration(current_state)

    rows = []
    telemetry_counter = 0

    while ts < day_end:
        in_anomaly = (
            asset_id == ANOMALY_ASSET_ID and ANOMALY_START <= ts <= ANOMALY_END
        )

        if ts >= state_ends_at:
            current_state = pick_next_state(current_state, asset_type, battery_pct)
            state_ends_at = ts + state_duration(current_state)
            # During the anomaly window, bias toward FAULTED harder than
            # the normal table allows — this is the "something is wrong"
            # signal a real monitoring dashboard would pick up on.
            if in_anomaly and current_state == "RUNNING" and random.random() < 0.35:
                current_state = "FAULTED"
                state_ends_at = ts + state_duration("FAULTED")

        target_temp = TARGET_TEMP_C[current_state]
        if in_anomaly:
            target_temp += ANOMALY_TEMP_BOOST_C
        current_temp = step_temp(current_temp, target_temp)

        if current_state == "RUNNING":
            speed_mps = round(random.uniform(0.5, 2.5), 2)
            motor_rpm = round(speed_mps * 400 + random.uniform(-15, 15), 1)
        else:
            speed_mps = 0.0
            motor_rpm = 0.0

        vibration_rms = round(
            max(0.0, VIBRATION_BASELINE[current_state] + random.uniform(-0.05, 0.05)), 3
        )

        if is_bot:
            if current_state == "CHARGING":
                battery_pct = min(100.0, battery_pct + random.uniform(0.8, 1.3))
            else:
                drain = 0.06 if current_state == "RUNNING" else 0.02
                battery_pct = max(0.0, battery_pct - random.uniform(drain * 0.5, drain * 1.5))
            battery_pct = round(battery_pct, 1)

        rows.append({
            # Date in the ID for the same reason as case_id: the counter
            # restarts at 0 every run, so without it every day would
            # produce tel_LIFT-03_0000000 again.
            "telemetry_id": f"tel_{asset_id}_{date_tag}_{telemetry_counter:04d}",
            "timestamp": ts.isoformat(),
            "asset_id": asset_id,
            "asset_type": asset_type,
            "zone_id": zone_id,
            "operational_state": current_state,
            "speed_mps": speed_mps,
            "motor_rpm": motor_rpm,
            "motor_temp_celsius": current_temp,
            "battery_pct": battery_pct,
            "vibration_rms": vibration_rms,
        })

        telemetry_counter += 1
        ts += timedelta(seconds=HEARTBEAT_SECONDS)

    return rows


def main():
    args = parse_args()
    day_start = datetime.strptime(args.date, "%Y-%m-%d")

    roster = build_roster()
    all_rows = []
    for asset in roster:
        all_rows.extend(simulate_asset(asset, day_start))

    out_path = partition_path(STREAM, args.date)
    write_jsonl(all_rows, out_path)

    print(f"Generated {len(all_rows)} telemetry rows across {len(roster)} assets for {args.date}")
    print(f"Wrote to {out_path}")


if __name__ == "__main__":
    main()
