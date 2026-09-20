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
"""
import csv
import json
import random
from datetime import datetime, timedelta

from asset_registry import (
    BOT_IDS, CONVEYOR_IDS, LIFT_IDS, PALLETIZER_IDS,
    conveyor_zone, lift_zone, bot_zone,
)

# --- Config ---
START_DATE = datetime(2026, 9, 1)
END_DATE = datetime(2026, 9, 30)
HEARTBEAT_SECONDS = 20
OUTPUT_DIR = "output"

# Deliberate anomaly: LIFT-03 runs hot for one shift on day 8, elevating
# its fault rate. This is a natural byproduct of the state-duration model
# below, not bolted-on special logic — it just biases the same mechanics.
ANOMALY_ASSET_ID = "LIFT-03"
ANOMALY_START = START_DATE + timedelta(days=7, hours=14)   # day 8, shift 2 start
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


def simulate_asset(asset: dict) -> list[dict]:
    asset_id = asset["asset_id"]
    asset_type = asset["asset_type"]
    zone_id = asset["zone_id"]
    is_bot = asset_type == "AMR_BOT"

    current_state = "IDLE"
    current_temp = AMBIENT_TEMP_C
    battery_pct = round(random.uniform(40, 100), 1) if is_bot else None

    ts = START_DATE
    state_ends_at = ts + state_duration(current_state)

    rows = []
    telemetry_counter = 0

    while ts < END_DATE:
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
            "telemetry_id": f"tel_{asset_id}_{telemetry_counter:07d}",
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


def write_jsonl(rows: list[dict], path: str) -> None:
    # Newline-delimited JSON, not a single indented array: at millions of
    # rows, pretty-printing a JSON array bloats the file ~3-4x for no
    # reason and can't be streamed. JSONL is also closer to what you'd
    # actually land in S3 for a high-volume stream like this.
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

    roster = build_roster()
    all_rows = []
    for asset in roster:
        all_rows.extend(simulate_asset(asset))

    write_jsonl(all_rows, f"{OUTPUT_DIR}/equipment_telemetry.jsonl")
    write_csv(all_rows, f"{OUTPUT_DIR}/equipment_telemetry.csv")

    print(f"Generated {len(all_rows)} telemetry rows across {len(roster)} assets")
    print(f"Wrote to {OUTPUT_DIR}/equipment_telemetry.{{json,csv}}")


if __name__ == "__main__":
    main()
