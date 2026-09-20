"""
Shared asset ID pools used across every event-stream generator, so the
same physical asset (e.g. BOT-001) shows up consistently in
case_fulfillment_events, equipment_telemetry, and (later)
system_exceptions_rejects. Keeping this in one place is what makes a
join between streams on asset_id actually mean something, instead of
each generator inventing its own disconnected IDs.
"""

BOT_IDS = [f"BOT-{i:03d}" for i in range(1, 21)]
CONVEYOR_IDS = [f"CNV-Z{z}-{n:02d}" for z in range(1, 4) for n in range(1, 6)]
LIFT_IDS = [f"LIFT-{i:02d}" for i in range(1, 6)]
PALLETIZER_IDS = [f"PLZ-{i:02d}" for i in range(1, 4)]
SCANNER_IDS = [f"SCANNER-{i:02d}" for i in range(1, 9)]

# Bots roam, but v1 keeps it simple: each bot gets one fixed "home zone"
# it reports from, rather than simulating movement between zones.
# Flagging this as a known simplification, same as the earlier
# order-per-case one — worth revisiting later, not worth the complexity now.
ZONES = ["Z1", "Z2", "Z3", "BUFFER", "PALLETIZING"]


def conveyor_zone(conveyor_id: str) -> str:
    # "CNV-Z1-04" -> "Z1"
    return conveyor_id.split("-")[1]


def lift_zone(lift_id: str) -> str:
    # "LIFT-02" -> "Z2"
    idx = int(lift_id.split("-")[1])
    return f"Z{idx}"


def bot_zone(bot_id: str) -> str:
    # Deterministic, not random — so the same bot maps to the same zone
    # no matter which generator script asks. Still the "fixed home zone"
    # simplification noted above (real bots roam); this just makes the
    # simplification consistent across every table that references it.
    idx = int(bot_id.split("-")[1])
    return ZONES[idx % len(ZONES)]


def scanner_zone(scanner_id: str) -> str:
    idx = int(scanner_id.split("-")[1])
    return ZONES[idx % len(ZONES)]
