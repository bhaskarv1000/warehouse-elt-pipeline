select
    raw_data:telemetry_id::varchar           as telemetry_id,
    raw_data:asset_id::varchar               as asset_id,
    raw_data:asset_type::varchar             as asset_type,
    raw_data:zone_id::varchar                as zone_id,
    raw_data:operational_state::varchar      as operational_state,
    raw_data:timestamp::timestamp_ntz        as reading_timestamp,
    raw_data:motor_rpm::float                as motor_rpm,
    raw_data:motor_temp_celsius::float       as motor_temp_celsius,
    raw_data:speed_mps::float                as speed_mps,
    raw_data:vibration_rms::float            as vibration_rms,
    raw_data:battery_pct::float              as battery_pct,  -- null for non-bot assets
    run_date,
    source_file,
    loaded_at
from {{ source('warehouse_raw', 'equipment_telemetry') }}
