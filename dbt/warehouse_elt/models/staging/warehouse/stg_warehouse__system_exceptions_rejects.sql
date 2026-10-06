select
    raw_data:exception_id::varchar           as exception_id,
    raw_data:case_id::varchar                as case_id,  -- null for VISION_NO_READ
    raw_data:asset_id::varchar               as asset_id,
    raw_data:zone_id::varchar                as zone_id,
    raw_data:reject_code::varchar            as reject_code,
    raw_data:reject_category::varchar        as reject_category,
    raw_data:severity::varchar               as severity,
    raw_data:action_taken::varchar           as action_taken,
    raw_data:timestamp::timestamp_ntz        as exception_timestamp,
    run_date,
    source_file,
    loaded_at
from {{ source('warehouse_raw', 'system_exceptions_rejects') }}
