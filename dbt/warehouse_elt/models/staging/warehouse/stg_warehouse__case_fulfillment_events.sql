select
    raw_data:event_id::varchar               as event_id,
    raw_data:case_id::varchar                as case_id,
    raw_data:event_type::varchar             as event_type,
    raw_data:event_timestamp::timestamp_ntz  as event_timestamp,
    raw_data:sku_id::varchar                 as sku_id,
    raw_data:source_location_id::varchar     as source_location_id,
    raw_data:target_location_id::varchar     as target_location_id,
    raw_data:assigned_asset_id::varchar      as assigned_asset_id,
    raw_data:order_id::varchar               as order_id,
    raw_data:order_line_id::varchar          as order_line_id,
    run_date,
    source_file,
    loaded_at
from {{ source('warehouse_raw', 'case_fulfillment_events') }}
