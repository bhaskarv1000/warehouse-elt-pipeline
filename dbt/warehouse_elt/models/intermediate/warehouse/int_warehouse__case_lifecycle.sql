-- Grain: one row per case.
-- Staging has one row per case per state change (up to 5 rows per case).
-- This model collapses them into a single row so later joins don't multiply rows.
select
    case_id,
    max(sku_id)                                                              as sku_id,
    max(order_id)                                                            as order_id,       -- only set from retrieval onward
    min(case when event_type = 'CASE_INDUCTED'         then event_timestamp end) as inducted_at,
    min(case when event_type = 'STORED_IN_BUFFER'      then event_timestamp end) as stored_at,
    min(case when event_type = 'RETRIEVED_FROM_BUFFER' then event_timestamp end) as retrieved_at,
    min(case when event_type = 'PALLET_BUILT'          then event_timestamp end) as pallet_built_at,
    min(case when event_type = 'SHIPPED'               then event_timestamp end) as shipped_at,
    count(*)                                                                 as event_count,
    max(event_type = 'SHIPPED')                                              as is_shipped
from {{ ref('stg_warehouse__case_fulfillment_events') }}
group by case_id
