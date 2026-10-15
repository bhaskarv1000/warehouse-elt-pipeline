-- Grain: one row per exception (same as staging).
-- LEFT join because VISION_NO_READ rejects have no case_id: an inner join
-- would silently drop them and understate the reject rate.
with exceptions as (
    select * from {{ ref('stg_warehouse__system_exceptions_rejects') }}
),

cases as (
    select * from {{ ref('int_warehouse__case_lifecycle') }}
)

select
    e.exception_id,
    e.case_id,
    e.asset_id,
    e.zone_id,
    e.reject_code,
    e.reject_category,
    e.severity,
    e.action_taken,
    e.exception_timestamp,
    e.run_date,

    -- context from the case (null when the exception has no case)
    c.sku_id,
    c.order_id,
    c.inducted_at,
    c.shipped_at,
    c.is_shipped,

    e.case_id is not null                                                   as has_case,

    -- furthest stage the case had reached when the exception happened
    case
        when e.case_id is null                           then null
        when c.shipped_at      <= e.exception_timestamp then 'SHIPPED'
        when c.pallet_built_at <= e.exception_timestamp then 'PALLET_BUILT'
        when c.retrieved_at    <= e.exception_timestamp then 'RETRIEVED_FROM_BUFFER'
        when c.stored_at       <= e.exception_timestamp then 'STORED_IN_BUFFER'
        when c.inducted_at     <= e.exception_timestamp then 'CASE_INDUCTED'
        else 'BEFORE_INDUCTION'
    end                                                                     as stage_at_exception
from exceptions e
left join cases c
    on e.case_id = c.case_id
