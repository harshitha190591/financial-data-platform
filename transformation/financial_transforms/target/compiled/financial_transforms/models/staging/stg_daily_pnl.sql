with source as (
    select * from "financial"."main"."daily_pnl"
),

renamed as (
    select
        trade_date,
        desk_id,
        instrument,
        round(net_pnl, 2)       as net_pnl,
        total_trades,
        total_volume,
        _created_at             as loaded_at
    from source
    where trade_date is not null
)

select * from renamed