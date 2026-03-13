with pnl as (
    select * from "financial"."main"."stg_daily_pnl"
),

final as (
    select
        desk_id,
        instrument,
        count(distinct trade_date)      as trading_days,
        sum(total_trades)               as total_trades,
        sum(total_volume)               as total_volume,
        round(sum(net_pnl), 2)          as total_net_pnl,
        round(avg(net_pnl), 2)          as avg_daily_pnl,
        round(max(net_pnl), 2)          as best_day_pnl,
        round(min(net_pnl), 2)          as worst_day_pnl,
        -- performance tier
        case
            when sum(net_pnl) > 0  then 'PROFITABLE'
            when sum(net_pnl) = 0  then 'FLAT'
            else                        'LOSS'
        end                             as performance_tier
    from pnl
    group by desk_id, instrument
    order by total_net_pnl desc
)

select * from final