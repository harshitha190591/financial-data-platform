with claims as (
    select * from "financial"."main"."stg_claims_summary"
),

portfolio as (
    select * from "financial"."main"."stg_policy_portfolio"
),

enriched as (
    select
        c.policy_type,
        c.claim_status,
        c.total_claims,
        c.total_claim_amount,
        c.avg_claim_amount,
        c.total_premium_collected,
        c.loss_ratio,
        p.total_policies,
        p.unique_customers,
        p.avg_premium,
        -- claims per policy ratio
        round(
            c.total_claims / nullif(p.total_policies, 0)
        , 4)                                        as claims_per_policy,
        -- risk tier based on loss ratio
        case
            when c.loss_ratio >= 0.8  then 'HIGH'
            when c.loss_ratio >= 0.5  then 'MEDIUM'
            else                           'LOW'
        end                                         as risk_tier
    from claims c
    left join portfolio p
        on  c.policy_type   = p.policy_type
)

select * from enriched