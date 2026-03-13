with source as (
    select * from "financial"."main"."claims_summary"
),

renamed as (
    select
        policy_type,
        status                                    as claim_status,
        total_claims,
        round(total_claim_amount, 2)              as total_claim_amount,
        round(avg_claim_amount, 2)                as avg_claim_amount,
        round(total_premium_collected, 2)         as total_premium_collected,
        round(coalesce(loss_ratio, 0), 4)         as loss_ratio,
        _created_at                               as loaded_at
    from source
    where policy_type is not null
)

select * from renamed