
  
    
    

    create  table
      "financial"."main"."mart_loss_ratio__dbt_tmp"
  
    as (
      with enriched as (
    select * from "financial"."main"."int_claims_enriched"
),

final as (
    select
        policy_type,
        risk_tier,
        sum(total_policies)             as total_policies,
        sum(unique_customers)           as total_customers,
        sum(total_claims)               as total_claims,
        round(sum(total_claim_amount), 2)   as total_claims_value,
        round(sum(total_premium_collected), 2) as total_premiums,
        round(avg(loss_ratio), 4)       as avg_loss_ratio,
        round(avg(claims_per_policy), 4) as avg_claims_per_policy,
        round(avg(avg_premium), 2)      as avg_premium
    from enriched
    group by policy_type, risk_tier
    order by avg_loss_ratio desc
)

select * from final
    );
  
  