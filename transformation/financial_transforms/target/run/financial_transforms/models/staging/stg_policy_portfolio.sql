
  
  create view "financial"."main"."stg_policy_portfolio__dbt_tmp" as (
    with source as (
    select * from "financial"."main"."policy_portfolio"
),

renamed as (
    select
        policy_type,
        status                            as policy_status,
        total_policies,
        round(total_premium, 2)           as total_premium,
        round(avg_premium, 2)             as avg_premium,
        unique_customers,
        _created_at                       as loaded_at
    from source
    where policy_type is not null
)

select * from renamed
  );
