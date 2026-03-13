
    
    

with all_values as (

    select
        policy_status as value_field,
        count(*) as n_records

    from "financial"."main"."stg_policy_portfolio"
    group by policy_status

)

select *
from all_values
where value_field not in (
    'ACTIVE','EXPIRED','CANCELLED'
)


