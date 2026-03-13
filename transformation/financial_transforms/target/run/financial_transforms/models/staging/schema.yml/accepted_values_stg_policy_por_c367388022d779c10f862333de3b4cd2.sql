
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

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



  
  
      
    ) dbt_internal_test