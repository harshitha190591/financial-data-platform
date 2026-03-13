
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        risk_tier as value_field,
        count(*) as n_records

    from "financial"."main"."mart_loss_ratio"
    group by risk_tier

)

select *
from all_values
where value_field not in (
    'HIGH','MEDIUM','LOW'
)



  
  
      
    ) dbt_internal_test