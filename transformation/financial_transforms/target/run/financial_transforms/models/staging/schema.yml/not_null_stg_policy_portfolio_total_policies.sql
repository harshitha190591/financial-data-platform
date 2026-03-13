
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_policies
from "financial"."main"."stg_policy_portfolio"
where total_policies is null



  
  
      
    ) dbt_internal_test