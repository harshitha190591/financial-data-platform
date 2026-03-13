
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select policy_type
from "financial"."main"."stg_policy_portfolio"
where policy_type is null



  
  
      
    ) dbt_internal_test