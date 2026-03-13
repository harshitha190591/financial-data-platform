
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select unique_customers
from "financial"."main"."stg_policy_portfolio"
where unique_customers is null



  
  
      
    ) dbt_internal_test