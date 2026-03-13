
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select loss_ratio
from "financial"."main"."stg_claims_summary"
where loss_ratio is null



  
  
      
    ) dbt_internal_test