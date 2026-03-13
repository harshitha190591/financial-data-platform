
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select performance_tier
from "financial"."main"."mart_trading_summary"
where performance_tier is null



  
  
      
    ) dbt_internal_test