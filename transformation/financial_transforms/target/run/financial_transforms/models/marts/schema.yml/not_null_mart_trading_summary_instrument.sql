
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select instrument
from "financial"."main"."mart_trading_summary"
where instrument is null



  
  
      
    ) dbt_internal_test