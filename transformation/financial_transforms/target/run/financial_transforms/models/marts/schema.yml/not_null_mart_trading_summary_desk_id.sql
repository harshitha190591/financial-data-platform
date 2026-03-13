
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select desk_id
from "financial"."main"."mart_trading_summary"
where desk_id is null



  
  
      
    ) dbt_internal_test