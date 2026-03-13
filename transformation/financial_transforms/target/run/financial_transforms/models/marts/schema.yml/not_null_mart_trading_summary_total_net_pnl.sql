
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_net_pnl
from "financial"."main"."mart_trading_summary"
where total_net_pnl is null



  
  
      
    ) dbt_internal_test