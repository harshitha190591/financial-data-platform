
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select net_pnl
from "financial"."main"."stg_daily_pnl"
where net_pnl is null



  
  
      
    ) dbt_internal_test