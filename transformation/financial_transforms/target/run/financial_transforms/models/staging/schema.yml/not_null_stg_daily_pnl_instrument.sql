
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select instrument
from "financial"."main"."stg_daily_pnl"
where instrument is null



  
  
      
    ) dbt_internal_test