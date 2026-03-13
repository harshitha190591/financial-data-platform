
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select avg_loss_ratio
from "financial"."main"."mart_loss_ratio"
where avg_loss_ratio is null



  
  
      
    ) dbt_internal_test