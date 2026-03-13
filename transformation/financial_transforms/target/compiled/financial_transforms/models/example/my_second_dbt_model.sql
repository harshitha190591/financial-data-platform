-- Use the `ref` function to select from other models

select *
from "financial"."main"."my_first_dbt_model"
where id = 1