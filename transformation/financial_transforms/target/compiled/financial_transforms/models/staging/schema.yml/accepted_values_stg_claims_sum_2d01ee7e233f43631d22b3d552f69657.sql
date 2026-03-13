
    
    

with all_values as (

    select
        claim_status as value_field,
        count(*) as n_records

    from "financial"."main"."stg_claims_summary"
    group by claim_status

)

select *
from all_values
where value_field not in (
    'OPEN','IN REVIEW','APPROVED','REJECTED','CLOSED'
)


