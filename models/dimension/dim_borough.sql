with 
stg_crashes_raw as
(

    select * from {{ref('stg_crashes_raw')}}

)

select 

    DISTINCT {{dbt_utils.generate_surrogate_key(['borough'])}} as borough_id,
    borough

from 

    stg_crashes_raw
