{{
    config(
        materialized= 'incremental',
        unique_id='location_id',
        on_schema_change='sync_all_columns'
    )
}}

with stg_crashes_raw as (

    select * from {{ref('stg_crashes_raw')}}

)

select 
   distinct {{dbt_utils.generate_surrogate_key(['on_street_name','cross_street_name','off_street_name'])}} as location_id,
   on_street_name,
   cross_street_name,
   off_street_name
from
    stg_crashes_raw