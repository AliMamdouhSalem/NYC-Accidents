{{
    config(
        materialized = "table"
    )
}}
with 
    date_generated_data as (

        {{ dbt_date.get_date_dimension("2012-07-01", "2050-12-31") }}

    )

select
    {{dbt_utils.generate_surrogate_key(['date_day'])}} as date_id,
    * 
from 
    date_generated_data
