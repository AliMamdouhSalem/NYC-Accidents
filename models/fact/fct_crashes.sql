{{
    config
        (
            materialized = 'incremental',
            unique_id = 'collision_id',
            on_schema_change = 'sync_all_columns'
        )
}}

with 

stg_crashes_raw as (

    select * from {{ref("stg_crashes_raw")}}

)

select 

    collision_id,
    EXTRACT(TIME FROM PARSE_DATETIME('%H:%M', crash_time)) as crash_time,
    {{dbt_utils.generate_surrogate_key(['crash_date'])}} as date_id,
    {{dbt_utils.generate_surrogate_key(['on_street_name','cross_street_name','off_street_name'])}} as location_id,
    {{dbt_utils.generate_surrogate_key(['borough'])}} as borough_id,
    PARSE_BIGNUMERIC(latitude) as latitude,
    PARSE_BIGNUMERIC(longitude) as longitude,
    {{convert_numerics('number_of_persons_injured')}} as number_of_injured,
    {{convert_numerics('number_of_persons_killed')}} as number_of_killed

from 

    stg_crashes_raw
