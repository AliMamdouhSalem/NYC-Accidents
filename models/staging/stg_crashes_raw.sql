{{
    config(
        materialized= 'incremental',
        on_schema_change='fail'
    )
}}


with 

source as (

    select * from {{ source('staging', 'crashes_raw') }}

),

renamed as (

    select
        distinct collision_id,
        EXTRACT(DATE FROM crash_date) as crash_date,
        crash_time,
        {{handle_string_nulls('borough')}} as borough,
        latitude,
        longitude,
        {{handle_string_nulls('on_street_name')}} as on_street_name,
        {{handle_string_nulls('cross_street_name')}} as cross_street_name,
        {{handle_string_nulls('off_street_name')}} as off_street_name,
        number_of_persons_injured,
        number_of_persons_killed,
        number_of_pedestrians_injured,
        number_of_pedestrians_killed,
        number_of_cyclist_injured,
        number_of_cyclist_killed,
        number_of_motorist_injured,
        number_of_motorist_killed,
        contributing_factor_vehicle_1,
        contributing_factor_vehicle_2,
        contributing_factor_vehicle_3,
        contributing_factor_vehicle_4,
        contributing_factor_vehicle_5,
        vehicle_type_code1,
        vehicle_type_code2,
        vehicle_type_code3,
        vehicle_type_code4,
        vehicle_type_code5

    from source

)

select * from renamed order by crash_date
