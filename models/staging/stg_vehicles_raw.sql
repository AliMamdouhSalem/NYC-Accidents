{{
    config(
        materialized= 'incremental',
        on_schema_change='fail'
    )
}}


with 

source as (

    select * from {{ source('staging', 'vehicles_raw') }}

),

renamed as (

    select
        distinct unique_id,
        collision_id,
        crash_date,
        crash_time,
        vehicle_id,
        state_registration,
        vehicle_type,
        vehicle_make,
        vehicle_model,
        vehicle_year,
        travel_direction,
        vehicle_occupants,
        driver_sex,
        driver_license_status,
        driver_license_jurisdiction,
        pre_crash,
        point_of_impact,
        vehicle_damage,
        vehicle_damage_1,
        vehicle_damage_2,
        vehicle_damage_3,
        public_property_damage,
        public_property_damage_type,
        contributing_factor_1,
        contributing_factor_2

    from source

)

select * from renamed
