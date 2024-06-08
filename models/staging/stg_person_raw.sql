{{
    config(
        materialized= 'incremental',
        on_schema_change='fail'
    )
}}



with 

source as (

    select * from {{ source('staging', 'person_raw') }}

),

renamed as (

    select
        distinct unique_id,
        collision_id,
        crash_date,
        crash_time,
        person_id,
        person_type,
        person_injury,
        vehicle_id,
        person_age,
        ejection,
        emotional_status,
        bodily_injury,
        position_in_vehicle,
        safety_equipment,
        ped_location,
        ped_action,
        complaint,
        ped_role,
        contributing_factor_1,
        contributing_factor_2,
        person_sex

    from source

)

select * from renamed 
