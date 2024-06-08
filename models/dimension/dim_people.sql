{{
    config
        (
            materialized='incremental',
            unique_id= 'unique_id',
            on_schema_change= 'sync_all_columns'
        )
}}

with

stg_person_raw as
(
    select * from {{ref("stg_person_raw")}}
)
select 
    unique_id,
    collision_id,
    person_id,
    {{handle_string_nulls('vehicle_id')}} as vehicle_id,
    {{handle_string_nulls('person_type')}} as person_type,
    {{handle_string_nulls('person_injury')}} as person_injury,
    {{handle_ages('person_age')}} as person_age,
    {{handle_string_nulls('person_sex')}} as person_sex,
    {{handle_string_nulls('ejection')}} as ejection,
    {{handle_string_nulls('emotional_status')}} as emotional_status,
    {{handle_string_nulls('bodily_injury')}} as bodily_injury,
    {{handle_string_nulls('position_in_vehicle')}} as position_in_vehicle,
    {{handle_string_nulls('safety_equipment')}} as safety_equipment,
    {{handle_string_nulls('ped_location')}} as ped_location,
    {{handle_string_nulls('ped_action')}} as ped_action,
    {{handle_string_nulls('complaint')}} as complaint,
    {{handle_string_nulls('ped_role')}} as  ped_role,
    {{handle_string_nulls('contributing_factor_1')}} as contributing_factor_1,
    {{handle_string_nulls('contributing_factor_2')}} as contributing_factor_2
from 
    stg_person_raw