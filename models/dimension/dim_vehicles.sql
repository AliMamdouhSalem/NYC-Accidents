{{
    config
        (
            materialized = 'incremental',
            unique_id = 'unique_id',
            on_schema_change = 'sync_all_columns'
        )
}}

with 

stg_vehicles_raw as
(

        select * from {{ref('stg_vehicles_raw')}}

)

select 
    unique_id,
    collision_id,
    vehicle_id,
    {{handle_string_nulls('state_registration')}} as state_registration,
    {{handle_string_nulls('vehicle_type')}} as vehicle_type,
    {{handle_string_nulls('vehicle_make')}} as vehicle_make,
    {{handle_string_nulls('vehicle_model')}} as vehicle_model,
    case 
        when {{convert_numerics('vehicle_year')}} > 2025 then 0
        when {{convert_numerics('vehicle_year')}} < 1800 then 0
        else {{convert_numerics('vehicle_year')}} 
    end as vehicle_year,
    case 
        when upper({{handle_string_nulls('travel_direction')}}) = 'N' or upper({{handle_string_nulls('travel_direction')}}) = 'NORTH' then 'North'
        when upper({{handle_string_nulls('travel_direction')}}) = 'S' or upper({{handle_string_nulls('travel_direction')}}) = 'SOUTH' then 'South'
        when upper({{handle_string_nulls('travel_direction')}}) = 'E' or upper({{handle_string_nulls('travel_direction')}}) = 'EAST' then 'East'
        when upper({{handle_string_nulls('travel_direction')}}) = 'W' or upper({{handle_string_nulls('travel_direction')}}) = 'WEST' then 'West'
        when upper({{handle_string_nulls('travel_direction')}}) = 'NE' or upper({{handle_string_nulls('travel_direction')}}) = 'NORTHEAST' then 'North-East'
        when upper({{handle_string_nulls('travel_direction')}}) = 'NW' or upper({{handle_string_nulls('travel_direction')}}) = 'NORTHWEST' then 'North-West'
        when upper({{handle_string_nulls('travel_direction')}}) = 'SE' or upper({{handle_string_nulls('travel_direction')}}) = 'SOUTHEAST' then 'South-East'
        when upper({{handle_string_nulls('travel_direction')}}) = 'SW' or upper({{handle_string_nulls('travel_direction')}}) = 'SOUTHWEST' then 'South-West'
        else 'NA'
    end as travel_direction,
    {{convert_numerics('vehicle_occupants')}} as vehicle_occupants,
    {{handle_string_nulls('driver_sex')}} as driver_sex,
    {{handle_string_nulls('driver_license_status')}} as driver_license_status,
    {{handle_string_nulls('driver_license_jurisdiction')}} as driver_license_jurisdiction,
    {{handle_string_nulls('pre_crash')}} as pre_crash,
    {{handle_string_nulls('point_of_impact')}} as point_of_impact,
    {{handle_string_nulls('vehicle_damage')}} as vehicle_damage,
    {{handle_string_nulls('vehicle_damage_1')}} as vehicle_damage_1,
    {{handle_string_nulls('vehicle_damage_2')}} as vehicle_damage_2,
    {{handle_string_nulls('vehicle_damage_3')}} as vehicle_damage_3,
    case 
        when public_property_damage_type is null then 'No public property damage'
        else 'Public property damage'
    end as is_public_property_damage, 
    {{handle_string_nulls('public_property_damage_type')}} as public_property_damage_description,
    {{handle_string_nulls('contributing_factor_1')}} as contributing_factor_1,
    {{handle_string_nulls('contributing_factor_2')}} as contributing_factor_2

from 
    stg_vehicles_raw
