# NYC-Accidents
This project is, hopefully, the first phase of analyzing the safety of New York City using different metrics. For this phase the NYC Motor collision datasets were used to analyze and assess the safety of New York City in regards to motor collisions by creating an end to end ELT incremental pipeline that refreshes daily to capture the most updated data.

## Project Overview

## Questions
This project aims to answer a number of question like: 
- In which borough do the most accidents happen?
- What are the main contributing factors to accidents?
- How many people die daily/monthly/yearly per borough because of accidents?
- in which streets do the most accidents happen?
- and hopefully many more!

## Data
This project uses three datasets from the [NYC Open Data](https://opendata.cityofnewyork.us/). 
- The [Motor Vehicle Collision - Crashes](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95/about_data) which contains data about the crash event. Each row represents a crash event.
- The [Motor Vehicle Collision - Vehicles](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Vehicles/bm4k-52h4/about_data) which contains details on each vehicle involved in the crash. Each row represents a motor vehicle involved in a crash.
- The [Motor Vehicle Collisions - Person](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Person/f55k-p6yu/about_data) which ontains details for people involved in the crash. Each row represents a person (driver, occupant, pedestrian, bicyclist,..) involved in a crash.
  
#### Source ERD
![image](https://github.com/AliMamdouhSalem/NYC-Accidents/assets/74428524/e8d6b522-ec3c-4597-88fa-009f9ef0e029)

## Data Exploration
Data was explored in two ways. First of all, the data dicitionaries for all three datasets were read well then python was used to explore the data further and develop the extraction technique. I used jupyter notebook on my Github Codespace to the data exploration. 

After researching the dictionary and the API documentation the following was found:
- Only 1000 records can be retrieved at a time
- The data is retrieved in json format
- the offset attribute can be used to retrieve the data in increments of 1000s
```'https://data.cityofnewyork.us/resource/h9gi-nx95.json?$offset=0' ``` Where the offset is the starting record number

After succesfully retrieving the data the following was found:
- Main attributes in the dictionary were not found in some records.
- New attributes appeared in some records that sometimes were regular values or an array with different values
- there were duplicates in the three tables
- The daily increment in the data is much less than 1000 records

The following decisions were taken based on this information:
- Only attributes that are in the data dictionary will be considered
- The data extraction and loading will be split into two. Initial loading for the first load of the dataset and incremental loading for daily updates.

## Extract, Load, and Orchestrate
The extraction, loading were done with python, orchestrated with Mage AI, hosted on Github Codespaces and the data would be loaded into BigQuey. To use Mage AI I followed the [quickstart guide](https://docs.mage.ai/getting-started/setup#docker-compose-template) on their website using the following way.

```
git clone https://github.com/mage-ai/compose-quickstart.git mage-quickstart \
&& cd mage-quickstart \
&& cp dev.env .env && rm dev.env \
&& docker compose up
```

As discussed in the last section two pipelines will be made for each dataset:
### Initial Loading
These pipelines handle the initial loading of the data. As we saw in the previous section that the API can only fetch 1000 records per request and our smallest table has more than 2 million records. One pipeline was created for each table. Mage AI uses code blocks to extract, transform and load data. It also has lots of preset blocks for different connections with pyton and sql

#### Person Pipeline

![image](https://github.com/AliMamdouhSalem/NYC-Accidents/assets/74428524/74cb243b-e12e-499f-a41d-cbfa0ba8b656)

#### Vehicles Pipeline

![image](https://github.com/AliMamdouhSalem/NYC-Accidents/assets/74428524/5fb5ea2c-0dd5-4020-bb59-cc4599939ee9)

#### Crashes Pipeline

![image](https://github.com/AliMamdouhSalem/NYC-Accidents/assets/74428524/9948ca72-af39-4966-8bcf-1e481bb9dcee)

 The initial load will be explained using the crashes pipeline as an example:

##### Ingestion
As discussed in the expolaration section, the columns must be pre-set as it sometimes change from the source so only the columns that were mentioned in the data dictionary was used. The approach here was to increase the offset by 1000 for each iteration and get the data from json format into the dataframe by using a nested loop for each 1000 records until we have pulled all the available data.
```
import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    columns=['collision_id',
    'crash_date',
    'crash_time',
    'borough',
    'latitude',
    'longitude',
    'on_street_name',
    'cross_street_name',
    'off_street_name',
    'number_of_persons_injured',
    'number_of_persons_killed',
    'number_of_pedestrians_injured',
    'number_of_pedestrians_killed',
    'number_of_cyclist_injured',
    'number_of_cyclist_killed',
    'number_of_motorist_injured',
    'number_of_motorist_killed',
    'contributing_factor_vehicle_1',
    'contributing_factor_vehicle_2',
    'contributing_factor_vehicle_3',
    'contributing_factor_vehicle_4',
    'contributing_factor_vehicle_5',
    'vehicle_type_code1',
    'vehicle_type_code2',
    'vehicle_type_code3',
    'vehicle_type_code4',
    'vehicle_type_code5']
    offset=0
    data=[]
    s=0
    link=f'https://data.cityofnewyork.us/resource/h9gi-nx95.json?$offset={offset}'
    response = requests.get(link).json()
    while len(response)>0:
        link=f'https://data.cityofnewyork.us/resource/h9gi-nx95.json?$offset={offset}'
        response = requests.get(link).json()
        if isinstance(response,str):
            print('Error retrieving data, retrying')
        else:
            for i in response:
                record=[]
                for j in columns:
                    if j in list(i.keys()):
                        record.append(i[j])
                    else:
                        record.append(None)
                data.append(record)
                s=s+1
            offset=offset+1000
            print(f'{offset} records were loaded to the dataframe successfully')
            crashes= pd.DataFrame(data=data,columns=columns)
            
    print(f'{offset} records were loaded to the dataframe successfully')
    return crashes


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'


```
##### Transformation
As we are doing an ELT pipeline the transformation here was vere minimal. Only the data type of crash date was changed from string to date as it is used in the incremental approach

```
import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test



@transformer
def transform(crashes, *args, **kwargs):
    print(crashes)
    crashes['crash_date'] = pd.to_datetime(crashes['crash_date'])
    return crashes


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'


```
##### Loading
To load the data, Mage AI's pre-set sql loader to BigQuery was used

```
SELECT * FROM {{ df_1 }} t

```
### Incremental Loading
These pipelines handle the daily increment in the data. They do that by fetching the max value of date from Google BigQuery, and pull the data from the max date value to the day the pipeline was run. These pipelines are scheduled to run daily
#### Person Pipeline

![image](https://github.com/AliMamdouhSalem/NYC-Accidents/assets/74428524/7acfbf84-1cfc-42af-bac0-475e2f7e7583)

#### Vehicles Pipeline

![image](https://github.com/AliMamdouhSalem/NYC-Accidents/assets/74428524/761c7b6f-e823-4372-bf8b-84c026d60797)


#### Crashes Pipeline
![image](https://github.com/AliMamdouhSalem/NYC-Accidents/assets/74428524/5590e958-77e5-4823-ab5f-0d0b44f4ca3a)

The incremental load will be explained using the crashes pipeline as an example:

##### Getting the max date
The max date is pulled from BigQuery using python
```
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test



@data_loader
def load_data_from_big_query(*args, **kwargs):
    """
    Template for loading data from a BigQuery warehouse.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#bigquery
    """
    query = 'SELECT max(crash_date) FROM `nyc-accidents-423921.raw.crashes_raw` LIMIT 1000'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    last_update_date = BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).load(query)
    
    print(last_update_date)
    return last_update_date


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

```
##### Getting the response
The max date is adjusted and used with the date of running the pipeline to get the new data from the source
```
import io
import pandas as pd
import requests
from datetime import date,timedelta
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
from mage_ai.data_preparation.variable_manager import set_global_variable

@data_loader
def load_data_from_api(last_update_date,*args, **kwargs):
    last_update_date = last_update_date.iloc[0,0]+timedelta(days=1)
    last_update_date = last_update_date.strftime('%Y-%m-%d')
    last_update_date= '\'' + last_update_date + '\''
    today= '\'' + date.today().strftime('%Y-%m-%d' ) + '\''
    print(last_update_date)
    set_global_variable('crashes_incremental_gcp', 'max_date', last_update_date)
    link=f'https://data.cityofnewyork.us/resource/h9gi-nx95.json?$where=crash_date between {last_update_date} and {today}'
    response = requests.get(link).json()
    print(len(response))
    return response
```

##### Conditional Block
Mage AI has conditional blocks that can be added on to other blocks and stop them from running if they didn't evaluate to true. In our project it is used to stop the pipeline if the response for these dates are empty

```
if 'condition' not in globals():
    from mage_ai.data_preparation.decorators import condition


@condition
def evaluate_condition(response,*args, **kwargs) -> bool:
    if len(response)>0:
        return True
    else:
        return False

```

##### Json to Dataframe
If the response isn't empty and the conditional block evaluated to true, the same json to dataframe code that's used in the initial load is used here.

```
import io
import pandas as pd
import requests
from datetime import date
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(response,*args, **kwargs):
    columns=['collision_id',
    'crash_date',
    'crash_time',
    'borough',
    'latitude',
    'longitude',
    'on_street_name',
    'cross_street_name',
    'off_street_name',
    'number_of_persons_injured',
    'number_of_persons_killed',
    'number_of_pedestrians_injured',
    'number_of_pedestrians_killed',
    'number_of_cyclist_injured',
    'number_of_cyclist_killed',
    'number_of_motorist_injured',
    'number_of_motorist_killed',
    'contributing_factor_vehicle_1',
    'contributing_factor_vehicle_2',
    'contributing_factor_vehicle_3',
    'contributing_factor_vehicle_4',
    'contributing_factor_vehicle_5',
    'vehicle_type_code1',
    'vehicle_type_code2',
    'vehicle_type_code3',
    'vehicle_type_code4',
    'vehicle_type_code5']
    data=[]
    for i in response:
        record=[]
        for j in columns:
            if j in list(i.keys()):
                record.append(i[j])
            else:
                record.append(None)
        data.append(record)
    crashes= pd.DataFrame(data=data,columns=columns)
    return crashes


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
```
##### Transformation
Same as in the initial load only the data type for crash date would be changed
```
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import pandas as pd

@transformer
def transform(crashes, *args, **kwargs):

    crashes['crash_date'] = pd.to_datetime(crashes['crash_date'])

    return crashes


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

```
##### Loading
Also same loading into BigQuery as in the inital load
```
SELECT * FROM {{ df_1 }}
```
## Transforming and Modelling
DBT was used to transform and model the data for analysis

### DBT DAG
![image](https://github.com/AliMamdouhSalem/NYC-Accidents/assets/74428524/0a634e5d-997d-4ff9-b6c3-dc09644029be)

### Macros

#### handle_string_nulls
```
{% macro handle_string_nulls(field) -%}

    case 
        when {{field}} is null then 'NA'
        else {{field}}
    end

{%- endmacro %}
```
#### convert_numerics
```
{% macro convert_numerics(number) -%}

    case 
        when {{number}} is null then 0
        else cast({{number}} as integer) 
    end

{%- endmacro %}
```
#### handle_ages
```
{% macro handle_ages(age) -%}
    case   
        when cast({{age}} as integer)<0 then -1
        when cast({{age}} as integer) is null then -1
        when cast({{age}} as integer)>125 then -1
        else cast({{age}} as integer)
    end 
{%- endmacro%}
```
### Staging

#### stg_crashes_raw
```
XX{{
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

```
#### stg_vehicles_raw
```
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

```
#### stg_person_raw
```
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

```

### Fact

#### fct_crashes

```
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

```

### Dimensions

#### dim_borough
```
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

```
#### dim_date
```
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

```
#### dim_location
```
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
```
#### dim_people
```
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
```
#### dim_vehicles
```
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

```
## Analysis and reporting
