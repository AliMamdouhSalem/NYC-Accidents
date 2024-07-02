# NYC-Accidents
This project is, hopefully, the first phase of analyzing the safety of New York City using different metrics. For this phase the NYC Motor collision datasets were used to analyze and assess the safety of New York City in regards to motor collisions by creating an end to end ELT incremental pipeline that refreshes daily to capture the most updated data.

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
  
