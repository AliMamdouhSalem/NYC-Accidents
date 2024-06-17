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
The extraction, loading were done with python, orchestrated with Mage AI and hosted on Github Codespaces. To use Mage AI I followed the [quickstart guide](https://docs.mage.ai/getting-started/setup#docker-compose-template) on their website using the following way. 

```
git clone https://github.com/mage-ai/compose-quickstart.git mage-quickstart \
&& cd mage-quickstart \
&& cp dev.env .env && rm dev.env \
&& docker compose up
```

As discussed in the last section two pipelines will be made for each dataset:
### Initial Loading
These pipelines handle the initial loading of the data. As we saw in the previous section that the API can only fetch 1000 records per request and our smallest table has more than 2 million records

### Incremental Loading
These pipelines handle the daily increment in the data. They do that by fetching the max value of date from Google BigQuery.


## Transforming and Modelling
DBT was used to transform and model the data for analysis
  
