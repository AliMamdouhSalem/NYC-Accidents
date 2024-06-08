# NYC-Accidents
This project is, hopefully, the first phase of analyzing the safety of New York City using different metrics. For this project the NYC Motor collision datasets were used to analyze and assess the safety of New York City in regards to motor collisions by creating an end to end ELT incremental pipeline that refreshes daily to capture the most updated data.

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


  
