# Covid-19 Dashboard Project
### Team Members: Mark Adut, Aryaman Dutta, Emre Toner, Andrew Wang

## Project Description:
In this project, we used Heroku to store a relational database of COVID-19 data sourced from the Center for Systems Science and Engineering (CSSE) at Johns Hopkins University (JHU). The data was acquired in raw form from the JHU github, from which a CSV was sourced and subsequently interfaced with pyscopg2 to create and store the data in an SQL relation. This data is continually updated on a daily basis to reflect the current state of the covid-19 pandemic across the world. There were two separate data frames that were sourced from JHU. The first dataframe was the historical dataframe, which contained information on the covid metrics since the beginning of the pandemic but not including the latest update. The second data frame was the updated-daily ( incremental update every 24 hrs) data frame which contained raw data on the daily updates of covid metrics. 

## Startup Instructions:
bash start.sh
OR
python3 data_acquire.py & python3 app.py

## Report/Documentation:
https://docs.google.com/document/d/1a-vemUzf0qReG4Q0yTCgZzUf-zAqW2XP8ql__4aEq8Q/edit?usp=sharing

### EDA/ETL and Database Testing in Jupyter Notebook