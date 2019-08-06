# Data Warehousing on the Cloud

### Purpose 

Purpose of this project is to build an ETL Pipeline for a database hosted on Redshift. 

### Overview

Here is the high level overview of how this works -
Log data of all the users listening to the songs along with songs data of a fictional music company called Sparkify is first stored in Amazon S3 location.
Data is first ingested into Amazon Redshift staging tables through copy commands.
Data is then ingested into the data warehouse via multiple insert statements.

### Database Model

Amazon Redshift database hosted is composed of following tables -

1. Staging Tables - stg_event, stg_log
2. Dimensions - dim_artist, dim_song, dim_time, dim_user
3. Facts - fact_songplay

### How to run

Here is how the project needs to be used to run.Setup a 4 node cluser Amazon Redshift database and obtain the  ARN and HOST Values. 
Update dwh.cfg is updated with the right HOST and ARN values.

Run the following command to create the tables in the Redshift database - 

```sh
$ python create_tables.py
```

Run the following command to perform the ETL

```sh
$ python etl.py
```