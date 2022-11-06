# PROJECT: DATA WAREHOUSE ON AWS (S3 & Redshift)


## Introduction
- A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
- This project is to build an ETL pipeline for a database hosted on Redshift by loading data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.


## Data Sources

- One source contains info about songs and artists: `s3://udacity-dend/song_data`
- The second source contains info log action of all users: `s3://udacity-dend/log_data`


## Database Schema
### Staging tables:
- **staging_songs**: info about songs and artists which was loaded from *song_data*
- **staging_events**: actions of all users which was loaded from *log_data*
### Fact Table:
- **songplays**: records in event data associated with song plays
### Dimension Tables:
- **users**: users in the application
- **songs**: songs in music database
- **artists**: artists in music database
- **time**: timestamps of records in songplays broken down into specific units.


## Project Files

1. `dwh.cfg`: config file containing info about S3, IAM and Redshift
2. `sql_queries.py`: python file containing all SQL queries used to be imported into the database.
3. `create_tables.py`: python file to drop and create tables.
4. `etl.py`: python script to excute ETL process dividing into 2 stages: extract JSON files to staging tables, and load from the staging tables into fact & dimensional tables in AWS Redshift.
5. `README.md`: this file - description for the project.


## How to Run

1. Setup in AWS: create Redshift cluster with IAM user, IAM role, store info into `dwh.cfg`
2. Run `create_tables.py` to DROP old tables and CREATE new ones in Redshift.
3. Run `etl.py` to excute ETL process.
