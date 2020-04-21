# Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

My mission as a data engineer is building an ETL pipeline that extracts events and song datasets from S3, stages them in Redshift, and transforms data into a set of dimensional tables for the Sparkify analytics team to continue finding insights in what songs their users are listening to.

# Database Purpose and Schema

**Purpose**

- Enable further ad-hoc analysis by using SQL language taking advantage of JOINS and aggregations;
- Structure the JSON data in tables in an ERD star-schema model making consumption easier;
- Very good query and ETL performance because the data is not big enough;
- Taking advantages of a scalable MPP architecture in the AWS Cloud by using Redshift Cluster.

**Fact Table**

- **fact_songplays**: records in event data associated with song plays.

**Dimension Tables**

- **dim_users**: users in the app
- **dim_songs**: songs in music database
- **dim_artists**: artists in music database
- **dim_time table**: timestamps of records in songplays broken down into specific units

**Staging Tables**

- **staging_events**: subset of real data from the Million Song Dataset.
- **staging_songs**: log files in JSON format generated that contains informations about actions done by users.

# Files in the repository

- **create_tables.py**: Python script that contains the logic to drop and create the schema.
- **dwh.cfg**: configuration file that contains needed informations about Redshift Cluster, IAM role and S3 buckets.
- **etl.py**: Python script responsible for extract data from Song and Log datasets inside the S3 buckets, loading into staging tables in Redshift, transforming and loading them to the fact and dimensional tables of the created database.
- **sql_queries.py**: Python script containing SQL Statements (DROP, CREATE, COPY and INSERT) used by create_tables.py and etl.py.

