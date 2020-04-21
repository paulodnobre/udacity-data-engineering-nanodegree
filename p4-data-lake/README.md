# Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

My mission as a data engineer is building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables using parquet format. This will allow the Sparkify analytics team to continue finding insights in what songs their users are listening to.

# Database Purpose and Schema

**Purpose**

- Enable further ad-hoc analysis by using SQL language thought AWS Athena or AWS Redshift Spectrum;
- Structure the JSON data in parquet files making consumption easier and faster;
- Very good query and ETL performance because the data is not big enough;
- Build a powerful, flexible, and cost effective analytics platform by using Python, SparkSQL and AWS S3.

**Fact Table**

- **songplays**: records in event data associated with song plays.

**Dimension Tables**

- **users**: users in the app
- **songs**: songs in music database
- **artists**: artists in music database
- **time table**: timestamps of records in songplays broken down into specific units

**SparkSQL Temp Tables**

I want to store the spark Dataframe as the table and query it. For this purpose, I used createOrReplaceTempView on spark Dataframe.

createOrReplaceTempView creates (or replaces if that view name already exists) a lazily evaluated "view" that you can then use like a hive table in Spark SQL. It does not persist to memory unless you cache the dataset that underpins the view [1].

- **vw_songs_data**: subset of real data from the Million Song Dataset.
- **vw_log_data**: log files in JSON format generated that contains informations about actions done by users.

# Files in the repository

- **dl.cfg**: configuration file that contains AWS Credentials.
- **etl.py**: Python script responsible for extract data from Song and Log datasets inside the S3 buckets, processes that data using Spark and writes them back to S3.

# References
[1] How does createOrReplaceTempView work in Spark? -  https://intellipaat.com/community/12213/how-does-createorreplacetempview-work-in-spark