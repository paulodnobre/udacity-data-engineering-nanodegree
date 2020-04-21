# Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

My role as a data engineer is to create a Postgres database schema with tables designed to optimize queries on song play analysis and ETL pipeline for this analysis.

# Database Purpose and Schema

**Purpose**

- Enable further ad-hoc analysis by using SQL language taking advantage of JOINS and aggregations;
- Structure the JSON data in tables in a ERD star-schema model making consumption easier;
- Very good query and ETL performance because the data is not big enough.

**Fact Table**

- **songplays**: records in event data associated with song plays.

**Dimension Tables**

- **users**: users in the app
- **songs**: songs in music database
- **artists**: artists in music database
- **time table**: timestamps of records in songplays broken down into specific units

# Files in the repository

- **test.ipynb**: displays the first few rows of each table to let you check your database
- **etl.ipynb**: reads and processes a single file from song_data and log_data and loads the data into your tables.
- **data**: Folder containing data of songs and logs
- **create_tables.py**: Python script containing SQL Statements for drop and creating database and tables
- **sql_queries.py**: Python script containing SQL Statements used by create_tables.py and etl.py
- **etl.py**: Python script to extract the needed information from Song and Log data inside the data folder and parsing/inserting them to the created database schema and tables
