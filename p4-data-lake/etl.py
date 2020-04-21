import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Read song data from S3 bucket, create Temp View vw_songs_table and process that data"""
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    
    # create Temp View vw_songs_table
    df.createOrReplaceTempView("vw_songs_table")

    # extract columns to create songs table
    songs_table = spark.sql("""
    SELECT  DISTINCT song_id, 
                     title, 
                     artist_id, 
                     year, 
                     duration 
    FROM vw_songs_table 
    WHERE song_id IS NOT NULL
    """)
    
    # write songs table to parquet files partitioned by year and artist
    print("Writing songs table")
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data+'songs_table/')

    # extract columns to create artists table
    artists_table = spark.sql("""
    SELECT DISTINCT artist_id,
                    artist_name, 
                    artist_location, 
                    artist_latitude, 
                    artist_longitude 
    FROM vw_songs_table
    WHERE artist_id IS NOT NULL
    """)
    
    # write artists table to parquet files
    print("Writing artists table")
    artists_table.write.mode('overwrite').parquet(output_data+'artists_table')


def process_log_data(spark, input_data, output_data):
    """Read log data from S3 bucket, create Temp View vw_log_table and process that data"""
    # get filepath to log data file
    log_path = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_path)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    # create Temp View vw_log_table
    df.createOrReplaceTempView("vw_log_table")

    # extract columns for users table    
    users_table = spark.sql("""
    SELECT DISTINCT userId as user_id,
                    firstName as first_name, 
                    lastName as last_name, 
                    gender, 
                    level 
    FROM vw_log_table 
    WHERE userId IS NOT NULL
    """)
    
    # write users table to parquet files
    print("Writing users table")
    users_table.write.mode('overwrite').parquet(output_data+'users_table/')
    
    # extract columns to create time table
    time_table = spark.sql("""
    SELECT DISTINCT A.date_time as start_time,
                    hour(A.date_time) as hour,
                    dayofmonth(A.date_time) as day,
                    weekofyear(A.date_time) as week,
                    month(A.date_time) as month,
                    year(A.date_time) as year,
                    dayofweek(A.date_time) as weekday
           FROM
            (SELECT to_timestamp(vlt.ts/1000) as date_time
             FROM vw_log_table vlt
             WHERE vlt.ts IS NOT NULL) A
    """)
    
    # write time table to parquet files partitioned by year and month
    print("Writing time table")
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'time_table/')


    # read in song data to use for songplays table
    songplays_df = spark.read.parquet(output_data+'songs_table/')

    # extract columns from joined song and log datasets to create songplays_table
    songplays_table = spark.sql("""
    SELECT  monotonically_increasing_id() as songplay_id,
            to_timestamp(vlt.ts/1000) as start_time, 
            month(to_timestamp(vlt.ts/1000)) as month, 
            year(to_timestamp(vlt.ts/1000)) as year, 
            vlt.userId as user_id, 
            vlt.level as level, 
            vst.song_id as song_id, 
            vst.artist_id as artist_id, 
            vlt.sessionId as session_id, 
            vlt.location as location, 
            vlt.userAgent as user_agent 
    FROM vw_songs_table vst
     JOIN vw_log_table vlt on vst.artist_name = vlt.artist 
     AND vlt.song = vst.title
    """)

    # write songplays table to parquet files partitioned by year and month
    print("Writing songplays table")
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'songplays_table/')



def main():
    spark = create_spark_session()
    input_data = "s3a://sparksqlinput/"
    output_data = "s3a://sparksqloutput/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
