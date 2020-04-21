import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
    (artist           VARCHAR,
     auth             VARCHAR,
     firstName        VARCHAR,
     gender           VARCHAR,
     itemInSession    INT,
     lastName         VARCHAR,
     length           FLOAT,
     level            VARCHAR,
     location         VARCHAR,
     method           VARCHAR,
     page             VARCHAR,
     registration     FLOAT,
     sessionId        INT,
     song             VARCHAR,
     status           INT,
     ts               TIMESTAMP,
     userAgent        VARCHAR,
     userId           INT
    )
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
    (num_songs        INT,
     artist_id        VARCHAR,
     artist_latitude  FLOAT,
     artist_longitude FLOAT,
     artist_location  VARCHAR,
     artist_name      VARCHAR,
     song_id          VARCHAR,
     title            VARCHAR,
     duration         FLOAT,
     year             INT
    )
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplay
    (songplay_id      INT IDENTITY(0,1) PRIMARY KEY, 
    start_time        TIMESTAMP SORTKEY, 
    user_id           INT NOT NULL, 
    level             VARCHAR, 
    song_id           VARCHAR NOT NULL DISTKEY, 
    artist_id         VARCHAR NOT NULL, 
    session_id        INT NOT NULL, 
    location          VARCHAR, 
    user_agent        VARCHAR)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_user
    (user_id          INT PRIMARY KEY, 
    first_name        VARCHAR, 
    last_name         VARCHAR, 
    gender            VARCHAR, 
    level             VARCHAR) DISTSTYLE ALL
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_song
    (song_id          VARCHAR SORTKEY DISTKEY PRIMARY KEY, 
    title             VARCHAR, 
    artist_id         VARCHAR NOT NULL, 
    year              INT, 
    duration          FLOAT)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artist
    (artist_id        VARCHAR SORTKEY PRIMARY KEY,
     name             VARCHAR, 
     location         VARCHAR, 
     latitude         FLOAT, 
     longitude        FLOAT) DISTSTYLE ALL
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time
    (start_time       TIMESTAMP SORTKEY PRIMARY KEY,
     hour             INT, 
     day              INT, 
     week             INT, 
     month            INT, 
     year             INT, 
     weekday          VARCHAR) DISTSTYLE ALL
""")

# STAGING TABLES

staging_events_copy = (f"""
copy staging_events 
    from {LOG_DATA}
    iam_role '{IAM_ROLE}'
    region 'us-west-2'
    compupdate off statupdate off
    format as JSON {LOG_JSONPATH}
    timeformat as 'epochmillisecs';
""")

staging_songs_copy = (f"""
copy staging_songs 
    from {SONG_DATA}
    iam_role '{IAM_ROLE}'
    region 'us-west-2'
    compupdate off statupdate off
    format as JSON 'auto';
""")

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO fact_songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  DISTINCT(e.ts),
            e.userId,
            e.level,        
            s.song_id,       
            s.artist_id,      
            e.sessionId,     
            e.location,     
            e.userAgent      
    FROM staging_events e 
    JOIN staging_songs s 
     ON (e.song = s.title AND e.artist = s.artist_name)
    AND e.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO dim_user (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT(userId) AS user_id,
            firstName,
            lastName,
            gender,
            level
    FROM staging_events
    WHERE page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO dim_song (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT(song_id) AS song_id,
            title,
            artist_id,
            year,
            duration
    FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO dim_artist (artist_id, name, location, latitude, longitude)
    SELECT  DISTINCT(artist_id),
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
    FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO dim_time (start_time, hour, day, week, month, year, weekday)
    SELECT  DISTINCT(start_time) AS start_time,
            EXTRACT(hour FROM start_time) AS hour,
            EXTRACT(day FROM start_time) AS day,
            EXTRACT(week FROM start_time) AS week,
            EXTRACT(month FROM start_time) AS month,
            EXTRACT(year FROM start_time) AS year,
            EXTRACT(dayofweek FROM start_time) AS weekday
    FROM fact_songplay;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
