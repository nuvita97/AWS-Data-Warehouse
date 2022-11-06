import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

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
    CREATE TABLE IF NOT EXISTS staging_events (
          artist varchar,
          auth varchar,
          firstName varchar,
          gender varchar,
          itemInSession integer,
          lastName varchar,
          length float,
          level varchar,
          location varchar,
          method varchar,
          page varchar,
          registration bigint,
          sessionId integer,
          song varchar,
          status integer,
          ts varchar,
          userAgent varchar,
          userId integer)
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
            song_id varchar PRIMARY KEY,
            num_songs integer,
            artist_id varchar,
            artist_latitude float,
            artist_longitude float,
            artist_location varchar,
            artist_name varchar,
            title varchar,
            duration float,
            year integer)
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id integer IDENTITY(0,1) PRIMARY KEY,
        start_time timestamp,
        user_id varchar,
        level varchar,
        song_id varchar,
        artist_id varchar,
        session_id bigint,
        location varchar,
        user_agent varchar)
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id varchar PRIMARY KEY,
        first_name varchar,
        last_name varchar,
        gender varchar,
        level varchar)
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar PRIMARY KEY,
        title varchar,
        artist_id varchar NOT NULL,
        year integer,
        duration decimal)
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar PRIMARY KEY,
        name varchar,
        location varchar,
        latitude decimal,
        longitude decimal)
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp PRIMARY KEY,
        hour integer,
        day integer,
        week integer,
        month integer,
        year integer,
        weekday integer)
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events 
    FROM {}
    iam_role '{}'
    REGION 'us-west-2'
    FORMAT as json {};
""").format(config.get('S3', 'LOG_DATA'), 
            config.get('IAM_ROLE', 'ARN'), 
            config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_songs 
    FROM {} 
    iam_role '{}'
    REGION 'us-west-2'
    FORMAT as json 'auto';
""").format(config.get('S3', 'SONG_DATA'), 
            config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, 
                            user_id, 
                            level, 
                            song_id, 
                            artist_id, 
                            session_id, 
                            location, 
                            user_agent) 
    SELECT
        TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' AS start_time, 
        e.userId AS user_id,
        e.level AS level,
        s.song_id AS song_id,
        s.artist_id AS artist_id,
        e.sessionId AS session_id,
        e.location AS location,
        e.userAgent AS user_agent
    FROM staging_events e, staging_songs s
    WHERE s.title = e.song 
        AND e.artist = s.artist_name
        AND e.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id,
                        first_name,
                        last_name,
                        gender,
                        level)
    SELECT DISTINCT 
        userId AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender AS gender,
        level AS level
    FROM staging_events
    WHERE userId IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, 
                        title,
                        artist_id,
                        year,
                        duration)
    SELECT DISTINCT 
        song_id AS song_id,
        title AS title,
        artist_id AS artist_id,
        year AS year,
        duration AS duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id,
                        name,
                        location,
                        latitude,
                        longitude)
    SELECT DISTINCT 
        artist_id AS artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time,
                        hour,
                        day,
                        week,
                        month,
                        year,
                        weekday)
    SELECT 
        start_time as start_time,
        extract(hour from start_time) AS hour,
        extract(day from start_time) AS day,
        extract(week from start_time) AS week,
        extract(month from start_time) AS month,
        extract(year from start_time) AS year,
        extract(weekday from start_time) AS weekday
    FROM songplays
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
