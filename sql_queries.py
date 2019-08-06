import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stg_event"
staging_songs_table_drop = "DROP TABLE IF EXISTS stg_song"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay"
user_table_drop = "DROP TABLE IF EXISTS dim_user"
song_table_drop = "DROP TABLE IF EXISTS dim_song"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS stg_event(
    artist text
    ,auth text
    ,firstName text
    ,gender text
    ,ItemInSession int
    ,lastName text
    ,length float
    ,level text
    ,location text
    ,method text
    ,page text
    ,registration text
    ,sessionId int
    ,song text
    ,status int
    ,ts bigint
    ,userAgent text
    ,userId int
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS stg_song(
    song_id text PRIMARY KEY
    ,artist_id text
    ,artist_latitude float
    ,artist_longitude float
    ,artist_location text
    ,artist_name text
    ,duration float
    ,num_songs int
    ,title text
    ,year int
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplay(
    songplay_id bigint identity(0,1) PRIMARY KEY
    ,start_time timestamp
    ,user_id varchar
    ,level varchar
    ,song_id varchar
    ,artist_id varchar
    ,session_id int
    ,location varchar
    ,user_agent varchar
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_user(
    user_id varchar PRIMARY KEY
    ,first_name varchar
    ,last_name varchar
    ,gender varchar
    ,level varchar
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_song(
    song_id varchar PRIMARY KEY
    ,title varchar
    ,artist_id varchar
    ,year int
    ,duration float
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artist(
    artist_id varchar PRIMARY KEY
    ,name varchar
    ,location varchar
    ,latitude float
    ,longitude float
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time(
     ts bigint
    ,start_time timestamp PRIMARY KEY
    ,hour int
    ,day int
    ,week int
    ,month int
    ,year int 
    ,weekday int
)
""")

# STAGING TABLES

staging_events_copy = ("""
    copy stg_event
    from {}
    iam_role {}
    region 'us-west-2'
    json {}
    compupdate off;
""").format(config.get('S3','LOG_DATA'),config.get('IAM_ROLE','ARN'),config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    copy stg_song
    from {}
    iam_role {}
    region 'us-west-2'
    json 'auto'
    compupdate off
    truncatecolumns;
""").format(config.get('S3','SONG_DATA'),config.get('IAM_ROLE','ARN'))

# FINAL TABLES
 
songplay_table_insert = ("""
    INSERT INTO fact_songplay(
        start_time
        ,user_id
        ,level
        ,song_id
        ,artist_id
        ,session_id
        ,location
        ,user_agent
    )
    SELECT  DISTINCT
            TIMESTAMP 'epoch' + evt.ts/1000 *INTERVAL '1 second' AS start_time
            ,evt.userId AS user_id
            ,evt.level
            ,sng.song_id
            ,sng.artist_id
            ,evt.sessionId AS session_id
            ,evt.location
            ,evt.userAgent AS user_agent
    FROM    stg_event evt
            JOIN stg_song sng
                ON evt.song = sng.title
                AND evt.artist = sng.artist_name
                AND evt.length = sng.duration
    WHERE    evt.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO dim_user(
        user_id
        ,first_name
        ,last_name
        ,gender
        ,level
    )
    SELECT  DISTINCT
            userId AS user_id
            ,firstName AS first_name
            ,lastName AS last_name
            ,gender
            ,level
    FROM    stg_event
    WHERE   userId IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO dim_song(
        song_id
        ,title
        ,artist_id
        ,year
        ,duration
    )
    SELECT  DISTINCT
            song_id
            ,title
            ,artist_id
            ,year
            ,duration
    FROM    stg_song
""")

artist_table_insert = ("""
    INSERT INTO dim_artist(
        artist_id
        ,name
        ,location
        ,latitude
        ,longitude
    )
    SELECT  DISTINCT
            artist_id
            ,artist_name AS name
            ,artist_location AS location
            ,artist_latitude AS latitude
            ,artist_longitude AS longitude
    FROM    stg_song
""")

time_table_insert = ("""
    INSERT INTO dim_time(
         ts
        ,start_time
        ,hour
        ,day
        ,week
        ,month
        ,year
        ,weekday
    )
    SELECT  inr.ts
            ,inr.start_time
            ,EXTRACT(HOUR FROM inr.start_time) AS hour
            ,EXTRACT(DAY FROM inr.start_time) AS day
            ,EXTRACT(WEEK FROM inr.start_time) AS week
            ,EXTRACT(MONTH FROM inr.start_time) AS month
            ,EXTRACT(YEAR FROM inr.start_time) AS year
            ,EXTRACT(DOW FROM inr.start_time) AS weekday
    FROM    (
                SELECT  DISTINCT
                        sngpl.ts
                        ,TIMESTAMP 'epoch' + sngpl.ts/1000 *INTERVAL '1 second' AS start_time
                FROM    stg_event sngpl
                WHERE   ts is not null
            )inr
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create,staging_songs_table_create,songplay_table_create,user_table_create,song_table_create,artist_table_create,time_table_create]
drop_table_queries = [staging_events_table_drop,staging_songs_table_drop,songplay_table_drop,user_table_drop,song_table_drop,artist_table_drop,time_table_drop]
copy_table_queries = [staging_events_copy,staging_songs_copy]
insert_table_queries = [songplay_table_insert,user_table_insert,song_table_insert,artist_table_insert,time_table_insert]