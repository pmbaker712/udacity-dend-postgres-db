# DROP TABLES

songplays_table_drop = "DROP TABLE IF EXISTS songplays;"
users_table_drop = "DROP TABLE IF EXISTS users;"
songs_table_drop = "DROP TABLE IF EXISTS songs;"
artists_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplays_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id SERIAL PRIMARY KEY,
        timestamp timestamp, 
        user_id varchar NOT NULL,
        level varchar,
        song_id varchar, --nullable since song ids missing from limited dataset
        song varchar, 
        length decimal,
        artist_id varchar, --nullable since artist ids missing from limited dataset
        artist varchar,
        session_id varchar NOT NULL,
        location varchar,
        user_agent varchar
    );
""")

users_table_create = ("""

    CREATE TABLE IF NOT EXISTS users (
        user_id int PRIMARY KEY,
        first_name varchar,
        last_name varchar,
        gender varchar,
        level varchar
    );
""")

songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar PRIMARY KEY,
        title varchar,
        artist_id varchar NOT NULL,
        year int,
        duration decimal
    );
""")

artists_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar PRIMARY KEY,
        name varchar,
        location varchar,
        latitude decimal,
        longitude decimal
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        timestamp timestamp PRIMARY KEY,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int
    );
""")

# INSERT RECORDS

# Discrete events, so no need to update
songplays_table_insert = ("""
    INSERT INTO songplays (
        songplay_id,
        timestamp,
        user_id,
        level,
        song_id,
        song,
        length,
        artist_id,
        artist,
        session_id,
        location,
        user_agent
    ) 
    VALUES (
        DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (songplay_id) DO NOTHING;
""")

# Derived from logs with timestamp, insert user attributes of latest songplay entry

users_table_insert = ("""
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level
    ) 
    VALUES (
        %s,%s,%s,%s,%s
    )
    ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level;
""")

# Check for duplicate primary keys before inserting

songs_table_insert = ("""
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration
    ) 
    VALUES (
        %s,%s,%s,%s,%s
    )
    ON CONFLICT (song_id) DO NOTHING;
""")

# Check for duplicate primary keys before inserting

artists_table_insert = ("""
    INSERT INTO artists (
        artist_id,
        name,
        location,
        latitude,
        longitude
    ) 
    VALUES (
        %s,%s,%s,%s,%s
    )
    ON CONFLICT (artist_id) DO NOTHING;
""")

# Check for duplicate primary keys before inserting

time_table_insert = ("""
    INSERT INTO time (
        timestamp,
        hour,
        day,
        week,
        month,
        year,
        weekday
    ) 
    VALUES (
        %s,%s,%s,%s,%s,%s,%s
    )
    ON CONFLICT (timestamp) DO NOTHING;
""")

# FIND SONGS

song_select = ("""
    SELECT 
        songs.song_id,
        artists.artist_id 
    FROM songs JOIN artists 
        ON songs.artist_id = artists.artist_id 
    WHERE songs.title = %s 
        AND artists.name = %s 
        AND songs.duration = %s;
""")

# QUERY LISTS

create_table_queries = [songs_table_create, artists_table_create, time_table_create, users_table_create, songplays_table_create] 
drop_table_queries = [songs_table_create, artists_table_create, time_table_create, users_table_drop, songplays_table_drop] 
