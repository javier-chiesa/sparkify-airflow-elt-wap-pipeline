class SqlQueries:
    songplay_table_insert = ("""
        SELECT DISTINCT
            md5(events.sessionid || events.start_time || songs.song_id) AS songplay_id,
            events.start_time, 
            events.userid AS user_id, 
            events.level, 
            songs.song_id, 
            songs.artist_id, 
            events.sessionid AS session_id, 
            events.artist AS artist_name,
            events.location, 
            events.useragent,
            DATE('{{ ds }}') AS dwh_ds
        FROM (
            SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging.staging_events
            WHERE page='NextSong'
                AND DATE(TIMESTAMP 'epoch' + ts/1000 * interval '1 second') = '{{ ds }}'
        ) events
        INNER JOIN staging.staging_songs songs
        ON events.song = songs.title
            AND events.artist = songs.artist_name
            AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT 
            userid AS user_id, 
            firstname, 
            lastname, 
            gender, 
            level,
            DATE('{{ ds }}') as dwh_ds
        FROM (
            SELECT 
                userid,
                firstname,
                lastname,
                gender,
                level,
                ROW_NUMBER() OVER (PARTITION BY userid ORDER BY ts DESC) AS row_n
            FROM staging.staging_events   
            WHERE page='NextSong'
                AND DATE(TIMESTAMP 'epoch' + ts/1000 * interval '1 second') = '{{ ds }}'
        )
        WHERE row_n = 1 
    """)

    song_table_insert = ("""
        SELECT 
            song_id, 
            title, 
            artist_id, 
            year, 
            duration,
            DATE('{{ ds }}') as dwh_ds
        FROM (
            SELECT
                song_id,
                title,
                artist_id,
                year,
                duration,
                ROW_NUMBER() OVER (PARTITION BY song_id ORDER BY duration DESC) AS row_n
            FROM staging.staging_songs
        )
        WHERE row_n = 1
    """)

    artist_table_insert = ("""
        SELECT 
            md5(artist_id || artist_name) artist_sk,
            artist_id, 
            artist_name, 
            MAX(artist_location) AS artist_location, 
            MAX(artist_latitude) AS artist_latitude, 
            MAX(artist_longitude) AS artist_longitude,
            DATE('{{ ds }}') as dwh_ds
        FROM staging.staging_songs
        GROUP BY 1, 2, 3
    """)

    time_table_insert = ("""
        WITH timestamp_from_epoch AS (
            SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time
            FROM staging.staging_events
            WHERE page='NextSong'
                AND DATE(TIMESTAMP 'epoch' + ts/1000 * interval '1 second') = '{{ ds }}'
        )

        SELECT 
            start_time, 
            extract(hour from start_time), 
            extract(day from start_time), 
            extract(week from start_time), 
            extract(month from start_time), 
            extract(year from start_time), 
            extract(dayofweek from start_time),
            DATE('{{ ds }}') as dwh_ds
        FROM timestamp_from_epoch
    """)