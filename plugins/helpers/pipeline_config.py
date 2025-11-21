"""Pipeline configuration for Airflow DAG with WAP pattern.

This module contains configuration for staging, fact, and dimension tables
in the data warehouse pipeline.
"""

from helpers.sql_queries import SqlQueries

# Staging tables configuration
STAGING_CONFIG = [
    {
        'table': 'staging_events',
        's3_key': (
            'log-data/{{ execution_date.year }}/'
            '{{ execution_date.month }}/{{ ds }}-events.json'
        ),
        'json_path': 'log_json_path.json',
        'delete_by_date': True
    },
    {
        'table': 'staging_songs',
        's3_key': 'song-data/',
        'json_path': 'auto',
        'delete_by_date': False
    }
]

# Fact tables configuration
FACT_CONFIG = [
    {
        'table': 'fct_songplays',
        'sql_query': SqlQueries.songplay_table_insert,
        'load_mode': 'partition_overwrite',
        'checks': {
            'unique': ['songplay_id'],
            'not_null': [
                'start_time',
                'user_id',
                'level',
                'song_id',
                'artist_id',
                'session_id',
                'artist_name'
            ]
        }
    }
]

# Dimension tables configuration
DIMENSION_CONFIG = [
    {
        'table': 'dim_users',
        'sql_query': SqlQueries.user_table_insert,
        'load_mode': 'merge',
        'pk_columns': ['user_id'],
        'checks': {
            'unique': ['user_id'],
            'not_null': ['user_id', 'first_name', 'last_name', 'level']
        }
    },
    {
        'table': 'dim_songs',
        'sql_query': SqlQueries.song_table_insert,
        'load_mode': 'full_refresh',
        'checks': {
            'unique': ['song_id'],
            'not_null': ['song_id', 'title', 'artist_id', 'duration'],
            'greater_than_zero': ['duration']
        }
    },
    {
        'table': 'dim_artists',
        'sql_query': SqlQueries.artist_table_insert,
        'load_mode': 'full_refresh',
        'checks': {
            'unique': ['artist_sk'],
            'not_null': ['artist_sk', 'artist_id', 'name']
        }
    },
    {
        'table': 'dim_times',
        'sql_query': SqlQueries.time_table_insert,
        'load_mode': 'partition_overwrite',
        'checks': {
            'unique': ['start_time'],
            'not_null': ['start_time'],
            'greater_than_zero': ['day', 'week', 'month', 'year']
        }
    }
]

# Connection identifiers
REDSHIFT_CONN_ID = 'redshift'
AWS_CONN_ID = 'aws_credentials'
S3_REGION = 'us-east-1'