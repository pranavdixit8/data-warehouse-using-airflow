from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
# from helpers import CreateQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

##   NEED HELP HERE, I am trying to read the variables for create table strings from sql_queries.py file but not able to get the variable here and hence 
## defining them here
create_table_staging_events = ("""
    CREATE TABLE IF NOT EXISTS public.staging_events (
        artist varchar(256),
        auth varchar(256),
        firstname varchar(256),
        gender varchar(256),
        iteminsession int4,
        lastname varchar(256),
        length numeric(18,0),
        "level" varchar(256),
        location varchar(256),
        "method" varchar(256),
        page varchar(256),
        registration numeric(18,0),
        sessionid int4,
        song varchar(256),
        status int4,
        ts int8,
        useragent varchar(256),
        userid int4
    );
    """)
    
create_table_staging_songs = ("""
    CREATE TABLE IF NOT EXISTS public.staging_songs (
        num_songs int4,
        artist_id varchar(256),
        artist_name varchar(256),
        artist_latitude numeric(18,0),
        artist_longitude numeric(18,0),
        artist_location varchar(256),
        song_id varchar(256),
        title varchar(256),
        duration numeric(18,0),
        "year" int4
    );
    """)

create_table_songplays = """
    CREATE TABLE IF NOT EXISTS public.songplays (
        playid varchar(32) NOT NULL,
        start_time timestamp NOT NULL,
        userid int4 NOT NULL,
        "level" varchar(256),
        songid varchar(256),
        artistid varchar(256),
        sessionid int4,
        location varchar(256),
        user_agent varchar(256),
        CONSTRAINT songplays_pkey PRIMARY KEY (playid)
    );
    """

create_table_users = """
    CREATE TABLE IF NOT EXISTS public.users (
        userid int4 NOT NULL,
        first_name varchar(256),
        last_name varchar(256),
        gender varchar(256),
        "level" varchar(256),
        CONSTRAINT users_pkey PRIMARY KEY (userid)
    );
    """

create_table_time = """
    CREATE TABLE IF NOT EXISTS public.time(
         start_time timestamp primary key , 
         hour int, 
         day int, 
         week int, 
         month int, 
         year int, 
         weekday int
         )  
    """

create_table_songs = """
    CREATE TABLE IF NOT EXISTS public.songs (
        songid varchar(256) NOT NULL,
        title varchar(256),
        artistid varchar(256),
        "year" int4,
        duration numeric(18,0),
        CONSTRAINT songs_pkey PRIMARY KEY (songid)
    );

    """

create_table_artists = """
    CREATE TABLE IF NOT EXISTS public.artists (
        artistid varchar(256) NOT NULL,
        name varchar(256),
        location varchar(256),
        lattitude numeric(18,0),
        longitude numeric(18,0)
    );
    """

star_schema_tables = ["songplays", "songs", "artists", "users", "time"]

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'start_date': datetime(2019, 1, 12),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          catchup = False,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)



stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table = "staging_events",
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    s3_bucket = "udacity-dend",
    s3_key = "log_data",
#     s3_key = "log_data/{execution_date.year}/{execution_date.month}",
    create_table_sql = create_table_staging_events,
    format_mode = "json 's3://udacity-dend/log_json_path.json'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table = "staging_songs",
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    s3_bucket = "udacity-dend",
    s3_key = "song_data",
    create_table_sql = create_table_staging_songs,
    format_mode = "json 'auto'"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table = "songplays",
    redshift_conn_id = "redshift",
    create_table_sql  = create_table_songplays,
    insert_table_sql = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table = "users",
    redshift_conn_id = "redshift",
    create_table_sql  =  create_table_users,
    insert_table_sql = SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table = "songs",
    redshift_conn_id = "redshift",
    create_table_sql  =  create_table_songs,
    insert_table_sql = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table = "artists",
    redshift_conn_id = "redshift",
    create_table_sql  =  create_table_artists,
    insert_table_sql = SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table = "time",
    redshift_conn_id = "redshift",
    create_table_sql  =  create_table_time,
    insert_table_sql = SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    tables = star_schema_tables,
    redshift_conn_id = "redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift

start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table

stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table

load_songplays_table >> load_song_dimension_table

load_songplays_table >>load_artist_dimension_table

load_songplays_table >>load_time_dimension_table

load_user_dimension_table >> run_quality_checks

load_song_dimension_table >> run_quality_checks

load_artist_dimension_table >> run_quality_checks

load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
