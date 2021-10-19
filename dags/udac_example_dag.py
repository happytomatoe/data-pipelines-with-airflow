from datetime import datetime
from textwrap import dedent

from airflow import DAG
from airflow.models import Variable
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator,
                               DataQualityOperator)
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

REDSHIFT_CONN_ID = Variable.get("redshift_conn_id", "redshift")
S3_BUCKET = Variable.get("s3_bucket", "udacity-dend")
LOG_DATA_S3_KEY = Variable.get("log_data_s3_key", "log_data")
SONG_DATA_S3_KEY = Variable.get("song_data_s3_key", "song_data/A/A/C")

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 12),
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
}

with DAG('udac_example_dag',
         default_args=default_args,
         description='Load and transform data in Redshift with Airflow',
         schedule_interval='0 * * * *',
         catchup=False,
         ) as dag:
    start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

    create_tables = PostgresOperator(
        task_id="create_tables",
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql="sql/create_tables.sql"
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        s3_bucket=S3_BUCKET,
        s3_key=LOG_DATA_S3_KEY,
        schema="public",
        table="staging_events",
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_conn_id="aws_credentials",
        copy_options=dedent("""
        COMPUPDATE OFF STATUPDATE OFF
        FORMAT AS JSON 'auto ignorecase'
        TIMEFORMAT AS 'epochmillisecs'
        TRUNCATECOLUMNS
        BLANKSASNULL;
        """),
        dag=dag
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        s3_bucket=S3_BUCKET,
        s3_key=SONG_DATA_S3_KEY,
        schema="public",
        table="staging_songs",
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_conn_id="aws_credentials",
        copy_options=dedent("""
        COMPUPDATE OFF STATUPDATE OFF
        FORMAT AS JSON 'auto'
        TRUNCATECOLUMNS
        BLANKSASNULL;
        """),
        dag=dag
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        table_name="songplays",
        redshift_conn_id=REDSHIFT_CONN_ID,
        dag=dag
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        dimension="users",
        redshift_conn_id=REDSHIFT_CONN_ID,
        mode="append",
        dag=dag
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        dimension="songs",
        redshift_conn_id=REDSHIFT_CONN_ID,
        mode="append",
        dag=dag
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        dimension="artists",
        redshift_conn_id=REDSHIFT_CONN_ID,
        mode="append",
        dag=dag
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        dimension="time",
        redshift_conn_id=REDSHIFT_CONN_ID,
        mode="append",
        dag=dag
    )
    #
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id=REDSHIFT_CONN_ID,
        tables=['users', 'artists', 'songs', 'time', 'songplays'],
        dag=dag
    )
    # For testing purposes
    drop_tables = PostgresOperator(
        task_id="drop_tables",
        dag=dag,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=[
            'DROP SCHEMA public CASCADE;',
            'CREATE SCHEMA public;'
        ]
    )

    end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

    start_operator >> create_tables \
    >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table \
    >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table,
        load_time_dimension_table] >> run_quality_checks >> end_operator
