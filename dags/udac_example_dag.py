from datetime import datetime
from textwrap import dedent

from airflow import DAG
from airflow.operators import (StageToRedshiftOperator)
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

S3_BUCKET = "udacity-dend"

LOG_DATA_S3_KEY = "log_data"
SONG_DATA_S3_KEY = "song_data/A/A/C"

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
}

# Copied from https://stackoverflow.com/questions/42982986/external-files-in-airflow-dag/46091929#46091929
# tmpl_search_path = Variable.get("sql_path")
# The DAG does not have dependencies on past runs
# On failure, the task are retried 3 times
# Retries happen every 5 minutes
# Catchup is turned off
# Do not email on retry

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          #           schedule_interval='0 * * * *',
          catchup=False,
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_tables = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="sql/create_tables.sql"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    s3_bucket=S3_BUCKET,
    s3_key=LOG_DATA_S3_KEY,
    schema="PUBLIC",
    table="staging_events",
    redshift_conn_id="redshift",
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
    schema="PUBLIC",
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    copy_options=dedent("""
    COMPUPDATE OFF STATUPDATE OFF
    FORMAT AS JSON 'auto'
    TRUNCATECOLUMNS
    BLANKSASNULL;
    """),
    dag=dag
)
#
# load_songplays_table = LoadFactOperator(
#     task_id='Load_songplays_fact_table',
#     dag=dag
# )
#
# load_user_dimension_table = LoadDimensionOperator(
#     task_id='Load_user_dim_table',
#     dag=dag
# )
#
# load_song_dimension_table = LoadDimensionOperator(
#     task_id='Load_song_dim_table',
#     dag=dag
# )
#
# load_artist_dimension_table = LoadDimensionOperator(
#     task_id='Load_artist_dim_table',
#     dag=dag
# )
#
# load_time_dimension_table = LoadDimensionOperator(
#     task_id='Load_time_dim_table',
#     dag=dag
# )
#
# run_quality_checks = DataQualityOperator(
#     task_id='Run_data_quality_checks',
#     dag=dag
# )

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> stage_events_to_redshift >> end_operator
#     [stage_songs_to_redshift]
# ,stage_events_to_redshift
# >> load_songplays_table \
# >>[load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table,load_time_dimension_table] \
# >> run_quality_checks >> end_operator
