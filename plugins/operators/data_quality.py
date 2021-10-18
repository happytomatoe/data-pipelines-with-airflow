from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = kwargs['redshift_conn_id']

    def execute(self, context):
        self.check_greater_than_zero("users")
        self.check_greater_than_zero("artists")
        self.check_greater_than_zero("time")
        self.check_greater_than_zero("songplays")
        self.check_greater_than_zero("songs")

    def check_greater_than_zero(self, table):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {table} table returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {table} table is empty")
        self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
    # TODO: add check if tables contain duplicates
