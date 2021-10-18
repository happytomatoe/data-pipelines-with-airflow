from textwrap import dedent

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.dimensions = {
            "users": SqlQueries.user_table_insert,
            "time": SqlQueries.time_table_insert,
            "artists": SqlQueries.artist_table_insert,
            "songs": SqlQueries.song_table_insert,
        }
        self.dimension = kwargs['dimension_name']
        self.redshift_conn_id = kwargs['redshift_conn_id']

    def execute(self, context):
        if self.dimension not in self.dimensions:
            raise ValueError(
                f"Cannot find dimension '{self.dimension}' with such name. Available values - "
                f"{', '.join(self.dimensions.keys())}")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        sql = self.dimensions[self.dimension]
        self.log.info(dedent(f"Executing {sql}"))

        redshift_hook.run(sql, True)
        for output in redshift_hook.conn.notices:
            self.log.info(output)
