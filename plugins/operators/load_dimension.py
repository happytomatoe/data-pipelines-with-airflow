from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries


class LoadDimensionOperator(BaseOperator):
    modes = ['append', 'delete-load']
    ui_color = '#80BD9E'
    dimensions = {
        "users": SqlQueries.user_table_insert.format("users"),
        "time": SqlQueries.time_table_insert.format("time"),
        "artists": SqlQueries.artist_table_insert.format("artists"),
        "songs": SqlQueries.song_table_insert.format("songs"),
    }

    @apply_defaults
    def __init__(self, dimension: str, redshift_conn_id: str, mode: str, *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.dimension = dimension
        self.redshift_conn_id = redshift_conn_id
        self.mode = mode

    def execute(self, context):
        if self.mode not in self.modes:
            raise ValueError(
                f"Cannot find mode '{self.modes}'. Available values - {', '.join(self.modes)}")
        if self.dimension not in self.dimensions:
            raise ValueError(
                f"Cannot find dimension '{self.dimension}'. Available values - "
                f"{', '.join(self.dimensions.keys())}")

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        sql = self.dimensions[self.dimension]

        if self.mode == 'delete-load':
            redshift_hook.run(f"TRUNCATE TABLE {self.dimension}", False)
            for output in redshift_hook.conn.notices:
                self.log.info(output)

        redshift_hook.run(sql, True)
        for output in redshift_hook.conn.notices:
            self.log.info(output)
