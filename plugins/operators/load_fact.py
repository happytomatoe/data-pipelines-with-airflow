from textwrap import dedent

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers.sql_queries import SqlQueries


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = kwargs['redshift_conn_id']

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(dedent(f"Executing {SqlQueries.songplay_table_insert}"))

        redshift_hook.run(SqlQueries.songplay_table_insert, True)
        for output in redshift_hook.conn.notices:
            self.log.info(output)

        self.log.info('Finished')
