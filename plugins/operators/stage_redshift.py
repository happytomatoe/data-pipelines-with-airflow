import logging
from textwrap import dedent

from airflow.hooks.base_hook import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

COPY_QUERY_TEMPLATE = dedent("""
            COPY {schema}.{table}
            FROM 's3://{s3_bucket}/{s3_key}'
            with credentials
            'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
            {copy_options}
        """)


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = kwargs['redshift_conn_id']
        self.aws_conn_id = kwargs['aws_conn_id']
        self.s3_bucket = kwargs['s3_bucket']
        self.s3_key = kwargs['s3_key']
        self.schema = kwargs['schema']
        self.table = kwargs['table']
        self.copy_options = kwargs['copy_options']

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        aws_connection = BaseHook.get_connection(self.aws_conn_id)
        logging.info(f"Aws aws_connection login {aws_connection.login}")
        copy_query = COPY_QUERY_TEMPLATE.format(schema=self.schema,
                                                table=self.table,
                                                s3_bucket=self.s3_bucket,
                                                s3_key=self.s3_key,
                                                access_key=aws_connection.login,
                                                secret_key=aws_connection.password,
                                                copy_options=self.copy_options)

        redshift_hook.run(copy_query, True)
        for output in redshift_hook.conn.notices:
            self.log.info(output)

        self.log.info('Finished')
