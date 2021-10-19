from textwrap import dedent

from airflow.hooks.base_hook import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

COPY_QUERY_TEMPLATE = dedent("""
            COPY {schema}.{table}
            FROM 's3://{s3_bucket}/{s3_key}'
            WITH CREDENTIALS
            'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
            {copy_options}
        """)

#TODO: The parameters should be used to distinguish between JSON file. Another important
# requirement of the stage operator is containing a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.
class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self, redshift_conn_id, aws_conn_id, s3_bucket, s3_key, schema,
                 table, copy_options, *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.schema = schema
        self.table = table
        self.copy_options = copy_options

    #TODO: {{ execution_date }}

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        aws_connection = BaseHook.get_connection(self.aws_conn_id)
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
