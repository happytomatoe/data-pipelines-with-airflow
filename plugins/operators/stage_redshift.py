from textwrap import dedent

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
        Copies data from s3 to redshift using `Copy Command <https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-source-s3.html/>`_.

        :param redshift_conn_id - redshift connection id
        :type redshift_conn_id: str
        :param aws_key - aws key used to access objects in s3
        :type aws_conn_id: str
        :param aws_secret_key - aws secret used to access objects in s3
        :type aws_secret_key: str
        :param s3_bucket: s3 bucket
        :type s3_bucket: str
        :param s3_key: s3 key/prefix. This field is templated. It works with jinja templates
        :type s3_key: str
        :param schema: redshift schema name
        :type schema: str
        :param table: redshift table name
        :type table: str
        :param copy_options: copy command options
        
    """
    COPY_QUERY_TEMPLATE = dedent("""
            COPY {schema}.{table}
            FROM 's3://{s3_bucket}/{s3_key}'
            WITH CREDENTIALS
            'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
            {copy_options}
        """)

    ui_color = '#358140'
    template_fields = ["s3_key"]

    @apply_defaults
    def __init__(self, redshift_conn_id, aws_key, aws_secret_key, s3_bucket, s3_key, schema,
                 table, copy_options, *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_key = aws_key
        self.aws_secret_key = aws_secret_key
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.schema = schema
        self.table = table
        self.copy_options = copy_options

    def execute(self, context):
        self.log.info(f"Load path {self.s3_key}")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        copy_query = StageToRedshiftOperator.COPY_QUERY_TEMPLATE.format(schema=self.schema,
                                                                        table=self.table,
                                                                        s3_bucket=self.s3_bucket,
                                                                        s3_key=self.s3_key,
                                                                        access_key=self.aws_key,
                                                                        secret_key=self.aws_secret_key,
                                                                        copy_options=self.copy_options)

        redshift_hook.run(copy_query, True)
        for output in redshift_hook.conn.notices:
            self.log.info(output)
