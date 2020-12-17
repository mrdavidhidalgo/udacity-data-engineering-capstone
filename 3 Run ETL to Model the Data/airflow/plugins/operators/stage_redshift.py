from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 aws_credentials="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = conn_id
        self.aws_credentials = aws_credentials
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        """
        Load data from S3 to Redshift,
        load a staging table using datasets stored in S3
        """
        connection = BaseHook.get_connection(self.aws_credentials)

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift_copy = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            IGNOREHEADER 1
            DELIMITER ','
            TRUNCATECOLUMNS
         """
        s3_location = f"s3://{self.s3_bucket}/{self.s3_key}"
        self.log.info(f'The S3 location: {s3_location}')
        sql = redshift_copy.format(
                self.table,
                s3_location,
                connection.login,#'AKIA56ZGMRFF3HEZQDBV',
                connection.password #'/1XTCzSZZ4myh5oaAxxAy5SXAKUyjK3awl+Q5Xmt'
        )
        self.log.info(f'Executing SQL: {redshift_copy}')
        redshift_hook.run(sql)
