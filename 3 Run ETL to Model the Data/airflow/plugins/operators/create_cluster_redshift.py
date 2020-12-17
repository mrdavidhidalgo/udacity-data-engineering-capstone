import configparser
import boto3
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook


class CreateRedshiftClusterOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 aws_credentials="",
                 *args, **kwargs):

        super(CreateRedshiftClusterOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.aws_credentials = aws_credentials

    def execute(self, context):
        aws_connection = BaseHook.get_connection(self.aws_credentials)
        redshift = PostgresHook(self.conn_id)
        redshift_connection = redshift.get_connection(self.conn_id)

        config = configparser.ConfigParser()
        config.read_file(open('/usr/local/airflow/plugins/operators/aws.cfg'))
        KEY = aws_connection.login
        SECRET = aws_connection.password
        REGION = config.get('AWS', 'REGION')
        VPC_SECUTIRY_GROUPS = config.get('AWS', 'VPC_SECUTIRY_GROUPS')
        REDSHIFT_CLUSTER_TYPE = config.get("REDSHIFT", "CLUSTER_TYPE")
        REDSHIFT_NODE_TYPE = config.get("REDSHIFT", "NODE_TYPE")
        REDSHIFT_CLUSTER_IDENTIFIER = config.get("REDSHIFT", "CLUSTER_ID")
        REDSHIFT_DB = redshift_connection.login
        REDSHIFT_DB_USER = redshift_connection.login
        REDSHIFT_DB_PASSWORD = redshift_connection.password
        REDSHIFT_ROLE_NAME = config.get("REDSHIFT", "S3_ROLE")

        redshift = boto3.client('redshift',
                            region_name=REGION,
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )

        response = redshift.create_cluster(

            ClusterType=REDSHIFT_CLUSTER_TYPE,
            NodeType=REDSHIFT_NODE_TYPE,
            DBName=REDSHIFT_DB,
            ClusterIdentifier=REDSHIFT_CLUSTER_IDENTIFIER,
            MasterUsername=REDSHIFT_DB_USER,
            MasterUserPassword=REDSHIFT_DB_PASSWORD,
            IamRoles=[REDSHIFT_ROLE_NAME],
            VpcSecurityGroupIds = [VPC_SECUTIRY_GROUPS]
         )
        self.log.info(response)