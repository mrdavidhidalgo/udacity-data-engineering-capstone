import configparser
import boto3


def create_cluster():
    """
    Create a Redshift cluster using infrastructure as code,
    credentials are in file aws.cfg
    """
    config = configparser.ConfigParser()
    config.read_file(open('aws.cfg'))
    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')
    REGION = config.get('AWS', 'REGION')
    VPC_SECUTIRY_GROUPS = config.get('AWS', 'VPC_SECUTIRY_GROUPS')
    REDSHIFT_CLUSTER_TYPE = config.get("REDSHIFT", "CLUSTER_TYPE")
    REDSHIFT_NUM_NODES = config.get("REDSHIFT", "NUM_NODES")
    REDSHIFT_NODE_TYPE = config.get("REDSHIFT", "NODE_TYPE")
    REDSHIFT_CLUSTER_IDENTIFIER = config.get("REDSHIFT", "CLUSTER_ID")
    REDSHIFT_DB = config.get("REDSHIFT", "DB_NAME")
    REDSHIFT_DB_USER = config.get("REDSHIFT", "DB_USER")
    REDSHIFT_DB_PASSWORD = config.get("REDSHIFT", "DB_PASSWORD")
    REDSHIFT_ROLE_NAME = config.get("REDSHIFT", "S3_ROLE")

    redshift = boto3.client('redshift',
                            region_name=REGION,
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )

    response = redshift.create_cluster(

        ClusterType=REDSHIFT_CLUSTER_TYPE,
        NodeType=REDSHIFT_NODE_TYPE,
       # NumberOfNodes=int(REDSHIFT_NUM_NODES),
        DBName=REDSHIFT_DB,
        ClusterIdentifier=REDSHIFT_CLUSTER_IDENTIFIER,
        MasterUsername=REDSHIFT_DB_USER,
        MasterUserPassword=REDSHIFT_DB_PASSWORD,
        IamRoles=[REDSHIFT_ROLE_NAME],
        VpcSecurityGroupIds = [VPC_SECUTIRY_GROUPS]
    )
    print(response)


if __name__ == '__main__':
    create_cluster()
