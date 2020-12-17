import configparser
import boto3


def delete_cluster():
    config = configparser.ConfigParser()
    config.read_file(open('aws.cfg'))
    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')
    REGION = config.get('AWS', 'REGION')
    REDSHIFT_CLUSTER_IDENTIFIER = config.get("REDSHIFT", "CLUSTER_ID")

    redshift = boto3.client('redshift',
                            region_name=REGION,
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )
    response = redshift.delete_cluster(
    ClusterIdentifier=REDSHIFT_CLUSTER_IDENTIFIER,
    SkipFinalClusterSnapshot=True)
    print(response)


if __name__ == '__main__':
    delete_cluster()
