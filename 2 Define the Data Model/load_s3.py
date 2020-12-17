
import boto3
import configparser


def upload_s3():
    """
    Moves datasets to S3. This function load the prepared_datasets to Amazon S3
    """
    config = configparser.ConfigParser()
    config.read_file(open('aws.cfg'))

    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')
    BUCKET = config.get('AWS', 'BUCKET_NAME')
    REGION = config.get('AWS', 'REGION')

    s3 = boto3.resource('s3',
                        region_name=REGION,
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        ).meta.client
    s3.upload_file('../prepared_datasets/cities.csv', BUCKET, 'countries.csv')
    s3.upload_file('../prepared_datasets/accidents.csv', BUCKET, 'accidents.csv')


if __name__ == '__main__':
    upload_s3()
