import logging
import boto3
import os

from botocore.exceptions import ClientError

# from s3transfer import TransferConfig

session = boto3.session.Session()
s3 = session.client(
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.net'
)


def create_bucket(bucket_name):
    try:
        s3.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def print_all_buckets():
    response = s3.list_buckets()
    print('Existing buckets:')
    for bucket in response['Buckets']:
        print(f'  {bucket["Name"]}')


def upload_file(file_name, bucket, object_name=None):
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # mb = 1024 ** 2
    # config = TransferConfig(multipart_threshold=10 * mb, max_concurrency=10)

    try:
        response = s3.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def download_file(bucket_name, obj_name, file_name):
    s3.download_file(bucket_name, obj_name, file_name)


print_all_buckets()
# create_bucket("bnb-bucket")
# print_all_buckets()
# upload_file("file.txt", "eth-bucket", "file-eth.txt")
# upload_file("file.shadow", "bnb-bucket", "file-bnb.txt")
# download_file("eth-bucket", "file-eth.txt", "file.txt")
# download_file("bnb-bucket", "file-bnb.txt", "file-bnb.txt")
