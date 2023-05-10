import boto3  # REQUIRED! - Details here: https://pypi.org/project/boto3/
from botocore.exceptions import ClientError
from botocore.config import Config
# Project Must install Python Package:  python-dotenv
from dotenv import load_dotenv
import os
import datetime
from time import sleep
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler
from logging.handlers import TimedRotatingFileHandler
# Return a boto3 resource object for s3 service

class Mylogger:
    def __init__(self, logger_name, log_file_path, log_level=logging.INFO, log_rotation_days=7):
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(log_level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        # create file handler which logs even debug messages
        # file_handler = RotatingFileHandler(log_file_path, maxBytes=1024 * 1024 * 5, backupCount=5)
        file_handler = TimedRotatingFileHandler(log_file_path, when='D', interval=1, backupCount=log_rotation_days)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        # create console handler with a higher log level
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def info(self, msg):
        self.logger.info(msg)
    
    def error(self, msg):
        self.logger.error(msg)
        
logger=Mylogger('mylogger', 'log.txt')



def get_s3_resource(endpoint, key_id, application_key):
    """
    Return a boto3 S3 resource object for the specified endpoint and credentials
    """
    s3 = boto3.resource(service_name='s3',
                        endpoint_url=endpoint,     # Backblaze endpoint
                        aws_access_key_id=key_id,  # Backblaze keyID
                        aws_secret_access_key=application_key,
                                                   # Backblaze applicationKey
                        config=Config(
                            signature_version='s3v4'))
    return s3


def get_s3_client(endpoint, key_id, application_key):
    """
    Return a boto3 S3 client object for the specified endpoint and credentials
    """
    s3_client = boto3.client(service_name='s3',
                             endpoint_url=endpoint,                # Backblaze endpoint
                             aws_access_key_id=key_id,              # Backblaze keyID
                             aws_secret_access_key=application_key)  # Backblaze applicationKey
    return s3_client


def list_object_keys(bucket, s3, prefix, d):
    """
    List object keys in the specified bucket with the given prefix that were last modified before the specified date
    """
    try:
        logger.info('connecting....')
        response = s3.Bucket(bucket).objects.filter(Prefix=prefix)
        return_list = []               # create empty list
        for object in response:        # iterate over response
            if object.last_modified.replace(tzinfo=None) <= d:
                # if object.last_modified.replace(tzinfo = None) < datetime.datetime(2022,12,31,tzinfo = None):
                # print results
                #print('File Name: %s ---- Date: %s' % (object.key, object.size))
                # for each item in response, append object.key to list
                return_list.append(object.key)
        return return_list                  # return list of keys from response
        print('get objects suscessful')
    except ClientError as ce:
        print('error', ce)

# Delete the specified objects from s3


def delete_files(bucket, keys, s3):
    objects = []
    for key in keys:
        print(f'add for delete {key}')
        objects.append({'Key': key})
    if objects != []:
        try:
            print(f'deleting {keys}')
            s3.Bucket(bucket).delete_objects(Delete={'Objects': objects})
        except ClientError as ce:
            print('error', ce)

# Delete the specified object from s3 - all versions


def delete_files_all_versions(bucket, keys, client, prefix):
    try:
        # SOURCE re LOGIC FOLLOWING:  https://stackoverflow.com/questions/46819590/delete-all-versions-of-an-object-in-s3-using-python
        paginator = client.get_paginator('list_object_versions')
        response_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
        for response in response_iterator:
            versions = response.get('Versions', [])
            versions.extend(response.get('DeleteMarkers', []))
            for key in keys:
                for version_id in [x['VersionId'] for x in versions if x['Key'] == key and x['VersionId'] != 'null']:
                    print('Deleting {} version {}'.format(key, version_id))
                    client.delete_object(
                        Bucket=bucket, Key=key, VersionId=version_id)

    except ClientError as ce:
        print('error', ce)


def main(bucketname, endpoint, keyid, appkey, prefix):
    load_dotenv()  # take environment variables from .env.
    # get environment variables from file .env
    private_bucket_name = os.getenv(bucketname)  # Backblaze endpoint
    endpoint = os.getenv(endpoint)  # Backblaze endpoint
    key_id_private_ro = os.getenv(keyid)  # Backblaze keyID
    application_key_private_ro = os.getenv(appkey)  # Backblaze applicationKey
    prefix = os.getenv(prefix)

    # Call function to return reference to s3 service using a set of keys
    s3_private = get_s3_resource(
        endpoint, key_id_private_ro, application_key_private_ro)
    s3_private_client = get_s3_client(
        endpoint, key_id_private_ro, application_key_private_ro)
    day_to_delete = datetime.today() - timedelta(days=15)

    bucket_object_keys = list_object_keys(
        private_bucket_name, s3_private, prefix, day_to_delete)
    print('BEFORE - Bucket Contents ')
    for key in bucket_object_keys:
        print(key)
    #sleep(160)
    #delete_files(private_bucket_name, bucket_object_keys, s3_private)
    delete_files_all_versions(
        private_bucket_name, bucket_object_keys, s3_private_client, prefix)
    print('\nAFTER - Bucket Contents ')
    my_bucket = s3_private.Bucket(private_bucket_name)
    for my_bucket_object in my_bucket.objects.filter(Prefix=prefix):
        print(my_bucket_object.key)


main("BUCKETNAME", "ENDPOINT", "KEY_ID_PRIVATE_RO", "APPLICATION_KEY_PRIVATE_RO", "PREFIX")
sleep(5)
main("BUCKETNAMEARDIS", "ENDPOINT", "KEY_ID_PRIVATE_RO_ARDIS", "APPLICATION_KEY_PRIVATE_RO_ARDIS", "PREFIXARDIS")
