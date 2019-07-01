# Ref: https://docs.aws.amazon.com/elasticsearch-service/latest/developerguide/es-managedomains-snapshots.html
# Install required libraries
# python3 -m pip install elasticsearch-curator --user
# python3 -m pip install AWS4Auth --user

import boto3
import logging
import requests
import os
from requests_aws4auth import AWS4Auth

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Environment Variable Configuration
# e.g. ap-southeast-1
AWS_CURRENT_REGION = os.environ['AWS_CURRENT_REGION']
# Include https:// and trailing /
AWS_ES_HOST_NAME = os.environ['AWS_ES_HOST_NAME']
# Snapshot name
SNAPSHOT_NAME = os.environ['SNAPSHOT_NAME']
# Want to delete Index name
DELETE_INDEX_NAME = os.environ['DELETE_INDEX_NAME']
# S3 bucket to store ES napshot
AWS_S3_BUCKET_NAME = os.environ['AWS_S3_BUCKET_NAME']
# Role for lambda
AWS_ROLE_ARN = os.environ['AWS_ROLE_ARN']
# Expected Operation:
#           take_snapshot
#           delete_index
#           restore_snapshot_all_index
#           restore_snapshot_with_index
ES_EXPECTED_OPERATION = os.environ['ES_EXPECTED_OPERATION']

service = 'es'
credentials = boto3.Session().get_credentials()
aws_auth = AWS4Auth(credentials.access_key, credentials.secret_key, AWS_CURRENT_REGION, service,
                    session_token=credentials.token)


def register_repo(es_hostname, s3_bucket_name, role_arn, aws_region, aws_authen):
    """
    Register repository.
    :param es_hostname:
    :param s3_bucket_name:
    :param role_arn:
    :param aws_region:
    :return:
    """
    # the ElasticSearch API endpoint
    path = '_snapshot/my-snapshot-repo'
    url = es_hostname + path

    payload = {
      "type": "s3",
      "settings": {
        "bucket": s3_bucket_name,
        "region": aws_region,
        "role_arn": role_arn
      }
    }

    headers = {"Content-Type": "application/json"}

    response = requests.put(url, auth=aws_authen, json=payload, headers=headers)

    logger.info(response.status_code)
    logger.info(response.text)
    return response


def take_snapshot(es_hostname, snapshot_name, aws_authen):
    """
    Take snapshot
    :param es_hostname:
    :param snapshot_name:
    :param aws_authen:
    :return:
    """
    # Take snapshot

    path = '_snapshot/my-snapshot-repo/' + snapshot_name
    url = es_hostname + path

    response = requests.put(url, auth=aws_authen)

    logger.info(response.text)
    return response


def delete_index(es_hostname, index_name, aws_authen):
    """
    Do delete index
    :param es_hostname:
    :param index_name:
    :param aws_authen:
    :return:
    """

    # Delete index

    path = index_name
    url = es_hostname + path

    response = requests.delete(url, auth=aws_authen)

    logger.info(response.text)
    return response


def restore_snapshot_all_index(es_hostname, snapshot_name, aws_authen):
    """
    Do restore with all index.
    :param es_hostname:
    :param snapshot_name:
    :param aws_authen:
    :return:
    """
    # Restore snapshots (all indices)

    path = '_snapshot/my-snapshot-repo/'+snapshot_name+'/_restore'
    url = es_hostname + path

    response = requests.post(url, auth=aws_authen)

    logger.info(response.text)
    return response


def restore_snapshot_with_index(es_hostname, snapshot_name, index_name, aws_authen):
    """
    Do restore snapshot with specify index
    :param es_hostname:
    :param snapshot_name:
    :param aws_authen:
    :return:
    """

    # Restore snapshot (one index)

    path = '_snapshot/my-snapshot-repo/'+snapshot_name+'/_restore'
    url = es_hostname + path

    payload = {"indices": index_name}

    headers = {"Content-Type": "application/json"}

    response = requests.post(url, auth=aws_authen, json=payload, headers=headers)

    logger.info(response.text)
    return response


def lambda_handler(event, context):
    """
    Main function for aws lambda execute
    :param event:
    :param context:
    :return:
    """
    logger.debug('Start job')
    try:
        logger.debug("Do " + ES_EXPECTED_OPERATION)
#           take_snapshot
#           delete_index
#           restore_snapshot_all_index
#           restore_snapshot_with_index
        if ES_EXPECTED_OPERATION == 'take_snapshot':
            register_repo(AWS_ES_HOST_NAME, AWS_S3_BUCKET_NAME, AWS_ROLE_ARN, AWS_CURRENT_REGION, aws_auth)
            take_snapshot(AWS_ES_HOST_NAME, SNAPSHOT_NAME, aws_auth)
            logger.info('Do take_snapshot')
        elif ES_EXPECTED_OPERATION == 'delete_index':
            logger.info('Do delete_index')
            delete_index(AWS_ES_HOST_NAME, DELETE_INDEX_NAME, aws_auth)
        elif ES_EXPECTED_OPERATION == 'restore_snapshot_all_index':
            logger.info('Do restore_snapshot_all_index')
            restore_snapshot_all_index(AWS_ES_HOST_NAME, SNAPSHOT_NAME, aws_auth)
        elif ES_EXPECTED_OPERATION == 'restore_snapshot_with_index':
            logger.info('Do restore_snapshot_with_index')
            restore_snapshot_with_index(AWS_ES_HOST_NAME, SNAPSHOT_NAME, DELETE_INDEX_NAME, aws_auth)
    except ValueError:
        logger.error('Error when process {}', ValueError)

    logger.debug('Ended job')


if __name__ == '__main__':
    lambda_handler(None, None)
