# Author: TuanBA
import boto3
import logging
import time
import csv
import json
from decimal import *

AWS_DYNAMO_DB_SERVICE_NAME = 'dynamodb'

# Change value of this, then run
# AWS INFO. You can move it to environment parameters
AWS_CURRENT_REGION = 'ap-northeast-1'
AWS_DYNAMO_TABLE_NAME = 'DYNAMODB_TABLE_NAME_ENV_PARAMETER'
# Input csv file. This file must following the export csv format of Dynamodb
CSV_FILE_NAME = './INPUT_FILE_NAME.csv'  

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# DynamoDB setting
dynamo_resource = boto3.resource(AWS_DYNAMO_DB_SERVICE_NAME, region_name=AWS_CURRENT_REGION)
dynamo_tbl = dynamo_resource.Table(AWS_DYNAMO_TABLE_NAME)


def read_and_extract_csv_data(csv_file_path, delimiter=','):
    """
    Read csv file and then extract into list.
    :param csv_file_path:
    :param delimiter:
    :return:
    """
    csv_content = {}
    csv_header_list = []
    csv_data_list = []

    # read csv, and split on "," the line
    csv_file = csv.reader(open(csv_file_path, "rt"), delimiter=delimiter)
    # loop through csv list
    i = 1
    for row in csv_file:
        if 1 < i:
            individual_data = {}
            j = 0
            for header in csv_header_list:
                # Construct individual data from each row of csv, except the first list
                individual_data[str(header)] = row[j]
                j = j + 1
            # Add into list
            csv_data_list.append(individual_data)
        elif 1 == i:
            for col in row:
                csv_header_list.append(col)

        # Increase count by 1 for each loop
        i = i + 1

    csv_content['headers'] = csv_header_list
    csv_content['data'] = csv_data_list

    return csv_content


PROPERTY_STRING = ' (S)'
PROPERTY_NUMBER = ' (N)'
PROPERTY_BINARY = ' (B)'
PROPERTY_NULL = ' (NULL)'
PROPERTY_BOOLEAN = ' (BOOL)'
PROPERTY_LIST = ' (L)'
PROPERTY_NUMBER_SET = ' (NS)'
PROPERTY_MAP = ' (M)'
PROPERTY_STRING_SET = ' (SS)'
PROPERTY_BINARY_SET = ' (BS)'


def extract_property_data_following_data_type(raw_header, raw_data):
    """
    Uses to extract from raw data to needed data.
    :param raw_header:
    :param raw_data:
    :return:
    """
    if raw_data is None or raw_data == '':
        return None

    result = {}
    raw_header = str(raw_header)
    if raw_header.endswith(PROPERTY_STRING):
        result['header'] = raw_header.replace(PROPERTY_STRING, '')
        result['data'] = str(raw_data)
    elif raw_header.endswith(PROPERTY_NUMBER):
        result['header'] = raw_header.replace(PROPERTY_NUMBER, '')
        result['data'] = Decimal(str(raw_data))
    elif raw_header.endswith(PROPERTY_NULL):
        result['header'] = raw_header.replace(PROPERTY_NULL, '')
        result['data'] = True
    elif raw_header.endswith(PROPERTY_BOOLEAN):
        result['header'] = raw_header.replace(PROPERTY_BOOLEAN, '')
        result['data'] = json.loads(raw_data)
    elif raw_header.endswith(PROPERTY_NUMBER_SET):
        result['header'] = raw_header.replace(PROPERTY_NUMBER_SET, '')
        result['data'] = set([Decimal(s) for s in str(raw_data).replace(',', '').replace('{', '').replace('}', '').split() if isinstance(Decimal(s),(Decimal))])
    elif raw_header.endswith(PROPERTY_STRING_SET):
        result['header'] = raw_header.replace(PROPERTY_STRING_SET, '')
        result['data'] = set([str(s[1:-1]) for s in raw_data.strip()[1:-1].strip().split(', ')])
    elif raw_header.endswith(PROPERTY_BINARY):
        result['header'] = raw_header.replace(PROPERTY_BINARY, '')
        result['data'] = bytes(raw_data, encoding='utf-8')
    elif raw_header.endswith(PROPERTY_BINARY_SET):
        result['header'] = raw_header.replace(PROPERTY_BINARY_SET, '')
        result['data'] = set([bytes(str(s[1:-1]), encoding='utf-8') for s in raw_data.strip()[1:-1].strip().split(', ')])

    # Not good for those below type
    elif raw_header.endswith(PROPERTY_MAP):
        result['header'] = raw_header.replace(PROPERTY_MAP, '')
        result['data'] = json.loads(raw_data)
    elif raw_header.endswith(PROPERTY_LIST):
        result['header'] = raw_header.replace(PROPERTY_LIST, '')
        result['data'] = raw_data
    return result


def lambda_handler(event, context):
    """
    Main function for aws lambda execute
    :param event:
    :param context:
    :return:
    """
    current_timestamp = int(round(time.time() * 1000))
    # Debug
    logger.debug('Start job at: %d', current_timestamp)

    csv_content = read_and_extract_csv_data(csv_file_path=CSV_FILE_NAME, delimiter=',')
    logger.debug(csv_content)

    # Do loop to put data into dynamo
    with dynamo_tbl.batch_writer() as batch:
        for individual_data in csv_content['data']:
            logger.debug(individual_data)
            try:
                item = {}
                for i in range(len(csv_content['headers'])):
                    # logger.debug(csv_content['headers'][i])
                    # logger.debug(individual_data[csv_content['headers'][i]])

                    property_data = \
                        extract_property_data_following_data_type(raw_header=csv_content['headers'][i],
                                                                  raw_data=individual_data[csv_content['headers'][i]])
                    logger.debug(property_data)

                    if property_data is not None:
                        item[property_data['header']] = property_data['data']

                logger.debug("item will be push")
                logger.debug(item)
                batch.put_item(
                    Item=item
                )
            except ValueError:
                logger.error('Error when process {}', ValueError)

    logger.debug('End job')
    return "done"


if __name__ == '__main__':
    lambda_handler(None, None)
