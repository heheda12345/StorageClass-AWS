import boto3
import os
import logging
from DynamoAdaptor import DynamoAdaptor
# from elasticsearch import Elasticsearch, RequestsHttpConnection
# from aws_requests_auth.aws_auth import AWSRequestsAuth

logger = logging.getLogger()
# make logger available in AWS
if logger.handlers:
    for handler in logger.handlers:
        logger.removeHandler(handler)
logging.basicConfig(format='%(asctime)s %(message)s',level=logging.DEBUG)

dynamoAdaptor = DynamoAdaptor(logger)

def lambda_handler(event, context):
    """Lambda Function entrypoint handler

    :event: S3 Put event
    :context: Lambda context
    :returns: Number of records processed

    """
    processed = 0

    for record in event['Records']:
        dynamoAdaptor.handle(record)
        processed = processed + 1

    logger.info('Successfully processed {} records'.format(processed))
    return processed
