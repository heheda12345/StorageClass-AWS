import boto3
import os
import logging
import uuid
# from elasticsearch import Elasticsearch, RequestsHttpConnection
# from aws_requests_auth.aws_auth import AWSRequestsAuth

logger = logging.getLogger()
# make logger available in AWS
if logger.handlers:
    for handler in logger.handlers:
        logger.removeHandler(handler)
logging.basicConfig(format='%(asctime)s %(message)s',level=logging.DEBUG)

# es_host = os.getenv('ELASTICSEARCH_URL')
# es_index = 'images'
access_key = os.getenv('AWS_ACCESS_KEY_ID')
secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
session_token = os.getenv('AWS_SESSION_TOKEN')
region = os.getenv('AWS_REGION')

# Create clients for AWS services
dst_bucket='storage-class-classified-tokyo'
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
s3_dst_bucket = s3.Bucket(dst_bucket)
rek_client = boto3.client('rekognition')
dynamodb_client = boto3.resource('dynamodb')
table = dynamodb_client.Table('storage-class-dynamodb-tokyo-tag-uuid')


def lambda_handler(event, context):
    """Lambda Function entrypoint handler

    :event: S3 Put event
    :context: Lambda context
    :returns: Number of records processed

    """
    s3 = boto3.client('s3')
    processed = 0

    for record in event['Records']:
        s3_record = record['s3']

        key = s3_record['object']['key']
        bucket = s3_record['bucket']['name']
        logger.info('key&bucket')
        logger.info(key)
        logger.info(bucket)

        resp = rek_client.detect_labels(
            Image={'S3Object': {'Bucket': bucket, 'Name': key}},
            MaxLabels=10,
            MinConfidence=80)
        logger.info('resp: {}'.format(resp))
        labels = []
        copy_source = {
                'Bucket': bucket,
                'Key': key
                }
        for l in resp['Labels']:
            labels.append(l['Name'])
            print('=====' + str(l) + str(key))
            dst_key='/'+l['Name']+'/'+key
            ret_copy = s3_dst_bucket.copy(copy_source, dst_key)
            table.put_item(
                Item={
                    'uuid': l['Name']+'/'+bucket+'/'+key,
                    'tag' : l['Name'],
                    'Bucket': bucket,
                    'Name': key
                    }
                )
            
        logger.debug('Detected labels: {}'.format(labels))
        # res = es.index(index=es_index, doc_type='event',
        #                id=key, body={'labels': labels})

        # logger.debug(res)


        processed = processed + 1


    logger.info('Successfully processed {} records'.format(processed))
    return processed
