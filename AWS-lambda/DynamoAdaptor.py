import boto3
import logging
import os


class DynamoAdaptor:
    def __init__(self, logger,
                 dynamodb_name = 'storage-class-dynamodb-tokyo-tag-uuid',
                 dynamodb_to_tag_name = 'storage-class-dynamodb-path-to-tag-tokyo',
                 dst_bucket_name = 'storage-class-classified-tokyo'):
        # es_host = os.getenv('ELASTICSEARCH_URL')
        # es_index = 'images'
        access_key = os.getenv('AWS_ACCESS_KEY_ID')
        secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        session_token = os.getenv('AWS_SESSION_TOKEN')
        region = os.getenv('AWS_REGION')

        # Create clients for AWS services
        self.s3 = boto3.resource('s3')
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        self.s3_dst_bucket = self.s3.Bucket(dst_bucket_name)
        self.rek_client = boto3.client('rekognition')
        self.dynamodb_client = boto3.resource('dynamodb')
        self.table = self.dynamodb_client.Table(dynamodb_name)
        self.idx_table = self.dynamodb_client.Table(dynamodb_to_tag_name)
        self.response = {
            'ObjectCreated:Put': self.create, # NOTE not sure
            'ObjectRemoved:DeleteMarkerCreated': self.remove # NOTE not sure
        }

        self.logger = logger
        self.logger.debug('===Succussfully load Dynamo Adaptor')
    

    def create(self, record):
        ''' record example
        {
         'eventVersion': '2.1',
         'eventSource': 'aws:s3',
         'awsRegion': 'ap-northeast-1',
         'eventTime': '2019-06-03T14:16:00.466Z',
         'eventName': 'ObjectCreated:Put',
         's3': {'bucket': {'name': 'storage-class-tokyo',
                           'arn': 'arn:aws:s3:::storage-class-tokyo'},
                'object': {'key': 'star.png',
                           'size': 1485,
                           'eTag': 'c91191fef7d2bc1f6e291a3dc6592311',
                           'versionId': '3Lr_Z.8QGGcTEGwV_XnAqmJm4nbb8FBh',
                           'sequencer': '005CF52BA05DB0A1FB'}
                }
        }
        '''
        s3_record = record['s3']
        key = s3_record['object']['key']
        bucket = s3_record['bucket']['name']
        self.logger.debug('===Create key:{}&bucket:{}'.format(key, bucket))

        resp = self.rek_client.detect_labels(
            Image={'S3Object': {'Bucket': bucket, 'Name': key}},
            MaxLabels=10,
            MinConfidence=80)
        self.logger.debug('===Detect result: {}'.format(resp))

        labels = []
        copy_source = {
                'Bucket': bucket,
                'Key': key
                }
        
        for l in resp['Labels']:
            labels.append(l['Name'])
            print('====={}'.format(l['Name']+'-'+bucket+'-'+key))
            dst_key='/'+l['Name']+'/'+key
            ret_copy = self.s3_dst_bucket.copy(copy_source, dst_key)
            self.table.put_item(
                Item={
                    'uuid': l['Name']+'/'+bucket+'/'+key,
                    'tag' : l['Name'],
                    'Bucket': bucket,
                    'Name': key
                    }
                )
        
        self.idx_table.put_item(
            Item={
                'path': bucket+'/'+key,
                'tag': ','.join(labels)
            }
        )
        self.logger.debug('===Finish{}/{}: {}'.format(bucket, key, labels))


    def remove(self, record):
        ''' record example
        {
        'eventVersion': '2.1',
        'eventSource': 'aws:s3', 
        'awsRegion': 'ap-northeast-1', 
        'eventTime': '2019-06-03T14:32:41.035Z', 
        'eventName': 'ObjectRemoved:DeleteMarkerCreated', 
        's3': {'s3SchemaVersion': '1.0', 
                'configurationId': '2b4ad7b4-0e7b-4e5e-a68d-e489576a8496', 
                'bucket': {'name': 'storage-class-tokyo', 
                        'arn': 'arn:aws:s3:::storage-class-tokyo'},
                'object': {'key': 'star6.png'}}
        }
        '''
        s3_record = record['s3']
        key = s3_record['object']['key']
        bucket = s3_record['bucket']['name']
        self.logger.debug('===Remove key:{}&bucket:{}'.format(key, bucket))

        resp = self.idx_table.get_item(
            Key= {'path': bucket+'/'+key}
        )
        self.logger.debug('===idx return: {}'.format(resp))
        if ('Item' not in resp):
            self.logger.debug('===Skip {}/{}'.format(bucket, key))
            return
        tags = resp['Item']['tag'].split(',')
        self.logger.debug('===after split: {}'.format(tags))

        
        for tag in tags:
            # TODO remove file in s3_dst_bucket
            try: # the item may not exist
                 self.table.delete_item(
                    Key = {
                        'uuid': tag+'/'+bucket+'/'+key,
                        'tag': tag

                    }
                )
            except:
                self.logger.debug('===Fail to remove {}'.format(tag+'/'+bucket+'/'+key))

        self.idx_table.delete_item(
            Key= {'path': bucket+'/'+key}
        )
           
        self.logger.debug('===Finish{}/{}: {}'.format(bucket, key, tags))


    def handle(self, record):
        eventName = record['eventName']
        if eventName not in self.response:
            self.logger.debug('=== ERROR! Invalid event {}'.format(eventName))
            exit(0) # TODO: how to send error message?
        else:
            self.response[eventName](record)
