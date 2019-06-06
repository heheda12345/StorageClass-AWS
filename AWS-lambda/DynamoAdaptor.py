import boto3
import logging
import os


class DynamoAdaptor:
    def __init__(self, logger,
                 dynamodb_name = 'storage-class-dynamodb-tokyo-tag-uuid',
                 dynamodb_to_tag_name = 'storage-class-dynamodb-path-to-tag-tokyo',
                 s3_dst_bucket_name = 'storage-class-classified-tokyo',
                 audio_bucket_name = 'storage-class-audio-tokyo'):
        # es_host = os.getenv('ELASTICSEARCH_URL')
        # es_index = 'images'
        access_key = os.getenv('AWS_ACCESS_KEY_ID')
        secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        session_token = os.getenv('AWS_SESSION_TOKEN')
        region = os.getenv('AWS_REGION')

        # Create clients for AWS services
        self.s3 = boto3.resource('s3')
        
        self.audio_bucket_name = audio_bucket_name
        # self.s3_dst_bucket_name = s3_dst_bucket_name
        self.dynamodb_name = dynamodb_name
        self.dynamodb_to_tag_name = dynamodb_to_tag_name

        self.response = {
            'ObjectCreated:Put': self.create, # NOTE not sure
            'ObjectRemoved:DeleteMarkerCreated': self.remove # NOTE not sure
        }

        self.logger = logger
        self.logger.debug('===Succussfully load Dynamo Adaptor')


    def insertImageToDynamo(self, bucket, key):
        rek_client = boto3.client('rekognition')
        resp = rek_client.detect_labels(
            Image={'S3Object': {'Bucket': bucket, 'Name': key}},
            MaxLabels=10,
            MinConfidence=80)
        self.logger.debug('===Detect result: {}'.format(resp))

        labels = []
        copy_source = {
                'Bucket': bucket,
                'Key': key
                }
        
        # s3_dst_bucket = self.s3.Bucket(self.s3_dst_bucket_name)
        table = boto3.resource('dynamodb').Table(self.dynamodb_name)
        idx_table = boto3.resource('dynamodb').Table(self.dynamodb_to_tag_name)
        
        for l in resp['Labels']:
            labels.append(l['Name'])
            print('====={}'.format(l['Name']+'-'+bucket+'-'+key))
            # dst_key='/'+l['Name']+'/'+key
            # ret_copy = s3_dst_bucket.copy(copy_source, dst_key)
            table.put_item(
                Item={
                    'uuid': key,
                    'tag' : l['Name'],
                    'Bucket': bucket,
                    'Name': key
                    }
                )
        
        idx_table.put_item(
            Item={
                'path': bucket+'/'+key,
                'tag': ','.join(labels)
            }
        )
        self.logger.debug('===Finish{}/{}: {}'.format(bucket, key, labels))
    

    def generateAudio(self, bucket, key):
        source_bucket = self.s3.Bucket(bucket)
        source_obj = source_bucket.Object(key)
        if (source_obj is None):
            self.logger.debug('==={}/{} not found'.format(bucket, key))
            return
        text = source_obj.get()['Body'].read().decode()
        self.logger.debug('===Read: {}'.format(text))

        polly = boto3.client('polly')
        response = polly.synthesize_speech(Text=text, OutputFormat="mp3", VoiceId="Matthew")
        if ("AudioStream" not in response):
            self.logger.debug("===Fail to generate audio")
            return
        stream = response["AudioStream"]
        target_bucket = self.s3.Bucket(self.audio_bucket_name)
        target_bucket.put_object(Key=key, Body=stream.read())
        self.logger.debug('===Finish generate audio, save to {}'.format(key))


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
        if key.startswith('image/'):
            self.insertImageToDynamo(bucket, key)
        elif key.startswith('read/'):
            self.generateAudio(bucket, key)
            # TODO remove the audio
    

    def removeImageFromDynamo(self, bucket, key):
        table = boto3.resource('dynamodb').Table(self.dynamodb_name)
        idx_table = boto3.resource('dynamodb').Table(self.dynamodb_to_tag_name)
        resp = idx_table.get_item(
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
                 table.delete_item(
                    Key = {
                        'uuid': key,
                        'tag': tag

                    }
                )
            except:
                self.logger.debug('===Fail to remove {}'.format(tag+'/'+bucket+'/'+key))

        idx_table.delete_item(
            Key= {'path': bucket+'/'+key}
        )
           
        self.logger.debug('===Finish{}/{}: {}'.format(bucket, key, tags))


    def removeAudio(self, bucket, key):
        audio_bucket = self.s3.Bucket(self.audio_bucket_name)
        obj = audio_bucket.Object(key)
        if (obj is None):
            self.logger.debug('==={} not found in {}'.format(key, self.audio_bucket_name))
            return
        obj.delete()
        self.logger.debug('===Finish remove {}/{}', self.audio_bucket_name, key)


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
        if key.startswith('image/'):
            self.removeImageFromDynamo(bucket, key)
        elif key.startswith('read/'):
            self.removeAudio(bucket, key)
        

    def handle(self, record):
        eventName = record['eventName']
        if eventName not in self.response:
            self.logger.debug('=== ERROR! Invalid event {}'.format(eventName))
            exit(0) # TODO: how to send error message?
        else:
            self.response[eventName](record)
