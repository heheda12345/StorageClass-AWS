import boto3
import os
import logging
dynamodb_client = boto3.resource('dynamodb')
table = dynamodb_client.Table('storage-class-dynamodb-tokyo')
print(table)
ret=table.put_item(
        Item={
            'uuid': 'asefawef',
            'tag' : 'apple',
            'path': 'a/b',
            }
        )

# dynamodb_client = boto3.client('dynamodb')
# ret=dynamodb_client.put_item(
#         TableName='storage-class-dynamodb-tokyo',
#         Item={
#             'uuid': {
#                 'S': 'asefawef'
#                 },
#             'tag': {
#                 'S': 'apple'
#                 },
#             'path': {
#                 'S': 'a/b'
#                 }
#             },
#         # ReturnValues='NONE'|'ALL_OLD'|'UPDATED_OLD'|'ALL_NEW'|'UPDATED_NEW',
#         # ReturnConsumedCapacity='INDEXES'|'TOTAL'|'NONE',
#         # ReturnItemCollectionMetrics='SIZE'|'NONE'
#         )
print(ret)

ret = table.get_item(
        Key={
            'uuid': 'a',
            'tag' : 'b'
            }
        )
print(ret)

ret = table.get_item(
        Key={
            'uuid': 'asefawef',
            'tag' : 'apple'
            }
        )
print(ret)
