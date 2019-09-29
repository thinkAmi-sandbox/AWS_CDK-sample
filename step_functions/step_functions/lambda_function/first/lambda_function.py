import os
import boto3
from numpy.random import rand


def lambda_handler(event, context):
    body = f'{event["message"]} \n value: {rand()}'
    client = boto3.client('s3')
    client.put_object(
        Bucket=os.environ['BUCKET_NAME'],
        Key='sfn_first.txt',
        Body=body,
    )

    return {
        'body': body,
        'message': event['message'],
    }
