import json
import boto3
from collections import defaultdict
from boto3.dynamodb.conditions import Key


def get_db_data(id): 
    table = boto3.resource('dynamodb').Table('events')
    print("T", table)
    response = table.query(KeyConditionExpression=Key('id').eq(id))
    print("R", response)
    return response

    
def lambda_handler(event, context):
    # TODO implement
    # Getting search keyword from event['key']
    print(event["key"])
    response = get_db_data(event['key'])
    print(response)
    return {
        'statusCode': 200,
        'body': response["Items"][0]
    }
