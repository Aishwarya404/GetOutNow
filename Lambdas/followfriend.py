import json
import boto3
from boto3.dynamodb.conditions import Key

table = boto3.resource('dynamodb').Table('user-table')


def get_user_db_data(userName): 
    result=table.query(KeyConditionExpression=Key('username').eq(userName))
    return result


def lambda_handler(event, context):
    print(event)
    username=event['username']
    friendName=event['friendname']
    
    result = get_user_db_data(username)
    friendResult = get_user_db_data(friendName)
    print(result, friendResult)
    
    friendData = {
        'username': friendName, 
        'name': friendResult["Items"][0]['data']['name'], 
        'photo': friendResult["Items"][0]['data']['photo']
    }
    
    result["Items"][0]['data']['friends'].append(friendData)

    result = table.update_item(
        Key={
            'username': username
        },
        AttributeUpdates={
            'data':{
                'Value': result["Items"][0]['data']
            }
        },
        ReturnValues="UPDATED_NEW"
    )
    
    print("Result:",result)
    return {
        'statusCode': 200,
        'body': result
    }
