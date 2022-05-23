import json
import boto3
from boto3.dynamodb.conditions import Key

table = boto3.resource('dynamodb').Table('user-table')


def get_user_db_data(userName): 
    result=table.query(KeyConditionExpression=Key('username').eq(userName))
    return result

    
    
def lambda_handler(event, context):
    print("Profile Lambda!")
    print("event:",event)
    print(event['key'])

    userName=event['key']
    userData = get_user_db_data(userName)
    
    if userData["Count"]:
        print(userData["Items"][0]['data'])
        return {
            'statusCode': 200,
            'body': userData["Items"][0]['data']
        }
    
    else:
        return {
            'statusCode': 404,
            'body': "User Not Found"
        }
        
