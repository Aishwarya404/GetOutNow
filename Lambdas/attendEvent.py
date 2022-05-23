import json
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

user_table = boto3.resource('dynamodb').Table('user-table')
events_table = boto3.resource('dynamodb').Table('events')


def get_event_db_data(eventId):
    result = events_table.query(KeyConditionExpression=Key('id').eq(eventId))
    return result


def get_user_db_data(userName): 
    result = user_table.query(KeyConditionExpression=Key('username').eq(userName))
    return result


def add_to_dynamo(username, id):
    # ADD EVENT DATA TO USER TABLE
    
    result = user_table.query(KeyConditionExpression=Key('username').eq(username))
    print(result)
    email = result["Items"][0]['data']['email']
    eventResult = get_event_db_data(id)
    event_name = eventResult["Items"][0]['data']['name']
    eventData = {
        'eventid': eventResult["Items"][0]['id'],
        'name': eventResult["Items"][0]['data']['name'],
        'photo': eventResult["Items"][0]['data']['image']['url']
    }
    result["Items"][0]['data']['events'].append(eventData)
    result = user_table.update_item(
        Key = {
            'username': username
        },
        AttributeUpdates = {
            'data':{
                'Value': result["Items"][0]['data']
            }
        },
        ReturnValues="UPDATED_NEW"
    )
    print(result)
    
    
    # ADD USER DATA TO EVENTS TABLE
    result = events_table.query(KeyConditionExpression=Key('id').eq(id))
    print(result)
    
    userResult = get_user_db_data(username)
    userData = {
        'username': username,
        'name': userResult["Items"][0]['data']['name'],
        'photo': userResult["Items"][0]['data']['photo']
    }
    result["Items"][0]['data']['attendees'].append(userData)
    result = events_table.update_item(
        Key = {
            'id': id
        },
        AttributeUpdates = {
            'data':{
                'Value': result["Items"][0]['data']
            }
        },
        ReturnValues = "UPDATED_NEW"
    )
    print(result)
    return email, event_name
    
    
def send_email(email, event_name):
    client = boto3.client('ses',region_name='us-east-1')
    body = "We are glad you are choosing to attend the event " + event_name + ". We look forward to seeing you there!"
    try:
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    email,
                ],
            },
            Message={
                'Body': {

                    'Text': {
                        'Charset': 'UTF-8',
                        'Data': body,
                    },
                },
                'Subject': {
                    'Charset': 'UTF-8',
                    'Data': 'See you there!',
                },
            },
            Source="getoutnowwebsite@gmail.com",
        )
        
    except ClientError as e:
        print(e.response['Error']['Message'])
    
def lambda_handler(event, context):
    id = event["id"]
    username = event["name"]
    email, event_name = add_to_dynamo(username, id)
    send_email(email, event_name)
    return {
        'statusCode': 200,
        'body': "Successfully updated dynamo"
    }
