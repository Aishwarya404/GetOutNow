import json
import boto3
from boto3.dynamodb.conditions import Key


def save_user_information(username, name, bio, photo, email, city, categories):
    table = boto3.resource('dynamodb', region_name='us-east-1').Table('user-table')
    data = {
        'username': username,
        'data': {
            'name': name,
            'bio': bio,
            'email': email,
            'friends': [],
            'events': [],
            'photo': photo,
            'city': city,
            'categories':categories
        }
    }
    response = table.put_item(Item=data)

def verify_email(email):
    ses_client = boto3.client("ses")
    if email not in ses_client.list_verified_email_addresses()["VerifiedEmailAddresses"]:
                response = ses_client.verify_email_identity(
                EmailAddress=email
        )
                print(response)

def lambda_handler(event, context):
    print(event)
    username = event['username']
    name = event['name']
    bio = event['bio']
    photo = event['photo']
    email = event['email']
    city = event['city']
    categories = event['categories']
    save_user_information(username, name, bio, photo, email, city, categories)
    verify_email(email)
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*'
        },
        'body': json.dumps('Hello from Lambda!'),
        'event': json.dumps(event)
    }
