import json, boto3
from boto3.dynamodb.conditions import Key

table = boto3.resource('dynamodb').Table('user-table')

def get_user_db_data(username): 
    result = table.query(KeyConditionExpression=Key('username').eq(username))
    return result["Items"][0]
    
    
def get_friends_events(friends):
    events = dict()
    for f in friends:
        friend = get_user_db_data(f['username'])
        events_data = friend["data"]["events"]
        for e in events_data:
            if e["eventid"] not in events:
                events[e["eventid"]] = e
                events[e["eventid"]]["friendsAttending"] = [f["username"]]
            else:
                events[e["eventid"]]["friendsAttending"].append(f["username"])
    return events


def lambda_handler(event, context):
    username = event['username']
    print("Uname", username)
    userData = get_user_db_data(username)
    print("UD", userData)
    result = get_friends_events(userData['data']['friends'])

    print("E", result)
    
    return {
        'statusCode': 200,
        'body': result
    }
