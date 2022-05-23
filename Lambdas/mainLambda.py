import json, boto3, requests, re, random
from boto3.dynamodb.conditions import Key, Attr
from requests.auth import HTTPBasicAuth
from collections import defaultdict
from datetime import datetime


def get_query(matches, size):
    return {
        'size': size, 
        'query': {
            'bool': {
                'should': matches, 
                'minimum_should_match': 1
            }
        }
    }    


def query_events(query):
    OS_HOST = "https://search-events-olistiprnbjai5j6547ry3ixra.us-east-1.es.amazonaws.com/_search?pretty"
    OS_HTTP_USER = 'master'
    OS_HTTP_PWD = "Abcd_1234"
    headers = {'Content-Type': 'application/json'}
    
    response = requests.request('GET', OS_HOST, data=json.dumps(query), auth=HTTPBasicAuth(OS_HTTP_USER, OS_HTTP_PWD), headers=headers)
    res = json.loads(response.text)
    print(res)
    
    return res['hits']['hits'] if 'hits' in res and 'hits' in res['hits'] else []


def get_os_data(search_msg):
    if " + " in search_msg:
        events = list()
        msgs = search_msg.split(" + ")
        for msg in msgs:
            matches = [
                {
                    'match': {
                        'categories': msg
                    }
                }, 
                {
                    'match': {
                        'location': msg.replace(",", "")
                    }
                }, 
            ]
            
            print("msg: {}".format(msg))
            events.extend(query_events(get_query(matches, 45)))
            print("events: {}".format(events))
        
        events_list = [{'id': e['_source']['eventID'], 'name': e['_source']['name'], 'image': e['_source']['image']} for e in events]    
        events = {'all': events_list}
    else:
        matches = [
            {
                'match': {
                    'categories': search_msg
                }
            }, 
            {
                'match': {
                    'location': search_msg.replace(",", "")
                }
            }, 
        ]
        
        # search message for date needs to be in datetime form
        # therefore if message is in date, include date in list of matches, otherwise don't 
        try: 
            match = re.match("(\d{2})[/-](\d{2})[/-](\d{4})", search_msg)
            if match:
                search_msg = match.group(3) + "-" + match.group(1) + "-" + match.group(2)
                print("search_msg after date change: ", search_msg)
            
            try:
                date_msg = datetime.strptime(search_msg, '%Y-%M-%d')
            except:
                date_msg = datetime.strptime(search_msg, '%Y/%M/%d')
            
            date_msg = search_msg
            print("date_msg: ")
            print(type(date_msg))
            print(date_msg)
            matches = [
                {
                    'match': {
                        'dates': date_msg
                    } 
                }   
            ]
        except:
            matches.append(
                {
                    'match': {
                        'aliases': search_msg
                    }
                }    
            )
    
        events = query_events(get_query(matches, 9))
        events = categorize_matched_events(events, search_msg)
    print("RETURNING EVENTS")
    print(events)
    return events
    
    
def categorize_matched_events(events, search_msg):
    print("events: ");
    print(events);
    matched_events = defaultdict(lambda: list())
    for e in events:
        for k, v in e['_source'].items():
            loc_split = [sm for sm in re.split(', ', search_msg) if sm in v]
            if (k == "location" and len(loc_split) > 0 and all(loc_split)) or search_msg in v: # matched to this field
                item = {
                    'id': e['_source']['eventID'], 
                    'name': e['_source']['name'], 
                    'image': e['_source']['image']
                }
                if k == "aliases": 
                    k = "name"
                matched_events[k].append(item)
                break
    return matched_events
    
    
def shuffle_events(events):
    for elist in events.values():
        random.shuffle(elist)
    

def get_user_dynamo(name):
    print("attempting to get username")
    table = boto3.resource('dynamodb').Table('user-table')
    response = table.scan()
    data = response['Items']
    print(data)
    users = [d for d in data if name in d['data']['name'].lower() or name in d['username']]
    return users
    
    
def lambda_handler(event, context):
    # TODO implement
    # Getting search keyword from event['key']
    key = event['key'].lower()
    print("Key:",key)
    events = get_os_data(key)
    shuffle_events(events)
    users = list()
    if " + " not in key:
        users = get_user_dynamo(key)
        print("users",users)
    
    return {
        'statusCode': 200,
        'body': {'events': events, 'users': users}
    }
