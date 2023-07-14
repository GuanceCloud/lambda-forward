import time
import json
import urllib3
import base64
import gzip


DATAKIT = ''

print('Start')

def push_dk(data):
    http = urllib3.PoolManager()
    data = json.dumps(data)
    response = http.request("POST", f'{DATAKIT}:9529/v1/write/logging', body=data, headers={'Content-Type': 'application/json'})
    print('dk_code:', response.status)

def to_datakit_data(event):
    data = {
        'measurement': 'lambda_test',
        'time'  : round(time.time()),
        'tags'  : {
            'id':event.get('id'),
            'timestamp':str(event.get('timestamp'))
        },
        'fields' : {'message':json.dumps(event)},
    }
    try:
        event_message = json.loads(event.get('message'))
    except:
        return data
        
    for k, v in event_message.items():
        if isinstance(v, str):
            data['tags'][k] = v
        else:
            data['fields'][k] = v
    return data

def event_encode(event):
    event = event['awslogs']['data']
    event = base64.b64decode(event.encode('utf-8'))
    event = gzip.decompress(event).decode('utf-8')
    event = json.loads(event)
    return event

def lambda_handler(event, context):
    try:
        event_list = event_encode(event).get('logEvents')
    except:
        event_list = [event]
        print('eventbridge event')

    dk_data_list = []
    for event in event_list:
        data =  to_datakit_data(event)
        dk_data_list.append(data)

    push_dk(dk_data_list)
