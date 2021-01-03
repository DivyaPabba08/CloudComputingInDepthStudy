from json import dumps
from decimal import Decimal
import json
import boto3
import base64



def lambda_handler(event, context):
    records = event['records']['Transaction_DetailsTopic-0']
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1') 
    table = dynamodb.Table('TransactionalData')
    published_events=[]
    for record in records:
        value = record['value']
        if value!='':
            published_event = json.loads(base64.b64decode(value).decode('UTF-8'))
            published_events.append(published_event)
            
    for published_event in published_events:
            published_event['amount'] = Decimal(str(published_event['amount']))
            print("item",published_event)
            table.put_item(Item = published_event)
    
    
    return {
        'headers': {
            "Access-Control-Allow-Headers" : "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
        },
        'statusCode': 200,
        'body': json.dumps('Hello From Lambda')
    }
