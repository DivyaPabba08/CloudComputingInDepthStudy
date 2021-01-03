import json
import random
import time
import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr


def lambda_handler(event, context):
    
    input = event['body'].replace("\'","\"")
    body = json.loads(input)
    profilename = body['profilename']
    year = body['year']
    month = body['month']
    if_monthly = body['if_monthly']
    if_yearly=body['if_yearly'] #all the available transactions
    print('body',body)
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('TransactionalData')
    #generate monthly billing statements:
    if if_monthly=='1' or if_yearly=='1':
        
        if if_monthly=='1':
            response = table.query(
                KeyConditionExpression = Key('profilename').eq(profilename) & Key('timeoftransaction').between(year+'-'+month+'-1', year+'-'+month+'-'+'31'),
                FilterExpression=Attr('status').eq('approved')
            )
            print('response in if_monthly',response)
            
        if if_yearly=='1':
            response = table.query(
                KeyConditionExpression = Key('profilename').eq(profilename),
                FilterExpression=Attr('status').eq('approved')
            )
            print('response in if_yearly',response)
    billed_amount=0
    for item in response['Items']:
        item['amount'] = float(item['amount'])
        billed_amount=billed_amount+item['amount']
    print("response",response['Items']) 
    if if_yearly=='1':
        print("Total billed_amount for the given year:",billed_amount)
    else:
        print("Total billed_amount for the given month:",billed_amount)
    
    
    return {
        'headers': {
            "Access-Control-Allow-Headers" : "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
        },
        'statusCode': 200,
        'body': json.dumps(response)
    }