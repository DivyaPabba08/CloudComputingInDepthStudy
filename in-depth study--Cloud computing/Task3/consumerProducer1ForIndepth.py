from kafka import KafkaProducer
import json
from json import dumps
from datetime import datetime
import boto3
import base64
import time
import random
from time import sleep

kafka_brokers="b-2.mskclusterforindepthst.hze228.c9.kafka.us-east-1.amazonaws.com:9094,b-1.mskclusterforindepthst.hze228.c9.kafka.us-east-1.amazonaws.com:9094"


def lookup_table(dynamodb, profilename):
	response = dynamodb.get_item(
		TableName="UserProfiles",
		Key={
			'profilename': {
				'S': profilename
			},
		}
	)
	return response

def lambda_handler(event, context):
    """
    print("KafkaConsumer... ")
    
    consumer = KafkaConsumer(KAFKA_TOPIC_NAME_CONS, security_protocol="SSL",auto_offset_reset='earliest',
                             bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS, api_version=(0, 10), consumer_timeout_ms=1000)
    for msg in consumer:
        print('hello2')
        #record = json.loads(msg.value)
        #print(record)
        result=list()
        result.append(msg.value)
        print(msg.value)
        sleep(10)

    if consumer is not None:
        consumer.close()
        
    """    
    records = event['records']['Transaction_AppReqTopic-0']
    dynamodb = boto3.client('dynamodb')
    published_events=[]
    for record in records:
        value = record['value']
        if value!='':
            published_event = json.loads(base64.b64decode(value).decode('UTF-8'))
            published_events.append(published_event)
    
    for published_event in published_events:
            print("published_event",published_event)
            profilename = published_event['profilename']
            userprofiledetails = lookup_table(dynamodb, profilename)['Item']
            valid_amounts_for_transaction = userprofiledetails['valid_amounts_for_transaction']['N']
            valid_places_for_transaction = userprofiledetails['valid_places_for_transaction']['S']
            valid_places_for_transaction.split(',')
        
            if published_event['place'] not in valid_places_for_transaction:
                published_event['status'] = 'rejected'
            elif int(published_event['amount']) > int(valid_amounts_for_transaction):
                published_event['status'] = 'rejected'
            else:
                published_event['status'] = 'approved'
                
    publisher = KafkaProducer(security_protocol="SSL", bootstrap_servers=kafka_brokers,
                             value_serializer=lambda x: dumps(x).encode('utf-8'))


    for published_event in published_events:
        print("published_event",published_event)
        publisher.send("Transaction_DetailsTopic", published_event)
        sleep(3)
        
    
        
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

