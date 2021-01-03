import json
import boto3
import time
import random

def lambda_handler(event, context):
    
    table_exists=False
    #create profile table
    if not table_exists:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1') 
        dynamodb.create_table(
                            TableName='UserProfiles',
                            KeySchema=[
                                {
                                    'AttributeName': 'profilename',
                                    'KeyType': 'HASH' 
                                },
                            ],
                            AttributeDefinitions=[
                                {
                                    'AttributeName': 'profilename',
                                    'AttributeType': 'S'
                                },
                            ],
                            ProvisionedThroughput={
                                'ReadCapacityUnits': 10,
                                'WriteCapacityUnits': 10
                            }
                        )
                        
        dynamodb.create_table(
                            TableName='TransactionalData',
                            KeySchema=[
                                {
                                    'AttributeName': 'profilename',
                                    'KeyType': 'HASH' 
                                },
                                {
                                    'AttributeName': 'timeoftransaction',
                                    'KeyType': 'RANGE'
                                }
                            ],
                            AttributeDefinitions=[
                                {
                                    'AttributeName': 'profilename',
                                    'AttributeType': 'S'
                                },
                                {
                                    'AttributeName': 'timeoftransaction',
                                    'AttributeType': 'S'
                                }
                            ],
                            ProvisionedThroughput={
                                'ReadCapacityUnits': 10,
                                'WriteCapacityUnits': 10
                            }
                        )
        time.sleep(10)

    #create user data
    persons=[]
    simulate_user_count=100    
    valid_places_for_transaction=['New York City','Ohio','Michigan','Los Angeles','Seattle','Bangalore','Rome','San Jose','Austin','Columbus']
    valid_amounts_for_transaction=[200,300,400,500,600,700,800,900,1000,1100,1200,1300]
    
    for i in range(simulate_user_count):
        person = {}
        places = ''
        valid_places = random.sample(valid_places_for_transaction, k=4)
        for place in valid_places:
            places += (place + ',')
        person['valid_places_for_transaction'] = places[:-1]
        persons.append(person)
        person['profilename'] = 'person' + str(i + 1)
        person['valid_amounts_for_transaction'] = random.choice(valid_amounts_for_transaction)
   
    transactions = []
    times_list = ['2020-12-2','2020-12-3','2020-12-4','2020-12-5','2020-12-6','2020-12-7','2020-12-8','2020-12-9','2020-12-10','2020-12-11','2020-12-12','2020-12-13','2020-12-14']
    amount_list = [200,300,400,500,600,700,800,900,1000,1100,1200,1300]
    status_list = ['approved','rejected']
    item_list=['watch','water_bottle','kettle','refrigerator','thermals','wollen bag','earphones','books','sweater','charger','cellphone']
    places_list=['New York City','Ohio','Michigan','Los Angeles','Seattle','Bangalore','Rome','San Jose','Austin','Columbus']
    for i in list(range(0,100)):
        user_id=random.randint(0,101)
        transaction = {}
        transaction['profilename'] = 'person'+str(user_id)
        transaction['timeoftransaction'] = random.choice(times_list)
        transaction['amount'] = random.choice(amount_list)
        transaction['status'] = random.choice(status_list)
        transaction['purchase_item'] =random.choice(item_list)
        transaction['place']=random.choice(places_list)
        transactions.append(transaction)
    return transactions
    
        
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1') 
    #populate user data
    table = dynamodb.Table('UserProfiles')
    for person in persons:
        response = table.put_item(Item = person)
        
    table = dynamodb.Table('TransactionalData')
    for transaction in transactions:
        response = table.put_item(Item = transaction)
        
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
