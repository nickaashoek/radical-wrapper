import argparse
import boto3
from botocore.exceptions import ClientError

RADICAL_TABLE = "radical_testing"
WRITE_TABLE = "write-intents"

def setup_intent_table(client):
    try:
        client.create_table(
            TableName=WRITE_TABLE,
            KeySchema=[
                { "AttributeName": "execution_id", "KeyType": "HASH"}
            ],
            AttributeDefinitions=[
                { "AttributeName": "execution_id", "AttributeType": "S"}
            ],
            ProvisionedThroughput={
                "ReadCapacityUnits": 10,
                "WriteCapacityUnits": 10
            }
        )
    except ClientError as e:
        print("Error creating intent table: {}".format(e))

def setup_item_table(client):
    try:
        client.create_table(
            TableName=RADICAL_TABLE,
            KeySchema=[
                { "AttributeName": "id", "KeyType": "HASH" },
            ],
            AttributeDefinitions=[
                { "AttributeName": "id", "AttributeType": "B" },
            ],
            ProvisionedThroughput={
                "ReadCapacityUnits": 10,
                "WriteCapacityUnits": 10
            }
        )
    except ClientError as e:
        print("Error creating item table: {}".format(e))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--user-region", dest="user_region", type=str, help="Near user region to setup")
    parser.add_argument("--data-region", dest="data_region", type=str, help="Near data region to setup")
    args = parser.parse_args()
    
    user_region, data_region = args.user_region, args.data_region
    print(f"Setting up AWS region pair: {user_region} (user) and {data_region} (data)")
    
    user_client = boto3.client('dynamodb', region_name=user_region)
    data_client = boto3.client('dynamodb', region_name=data_region)
    
    user_pages = user_client.get_paginator('list_tables')
    data_pages = data_client.get_paginator('list_tables')
    user_tables = set()
    data_tables = set()

    for page in user_pages.paginate():
        for table in page.get('TableNames', []):
            user_tables.add(table)
    
    for page in data_pages.paginate():
        for table in page.get('TableNames', []):
            data_tables.add(table)
    
    print("User region {} tables: {}".format(user_region, user_tables))
    print("Data region {} tables: {}".format(data_region, data_tables))
    if WRITE_TABLE not in data_tables:
        setup_intent_table(data_client)
    if RADICAL_TABLE not in data_tables:
        setup_item_table(data_client)
    if RADICAL_TABLE not in user_tables:
        setup_item_table(user_client)
    