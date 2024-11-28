import argparse
import boto3
from botocore.exceptions import ClientError

def setup_intent_table(client):
    try:
        client.create_table(
            TableName='write-intents',
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
            TableName='radical-testing',
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
    parser.add_argument("--region", dest="aws_region", type=str, help="AWS region to setup")
    args = parser.parse_args()
    
    region = args.aws_region
    print(f"Setting up AWS region: {region}")
    
    dynamodb = boto3.client('dynamodb', region_name=region)
    
    paginator = dynamodb.get_paginator('list_tables')
    tables = set()
    for page in paginator.paginate():
        for table in page.get('TableNames', []):
            tables.add(table)
    
    print("Region {} tables: {}".format(region, tables))
    if 'write-intents' not in tables:
        setup_intent_table(dynamodb)
    if 'radical-testing' not in tables:
        setup_item_table(dynamodb)
    