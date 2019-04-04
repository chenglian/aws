#
# I tested this code with Python 2.7
# This Lambda code snippet demos how to load data from a CSV file in S3 bucket to DynamoDB triggered by S3 file upload/creation event.
# It uses OKTA event logs as an example.
# You will have to increase Lambda timeout limit if you are loading large volume of data.

import boto3
import json

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    
    # log the event details
    print json.dumps(event)
    
    # get the name of the bucket that triggered this event
    bucket = event['Records'][0]['s3']['bucket']['name']
    
    # get the name of the file that triggered this event
    key = event['Records'][0]['s3']['object']['key']
    
    obj = s3.get_object(Bucket=bucket, Key=key)
    
    rows = obj['Body'].read().split('\n')
    
    rows.pop(0) ## remove header !!!ASSUMING the first row is HEADER!!!

    print "total # of rows (without header): ", len(rows)
    
    table = dynamodb.Table('oktaevents')
    
    counter = 0
    
    #
    # For better performace,
    # use batch_writer which automatically handles buffering and reduces # of inserts 
    # 
    # The batch writer can help to de-duplicate request by specifying 
    # overwrite_by_pkeys=['partition_key', 'sort_key'] if you want to bypass no duplication limitation of single batch write request 
    # as botocore.exceptions.ClientError: An error occurred (ValidationException) when calling the BatchWriteItem operation: Provided list of item keys contains duplicates.
    with table.batch_writer(overwrite_by_pkeys=['actor.id', 'timestamp']) as batch:
        
        for row in rows:
            
            values = row.split(',')
            if len(values) < 56:
                print "row #", counter, " maybe invalid hence skipped: ", values, "\n"
                continue
            
            # build DynamoDB item as a key/value dictionary.
            # here I used hardcoded header for better control.
            # you may read the header from the CVS file if you wish.
            Item= dict(zip(header, values)) 
            
            # remove key/value pairs with empty value - DynamoDB only accepts non-empty AttributeValue
            Item = {key: value for key, value in Item.items() if value}
            
            batch.put_item(Item)
            
            counter = counter + 1
    
        print "total # of items added (assuming no duplicates): ", counter, "\n"
        
#
# Hardcode header/columns here
# 
header = [
    "severity",
    "event_type",    
    "display_message",
    "uuid",    
    "version",
    "timestamp",    
    "outcome.result",
    "outcome.reason",    
    "actor.id",
    "actor.type",    
    "actor.display_name",
    "actor.alternate_id",    
    "authentication_context.authentication_step",
    "authentication_context.authentication_provider",    
    "authentication_context.credential_provider",
    "authentication_context.credential_type",    
    "authentication_context.issuer",
    "authentication_context.external_session_id",    
    "client.zone",
    "client.ip_address",    
    "client.device",
    "client.user_agent.raw_user_agent",
    "client.user_agent.os",
    "client.user_agent.browser",
    "client.geographical_context.country",
    "client.geographical_context.city",
    "client.geographical_context.postal_code",
    "client.geographical_context.geolocation.lon",
    "client.geographical_context.geolocation.lat",
    "transaction.id",    
    "transaction.type",    
    "debug_context.debug_data.request_uri",
    "legacy_event_type",
    "target0.id",    
    "target0.type",    
    "target0.alternate_id",    
    "target0.display_name",
    "target1.id",
    "target1.type",
    "target1.alternate_id",
    "target1.display_name",
    "target2.id",
    "target2.type",
    "target2.alternate_id",
    "target2.display_name",
    "target3.id",
    "target3.type",
    "target3.alternate_id",
    "target3.display_name",
    "request.ip_chain.geographical_context.postal_code",
    "request.ip_chain.geographical_context.geolocation.lon",
    "request.ip_chain.geographical_context.geolocation.lat",
    "request.ip_chain.geographical_context.geolocation.state",
    "request.ip_chain.ip",
    "request.ip_chain.source",
    "request.ip_chain.version"
]
