import boto3
import json

def lambda_handler(event, context):
    
    client = boto3.client('dynamodb')
    
    if 'category' in event['params']['querystring']:
        print(event)
        data = client.scan(
        TableName='spp_res_ddb_templates',
        FilterExpression = 'category = :ctr',
        ExpressionAttributeValues = {":ctr":{'S': event['params']['querystring']['category']}},
        ProjectionExpression='id, category, created_by, created_time, description, version'
        )
    
    elif 'templateid' in event['params']['path']:
        data = client.query(
        TableName='spp_res_ddb_templates',
        KeyConditionExpression ='id = :tempid',
        ExpressionAttributeValues = {":tempid":{'N': event['params']['path']['templateid']}},
        )
 
        if not data['Items']:
            data['ResponseMetadata']['HTTPStatusCode'] = 400
            data["message"] = "Item not found"
     
    else:
        data = {"message": "invalid request",
                "ResponseMetadata": {"HTTPStatusCode": 400}
            }           

    response = {"body": data}
    return response