import boto3
import json
import logging
import os

from marshmallow import EXCLUDE, Schema, fields

class EnvironmentSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating environment params: {e}")
        raise ValueError(f"Error validating environment params: {e}")

    table_name = fields.Str(required=True)

def lambda_handler(event, context):
    
    client = boto3.client('dynamodb')

    environment_variables = EnvironmentSchema().load(os.environ)

    # Environment Variables.
    table_name = environment_variables["table_name"]
    
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