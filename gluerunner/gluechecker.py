import boto3
import json
import os
from botocore.client import Config
from boto3.dynamodb.conditions import Key
from es_aws_functions import general_functions

current_module = "spp-res_lam_glue_checker"

# Load environment variables
environment = os.environ.get("environment")
sfn_activity_arn = os.environ.get("sfn_activity_arn")
ddb_table_name = os.environ.get("ddb_table")
ddb_query_limit = int(os.environ.get("ddb_query_limit"))

# Set up logger with just environment at first
logger = general_functions.get_logger(None, current_module, environment, None)


def handler(event, context):
    sfn_client_config = Config(connect_timeout=50, read_timeout=70)
    sfn = boto3.client("stepfunctions", config=sfn_client_config)
    dynamodb = boto3.resource("dynamodb")
    ddb_table = dynamodb.Table(ddb_table_name)
    glue_job_run_id = event['detail']['jobRunId']

    try:
        ddb_resp = ddb_table.query(
            KeyConditionExpression=Key("glue_job_run_id").eq(glue_job_run_id)
        )
    except dynamodb.exceptions.ResourceNotFoundException as e:
        logger.error(f"Job {glue_job_run_id} not present in {ddb_table_name}. Error message: {e}")
        return

    for item in ddb_resp["Items"]:
        glue_job_run_id = item["glue_job_run_id"]
        glue_job_name = item["glue_job_name"]
        sfn_task_token = item["sfn_task_token"]

        if event['detail']['state'] == "SUCCEEDED":
            logger.info(f"Job with Run Id {glue_job_run_id} SUCCEEDED.")
            task_output_dict = {
                "GlueJobName": glue_job_name,
                "GlueJobRunId": glue_job_run_id,
                "GlueJobRunState": event['detail']['state'],
                "GlueJobTime": event['time'],
            }
            task_output_json = json.dumps(task_output_dict)
            sfn.send_task_success(
                taskToken=sfn_task_token, output=task_output_json
            )
        elif event['detail']['state'] in ["FAILED", "STOPPED", "TIMEOUT"]:
            message = f"Glue job {glue_job_name} run with Run Id {glue_job_run_id[:8]} failed. " \
                      f"Last state: {event['detail']['state']}. Error message: {event['detail']['message']}"

            logger.error(message)

            message_json = {
                "glue_job_name": glue_job_name,
                "glue_job_run_id": glue_job_run_id,
                "glue_job_run_state": event['detail']['state'],
                "glue_job_run_error_msg": event['detail']['message']
            }

            sfn.send_task_failure(
                taskToken=sfn_task_token,
                cause=json.dumps(message_json),
                error="GlueJobFailedError",
            )

        # After logging and messages then delete item from dynamodb table
        try:
            job_key = {
                "sfn_activity_arn": sfn_activity_arn,
                "glue_job_run_id": glue_job_run_id
            }
            ddb_table.delete_item(Key=job_key)
        except Exception as e:
            logger.error(f"Failed to delete glue job, error: {e}")
