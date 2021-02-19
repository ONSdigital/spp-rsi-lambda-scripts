import base64
import boto3
import json
import os
from botocore.client import Config
from boto3.dynamodb.conditions import Key
from es_aws_functions import general_functions

current_module = "spp-res_lam_glue_runner"

# Load environment variables
environment = os.environ.get("environment")
sfn_activity_arn = os.environ.get("sfn_activity_arn")
sfn_worker_name = os.environ.get("sfn_worker_name")
ddb_table_name = os.environ.get("ddb_table")
ddb_query_limit = int(os.environ.get("ddb_query_limit"))
spark_glue_job_capacity = int(os.environ.get("spark_glue_job_capacity"))
ingest_glue_name = os.environ.get("ingest_glue_name")
emr_glue_name = os.environ.get("emr_glue_name")

# Set up logger with just environment at first
logger = general_functions.get_logger(None, current_module, environment, None)


glue = boto3.client("glue")
# Because Step Functions client uses long polling, read timeout has to be > 60 seconds
sfn_client_config = Config(connect_timeout=50, read_timeout=70)
sfn = boto3.client("stepfunctions", config=sfn_client_config)
dynamodb = boto3.resource("dynamodb")


def start_glue_jobs(job_name, config):
    ddb_table = dynamodb.Table(ddb_table_name)
    glue_job_capacity = spark_glue_job_capacity

    try:
        response = glue.start_job_run(
            JobName=job_name,
            Arguments=config,
            MaxCapacity=glue_job_capacity,
        )

        glue_job_run_id = response["JobRunId"]
    except Exception as e:
        logger.error(f"Error starting glue job {job_name}. Error: {e}")

    try:

        # Store SFN 'Task Token' and Glue Job 'Run Id' in DynamoDB
        item = {
            "sfn_activity_arn": sfn_activity_arn,
            "glue_job_name": job_name,
            "glue_job_run_id": glue_job_run_id
        }

        ddb_table.put_item(Item=item)
    except Exception as e:
        logger.error(f"Error saving glue job in dynamo table {ddb_table_name}. Error: {e}")


def check_glue_job(glue_info):
    args_pass = {}
    ddb_table = dynamodb.Table(ddb_table_name)
    glue_job_run_id = glue_info[""]
    try:
        ddb_resp = ddb_table.query(
            KeyConditionExpression=Key("glue_job_run_id").eq(glue_job_run_id)
        )
    except dynamodb.exceptions.ResourceNotFoundException as e:
        logger.error(f"Job {glue_job_run_id} not present in {ddb_table_name}. Error message: {e}")

    if glue_info['detail']['state'] == "SUCCEEDED":
        logger.info(f"Job with Run Id {glue_job_run_id} SUCCEEDED.")
        glue = boto3.client("glue")
        response = glue.get_job_run(JobName=glue_info["detail"]["jobName"], RunId=glue_info["detail"]["jobRunId"])
        args_pass = response["JobRun"]["Arguments"]
    elif glue_info['detail']['state'] in ["FAILED", "STOPPED", "TIMEOUT"]:
        message = f"Glue job {glue_info['detail']['jobName']} run with Run Id {glue_job_run_id[:8]} failed. " \
                  f"Last state: {glue_info['detail']['state']}. Error message: {glue_info['detail']['message']}"

        logger.error(message)
    else:
        logger.error(f"Glue job {glue_info['detail']['jobName']} response does not contain state.")

    # After logging and messages then delete item from dynamodb table
    try:
        job_key = {
            "sfn_activity_arn": sfn_activity_arn,
            "glue_job_run_id": glue_job_run_id
        }
        ddb_table.delete_item(Key=job_key)
    except Exception as e:
        logger.error(f"Failed to delete glue job, error: {e}")
    return args_pass


def handler(event, context):
    if "source" in event and event["source"] == "aws.glue":
        if event["jobName"] == ingest_glue_name:
            config_to_pass = check_glue_job(event)
            if config_to_pass:
                start_glue_jobs(emr_glue_name, config_to_pass)
        elif event["jobName"] == emr_glue_name:
            config_to_pass = check_glue_job(event)
    else:
        # Load config from api handler
        json_data = event["Records"][0]["body"].replace("\n", " ")
        event = json.loads(json_data)
        start_glue_jobs(ingest_glue_name, event)
