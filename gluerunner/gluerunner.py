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

# Set up logger with just environment at first
logger = general_functions.get_logger(None, current_module, environment, None)
glue = boto3.client("glue")
# Because Step Functions client uses long polling, read timeout has to be > 60 seconds
sfn_client_config = Config(connect_timeout=50, read_timeout=70)
sfn = boto3.client("stepfunctions", config=sfn_client_config)
dynamodb = boto3.resource("dynamodb")


def handler(event, context):
    ddb_table = dynamodb.Table(ddb_table_name)
    glue_job_capacity = spark_glue_job_capacity

    # Loop until no tasks are available from Step Functions
    while True:

        try:
            response = sfn.get_activity_task(
                activityArn=sfn_activity_arn, workerName=sfn_worker_name
            )

        except Exception as e:
            logger.error(f"Failed to get_activity_task for {sfn_activity_arn}: {e} ")
            return

        # Keep the new Task Token for reference later
        task_token = response.get("taskToken", "")

        # If there are no tasks, return
        if not task_token:
            return

        # Parse task input and create Glue job input
        task_input = ""
        try:
            task_input = json.loads(str(response["input"]))
            if isinstance(task_input, dict):
                task_input_dict = json.loads(task_input["input"])
                glue_job_name = task_input["job"]
            else:
                task_input_dict = json.loads(task_input)
                glue_job_name = task_input_dict["GlueJobName"]

            config_string = task_input_dict["--config"]
            logger_variables = json.loads(config_string)
            run_id = logger_variables["pipeline"]["run_id"]
            survey = logger_variables["survey"]

            logger.info(f"Got task with run id {run_id} survey {survey}")
        except Exception as e:
            logger.error(f"Unable to load config: {e}")
            return

        try:
            # We need to base64 encode the json for config otherwise it'll
            # be mangled on the way through
            task_input_dict["--config"] = base64.b64encode(
                task_input_dict["--config"].encode("ascii")
            ).decode("ascii")
            logger.info('Running Glue job named "{}"..'.format(glue_job_name))
            response = glue.start_job_run(
                JobName=glue_job_name,
                Arguments=task_input_dict,
                MaxCapacity=glue_job_capacity,
            )

            glue_job_run_id = response["JobRunId"]

            # Store SFN 'Task Token' and Glue Job 'Run Id' in DynamoDB
            item = {
                "sfn_activity_arn": sfn_activity_arn,
                "glue_job_name": glue_job_name,
                "glue_job_run_id": glue_job_run_id,
                "sfn_task_token": task_token,
            }

            ddb_table.put_item(Item=item)
            logger.info("Glue job run started. Run Id: {}".format(glue_job_run_id))

        except Exception as e:
            logger.error(f"Failed to start Glue job named {glue_job_name}: {e}")
            response = sfn.send_task_failure(
                taskToken=task_token,
                error="Failed to start Glue job."
                      "Check Glue Runner logs for more details.",
            )
            return
