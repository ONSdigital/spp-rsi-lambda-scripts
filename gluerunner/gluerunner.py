# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import base64
import boto3
import json
import os
from botocore.client import Config
from boto3.dynamodb.conditions import Key, Attr
from es_aws_functions import aws_functions, exception_classes, general_functions

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


def start_glue_jobs():
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
                error="Failed to start Glue job. Check Glue Runner logs for more details.",
            )
            return


def check_glue_jobs():

    # Query all items in table for a particular SFN activity ARN
    # This should retrieve records for all started glue jobs for this particular activity ARN
    ddb_table = dynamodb.Table(ddb_table_name)

    ddb_resp = ddb_table.query(
        KeyConditionExpression=Key("sfn_activity_arn").eq(sfn_activity_arn),
        Limit=ddb_query_limit,
    )

    for item in ddb_resp["Items"]:
        glue_job_run_id = item["glue_job_run_id"]
        glue_job_name = item["glue_job_name"]
        sfn_task_token = item["sfn_task_token"]

        try:

            logger.debug("Polling Glue job run status")

            glue_resp = glue.get_job_run(
                JobName=glue_job_name, RunId=glue_job_run_id, PredecessorsIncluded=False
            )

            job_run_state = glue_resp["JobRun"]["JobRunState"]
            job_run_error_message = glue_resp["JobRun"].get("ErrorMessage", "")

            logger.debug(
                'Job with Run Id {} is currently in state "{}"'.format(
                    glue_job_run_id, job_run_state
                )
            )
            job_key = {
                "sfn_activity_arn": sfn_activity_arn,
                "glue_job_run_id": glue_job_run_id,
            }

            if job_run_state in ["STARTING", "RUNNING", "STARTING", "STOPPING"]:
                job_key = None
                logger.debug(
                    "Job with Run Id {} hasn't succeeded yet.".format(glue_job_run_id)
                )

                # Send heartbeat
                sfn_resp = sfn.send_task_heartbeat(taskToken=sfn_task_token)

                logger.debug("Heartbeat sent to Step Functions.")

            elif job_run_state == "SUCCEEDED":

                logger.info("Job with Run Id {} SUCCEEDED.".format(glue_job_run_id))

                task_output_dict = {
                    "GlueJobName": glue_job_name,
                    "GlueJobRunId": glue_job_run_id,
                    "GlueJobRunState": job_run_state,
                    "GlueJobStartedOn": glue_resp["JobRun"]
                    .get("StartedOn", "")
                    .strftime("%x, %-I:%M %p %Z"),
                    "GlueJobCompletedOn": glue_resp["JobRun"]
                    .get("CompletedOn", "")
                    .strftime("%x, %-I:%M %p %Z"),
                    "GlueJobLastModifiedOn": glue_resp["JobRun"]
                    .get("LastModifiedOn", "")
                    .strftime("%x, %-I:%M %p %Z"),
                }

                task_output_json = json.dumps(task_output_dict)

                sfn_resp = sfn.send_task_success(
                    taskToken=sfn_task_token, output=task_output_json
                )

            elif job_run_state in ["FAILED", "STOPPED"]:

                message = 'Glue job "{}" run with Run Id "{}" failed. Last state: {}. Error message: {}'.format(
                    glue_job_name,
                    glue_job_run_id[:8] + "...",
                    job_run_state,
                    job_run_error_message,
                )

                logger.error(message)

                message_json = {
                    "glue_job_name": glue_job_name,
                    "glue_job_run_id": glue_job_run_id,
                    "glue_job_run_state": job_run_state,
                    "glue_job_run_error_msg": job_run_error_message,
                }

                sfn_resp = sfn.send_task_failure(
                    taskToken=sfn_task_token,
                    cause=json.dumps(message_json),
                    error="GlueJobFailedError",
                )

            else:
                logger.error(f"Unknown job state {job_run_state}")

        except Exception as e:
            logger.error(
                f"Error checking job {glue_job_name} run id: {glue_job_run_id}: {e}"
            )

        if job_key:
            try:
                resp = ddb_table.delete_item(Key=job_key)

            except Exception as e:
                logger.error(f"Failed to delete glue job, error: {e}")


glue = boto3.client("glue")
# Because Step Functions client uses long polling, read timeout has to be > 60 seconds
sfn_client_config = Config(connect_timeout=50, read_timeout=70)
sfn = boto3.client("stepfunctions", config=sfn_client_config)
dynamodb = boto3.resource("dynamodb")


def handler(event, context):
    start_glue_jobs()

    check_glue_jobs()
