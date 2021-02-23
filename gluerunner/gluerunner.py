import boto3
import os
from es_aws_functions import general_functions

current_module = "spp-res_lam_glue_runner"

# Load environment variables
environment = os.environ.get("environment")
spark_glue_job_capacity = int(os.environ.get("spark_glue_job_capacity"))
ingest_glue_name = os.environ.get("ingest_glue_name")
emr_glue_name = os.environ.get("emr_glue_name")

# Set up logger with just environment at first
logger = general_functions.get_logger(None, current_module, environment, None)
glue = boto3.client("glue")


def start_glue_jobs(job_name, config):
    try:
        response = glue.start_job_run(
            JobName=job_name,
            Arguments=config,
            MaxCapacity=spark_glue_job_capacity,
        )
        logger.info(f"Started job {job_name} with Id {response['JobRunId']}")
    except Exception:
        logger.exception(f"Error starting glue job {job_name}")


def check_glue_job(glue_info):
    args_pass = {}

    if glue_info['detail']['state'] == "SUCCEEDED":
        logger.info(f"Job run {glue_info['detail']['jobRunId']} succeeded")
        response = glue.get_job_run(
            JobName=glue_info["detail"]["jobName"],
            RunId=glue_info["detail"]["jobRunId"]
        )
        args_pass = response["JobRun"]["Arguments"]

    elif glue_info['detail']['state'] in ["FAILED", "STOPPED", "TIMEOUT"]:
        logger.error(
            "Error running %s: run: %s: %s",
            glue_info['detail']['jobName'],
            glue_info['detail']['state'],
            glue_info['detail']['message']
        )

    else:
        logger.error(
            "Glue job %s response does not contain state",
            glue_info['detail']['jobName']
        )

    return args_pass


def handler(event, context):
    if event.get("source") == "aws.glue":
        if event["detail"]["jobName"] == ingest_glue_name:
            config_to_pass = check_glue_job(event)
            if config_to_pass:
                start_glue_jobs(emr_glue_name, config_to_pass)

            else:
                # this means config_to_pass is empty
                logger.error(
                    "Failed to retrieve arguments from finished ingest glue"
                )

        elif event["detail"]["jobName"] == emr_glue_name:
            # Check and log the status of the glue job. The only effect here
            # is a log message.
            check_glue_job(event)

    else:
        # Initial config should be loaded correctly in api_handler.py
        start_glue_jobs(ingest_glue_name, event)
