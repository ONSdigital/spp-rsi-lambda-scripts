import boto3
import os
from es_aws_functions import general_functions

current_module = "spp-res_lam_glue_runner"

# Load environment variables
environment = os.environ.get("environment")
spark_glue_job_capacity = int(os.environ.get("spark_glue_job_capacity"))
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
    if glue_info['detail']['state'] == "SUCCEEDED":
        logger.info(f"Job run {glue_info['detail']['jobRunId']} succeeded")
        response = glue.get_job_run(
            JobName=glue_info["detail"]["jobName"],
            RunId=glue_info["detail"]["jobRunId"]
        )

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


def handler(event, context):
    try:
        if event.get("source") == "aws.glue":
            check_glue_job(event)

        else:
            start_glue_jobs(emr_glue_name, event)

        return {
            'statusCode': 200,
            'body': 'Glue job successfully started.'
        }

    except Exception:
        logger.exception(f"There was an error starting glue jobs.")
        return {
            'statusCode': 500,
            'body': 'Failed to start glue job.'
        }
