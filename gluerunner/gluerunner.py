import boto3
import os
from es_aws_functions import general_functions

current_module = "spp-res_lam_glue_runner"

# Environment inwhich we're running
environment = os.environ.get("environment")
# Max number of glue jobs to run at once
spark_glue_job_capacity = int(os.environ.get("spark_glue_job_capacity"))
# The name of the glue job to run
emr_glue_name = os.environ.get("emr_glue_name")


def handler(payload, context):
    # We only have environment and our module name for logging
    logger = general_functions.get_logger(None, current_module, environment, None)

    try:
        glue = boto3.client("glue")
        response = glue.start_job_run(
            JobName=emr_glue_name,
            Arguments=payload,
            MaxCapacity=spark_glue_job_capacity,
        )
        logger.info(f"Started job {emr_glue_name} with Id {response['JobRunId']}")
        return {
            'statusCode': 200,
            'body': 'Glue job successfully started.'
        }

    except Exception:
        logger.exception("Error starting glue job")
        return {
            'statusCode': 500,
            'body': 'Failed to start glue job.'
        }
