import random
import json
import boto3
import botocore
from botocore.client import Config
import os

from es_aws_functions import general_functions, exception_classes

lambda_name = "spp-res_lam_step_runner"
# Because Step Functions client uses long polling, read timeout has to be > 60 seconds
sfn_client_config = Config(connect_timeout=100, read_timeout=200)
sfn = boto3.client("stepfunctions", config=sfn_client_config)
sfn_arn = os.environ["sfn_glue_name"]
ddb_client = boto3.resource("dynamodb")

ddb_table = ddb_client.Table("spp-res_ddb_pipeline_runs")


def split_s3_path(s3_path):
    path_parts = s3_path.replace("s3://", "").split("/")
    bucket = path_parts.pop(0)
    key = "/".join(path_parts)
    return bucket, key


def handler(event, context):
    environment = os.environ.get("environment")
    # Set up logger with just environment
    logger = general_functions.get_logger(None, lambda_name, environment, None)
    logger.info("Started steprunner")
    json_data = event["Records"][0]["body"].replace("\n", " ")
    event = json.loads(json_data)
    pipeline = json.loads(event["Payload"]["pipeline"])
    environment = pipeline["environment"]
    run_id = event["Payload"]["run_id"]
    survey = event["Payload"]["survey"]
    bpm_queue_url = pipeline["bpm_queue_url"]
    snapshot_location = event["Payload"]["snapshot_location"]

    logger.info("Retrieved event vars, starting logger")
    try:
        logger = general_functions.get_logger(survey, lambda_name, environment, run_id)
    except Exception as e:
        error_message = general_functions.handle_exception(
            e, lambda_name, run_id, context=context, bpm_queue_url=bpm_queue_url
        )
        logger.error(error_message)
        raise exception_classes.LambdaFailure(error_message)

    logger.info(f"Event: {event}")
    logger.info(f"JSON config: {json_data}")
    logger.info(f"Config type: {type(json_data)}")

    try:
        logger.info("Checking Snapshot file " + snapshot_location)
        bucket_name, full_file_name = split_s3_path(snapshot_location)

        s3client = boto3.resource("s3", region_name="eu-west-2")
        s3_object = s3client.Object(bucket_name, full_file_name)
        s3_object.get()

    except botocore.exceptions.ClientError as e:

        if "AccessDenied" in str(e):
            message = "There was access denied on the file"
        elif "NoSuchKey" in str(e):
            message = "The file doesnt exist in the location"
        logger.error(message)
        return json.dumps({"statusCode": 400, "body": message})
    except Exception:

        message = "There was an error retrieving the file"
        logger.error(message)
        return json.dumps({"statusCode": 400, "body": message})

    glue_spark_flag = pipeline["spark"]
    glue_job_name = ""
    if glue_spark_flag:
        glue_job_name = os.environ["glue_emr_job_name"]
    elif not glue_spark_flag:
        glue_job_name = os.environ["glue_pyshell_job_name"]
    else:
        logger.info(
            "Error Steprunner :glue_job_name has not been set."
            "Check glurunner lambda log for more information"
        )
    logger.info("Steprunner: glue_job_name : " + str(glue_job_name))

    try:

        run_id = event["Payload"]["run_id"]

        logger.info(f"Assigning run ID {run_id}")

        logger.info(f"Saving metadata to Dynamo table '{ddb_table.table_name}'")
        logger.info(f"Metadata: {json.dumps(event['Payload'])}")

        response = ddb_table.put_item(
            TableName=ddb_table.table_name,
            Item={
                "run_id": run_id,
                "category": event["Payload"]["survey"],
                "template_id": event["Payload"]["template_id"],
                "template_version": event["Payload"]["template_version"],
                "client_id": event["Payload"]["client_id"],
                "run_status": "SUBMITTED",
                "runtime_config": event["Payload"]["pipeline"],
            },
        )

        logger.info(f"Dynamo response: {str(response)}")

    except Exception as ex:
        return str(ex)

    try:

        logger.info(f"Inserting run ID into config")

        event["Payload"]["pipeline"] = pipeline
        event["Payload"]["pipeline"]["run_id"] = run_id

        logger.info(
            f"Running {lambda_name} Lambda to invoke Step Function "
            f"{sfn_arn.split(':')[-1]}"
        )

        # Expects a 'Payload' dictionary at the top-level in the event.
        if event.get("Payload") is None:
            raise KeyError(
                "Top-level 'Payload' object attribute expected in event but not found"
            )

        logger.info(f"Payload: {str(event['Payload'])}")

        sfn_response = sfn.start_execution(
            stateMachineArn=sfn_arn,
            name="Lambda-Execution-" + run_id + str(random.randint(1111, 9999)),
            input=json.dumps(
                json.dumps(
                    {
                        "GlueJobName": glue_job_name,
                        "--config": json.dumps(event["Payload"]),
                    }
                )
            ),
        )

        logger.info("State machine response: " + str(sfn_response))

    except Exception as ex:
        return str(ex)

    return {"statusCode": 200, "body": sfn_response["executionArn"]}
