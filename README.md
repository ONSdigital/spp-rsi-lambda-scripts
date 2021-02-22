# SPP-RSI-lambda-scripts
A collection of python lambda scripts used within the RSI pipeline.

## Deploy
The lambdas are deployed through Serverless and utilise the serverless.yml file for the deployment and configuration information.

## Gluerunner.py
Responsible for communicating and kicking off the glue activity.

## Steprunner.py
steprunner is responsible for adding details around the run into the dynamodb table and then calls out to the step function.

## Dynamo_queryrunner.py
Responsible for querying the dynamodb table to return the config used for a run.