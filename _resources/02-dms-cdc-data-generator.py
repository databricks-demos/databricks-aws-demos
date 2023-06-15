# Databricks notebook source
dbutils.widgets.text("region_name", "ap-southeast-2", "AWS Region")

# COMMAND ----------

import boto3
region = dbutils.widgets.get("region_name")
client = boto3.client('lambda',region_name=dbutils.widgets.get("region_name"))
response = client.invoke(
    FunctionName='db-CDCDataGen',
    InvocationType='RequestResponse',  # Set the invocation type as needed
    LogType='Tail',  # Set the log type as needed
    Payload='{}'  # Pass the payload as a string (if required)
)
status_code = response['StatusCode']
response_payload = response['Payload'].read().decode('utf-8')

# Process the response as needed
#print(f"Status code: {status_code}")
print(f"Response : {response_payload}")
#print(response)
