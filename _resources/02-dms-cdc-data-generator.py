# Databricks notebook source
import boto3
region = get_region()
client = boto3.client('lambda',region_name=region)
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
