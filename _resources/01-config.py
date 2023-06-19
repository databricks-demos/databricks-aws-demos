# Databricks notebook source
import boto3
from botocore.exceptions import ClientError
import json

def get_secret(region_name,secret_name):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']
    password = json.loads(secret)["password"] 
    return password

# COMMAND ----------

import requests

def get_region():
    # Define the URL and headers
    token_url = "http://169.254.169.254/latest/api/token"
    token_headers = {"X-aws-ec2-metadata-token-ttl-seconds": "21600"}

    # Make the PUT request to get the token
    token_response = requests.put(token_url, headers=token_headers)

    # Get the token from the response
    token = token_response.text

    # Define the URL and headers for the second request
    metadata_url = "http://169.254.169.254/latest/meta-data/placement/region"
    metadata_headers = {"X-aws-ec2-metadata-token": token}

    # Make the GET request using the token
    metadata_response = requests.get(metadata_url, headers=metadata_headers)

    # Print the response
    return metadata_response.text


# COMMAND ----------

import boto3
def get_cfn():
    client = boto3.client('cloudformation',region_name=get_region())
    response = client.describe_stacks()#StackName=dbutils.widgets.get("stack"))
    #stack = response['Stacks'][0] 
    for stack in response['Stacks']:
    #spark.conf.set("da.stack",dbutils.widgets.get("stack"))
        outputs = stack.get('Outputs', [])
        if outputs:

            desired_output_keys = ['DatabrickWorkshopBucket', 'RDSendpoint', 'RDSsecret']
            cfn_outputs = {}

            for output in outputs:
                output_key = output['OutputKey']
                if output_key in desired_output_keys:
                    cfn_outputs[output_key] = output['OutputValue']

            workshop_bucket = cfn_outputs['DatabrickWorkshopBucket']
            rds_endpoint = cfn_outputs['RDSendpoint']
            rds_user = 'labuser'
            rds_password = get_secret(get_region(),cfn_outputs['RDSsecret'])
            spark.conf.set("da.workshop_bucket",workshop_bucket)
            spark.conf.set("da.rds_endpoint",rds_endpoint)
            spark.conf.set("da.rds_user",rds_user)
            spark.conf.set("da.rds_password",rds_password)

    # print(f"""
    # S3 Bucket:                  {cfn_outputs['DatabrickWorkshopBucket']}
    # RDS End Point:              {cfn_outputs['RDSendpoint']}
    # Secret Manager:             {cfn_outputs['RDSsecret']}
    # RDS User:                   labuser
    # RDS Password:               {rds_password}
    # """)

# COMMAND ----------

get_cfn()
