# Databricks notebook source
#pip install mysql.connector

# COMMAND ----------

#pip install faker

# COMMAND ----------

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

def get_rds_endpoint(cluster_name,region_name):
    # region_name = 'us-east-1'
    # cluster_name = 'workshop-serverless-cluster'
    client = boto3.client('rds',region_name=region_name)
    response = client.describe_db_cluster_endpoints(
        DBClusterIdentifier=cluster_name
    )
    return response['DBClusterEndpoints'][0]['Endpoint']


