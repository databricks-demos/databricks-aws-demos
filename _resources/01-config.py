# Databricks notebook source
#pip install mysql.connector

# COMMAND ----------

#pip install faker

# COMMAND ----------

dbutils.widgets.text("stack", "db_workshop", "CFN Stack")
dbutils.widgets.text("region_name", 'ap-southeast-2', "AWS Region")

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

# def get_rds_endpoint(cluster_name,region_name):
#     # region_name = 'us-east-1'
#     # cluster_name = 'workshop-serverless-cluster'
#     client = boto3.client('rds',region_name=region_name)
#     response = client.describe_db_cluster_endpoints(
#         DBClusterIdentifier=cluster_name
#     )
#     return response['DBClusterEndpoints'][0]['Endpoint']



# COMMAND ----------

#import boto3
client = boto3.client('cloudformation',region_name=dbutils.widgets.get("region_name"))
response = client.describe_stacks(StackName=dbutils.widgets.get("stack"))
stack = response['Stacks'][0] 

outputs = stack['Outputs']

desired_output_keys = ['DatabrickWorkshopBucket', 'RDSendpoint', 'RDSsecret']
cfn_outputs = {}

for output in outputs:
    output_key = output['OutputKey']
    if output_key in desired_output_keys:
        cfn_outputs[output_key] = output['OutputValue']

workshop_bucket = cfn_outputs['DatabrickWorkshopBucket']
rds_endpoint = cfn_outputs['RDSendpoint']
rds_user = 'labuser'
rds_password = get_secret(dbutils.widgets.get("region_name"),cfn_outputs['RDSsecret'])
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


