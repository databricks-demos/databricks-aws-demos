# Databricks notebook source
# MAGIC %md
# MAGIC **Python: Creating a Kinesis Data Stream**
# MAGIC
# MAGIC **Description:**
# MAGIC
# MAGIC The below Python code demonstrates how to create a Kinesis Data Stream using the Boto3 library. It imports the `boto3` module, sets the desired stream name and region, creates a client object, and calls the `create_stream()` method with the specified parameters.

# COMMAND ----------

import boto3

kinesisStreamName = "StockTickerStream"
kinesisRegion = "ap-southeast-2"
client = boto3.client('kinesis',region_name=kinesisRegion)

response = client.create_stream(
    StreamName=kinesisStreamName,
    StreamModeDetails={
        'StreamMode': 'ON_DEMAND'
    }
)

# COMMAND ----------

spark.conf.set("spark.databricks.kinesis.listShards.enabled", False)

# COMMAND ----------

import datetime
import json
import random

def get_data():
    return {
        'event_time': datetime.datetime.now().isoformat(),
        'ticker': random.choice(['AAPL', 'AMZN', 'MSFT', 'INTC', 'TBV']),
        'price': round(random.random() * 100, 2)}


def generate(stream_name, kinesis_client):
     for val in range(300):
        data = get_data()
        #print(data)
        kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(data),
            PartitionKey="partitionkey")

# COMMAND ----------


