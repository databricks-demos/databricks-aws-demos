# Databricks notebook source
# MAGIC %md
# MAGIC ### Using AWS SDK for Python (Boto3) to create resources
# MAGIC <a>https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html</a>

# COMMAND ----------

# DBTITLE 1,Create the Kinesis Stream
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

print(response)

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

generate(kinesisStreamName,client)

# COMMAND ----------

kinesisData = (spark.readStream
                  .format("kinesis")
                  .option("streamName", kinesisStreamName)
                  .option("region", kinesisRegion)
                  .option("initialPosition", 'TRIM_HORIZON')
                  .load()
                )

# COMMAND ----------

# DBTITLE 1,Define a Schema to use
from pyspark.sql.types import *

pythonSchema = StructType() \
          .add("event_time", TimestampType()) \
          .add("ticker", StringType()) \
          .add ("price", DoubleType())

# COMMAND ----------

from pyspark.sql.functions import col, from_json

newDF = kinesisData.selectExpr("cast (data as STRING) jsonData") \
            .select(from_json("jsonData", pythonSchema).alias("payload")) \
            .select("payload.*")
display(newDF)

# COMMAND ----------

# DBTITLE 1,Clean up resources
response = client.delete_stream(
    StreamName=kinesisStreamName
)
