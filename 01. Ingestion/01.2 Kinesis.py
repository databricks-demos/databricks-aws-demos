# Databricks notebook source
dbutils.widgets.text("cloud_storage_path", "s3://{bucket_name}", "S3 Bucket")

# COMMAND ----------

cloud_storage_path = dbutils.widgets.get("cloud_storage_path")

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab 2: Consuming Kinesis Streams

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using AWS SDK for Python (Boto3) to create resources
# MAGIC The approach is used in a lab environment. In a normal production environment these cloud resources would be created by the infrastructure team.
# MAGIC <a>https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html</a>

# COMMAND ----------

# MAGIC %run ../_resources/01-setup

# COMMAND ----------

#Generate some data into the Stream. 
generate(kinesisStreamName,client)

# COMMAND ----------

# MAGIC %md
# MAGIC ##  2. Reading from Kinesis Streams
# MAGIC We use Spark Structured Streaming to read data from a Kinesis Stream. Let's define the necessary parameters and read the data.

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

# MAGIC %md
# MAGIC ### 3. Data Serialization/Deserialization
# MAGIC Kinesis data is binary, we need to convert the data into a usable format. Let's assume the data is in UTF-8 encoded strings.
# MAGIC
# MAGIC ```
# MAGIC from pyspark.sql.functions import col
# MAGIC kinesisDF = kinesisDF.selectExpr("CAST(data AS STRING)")
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import col, from_json

kinesisDF = kinesisData.selectExpr("cast (data as STRING) jsonData") \
            .select(from_json("jsonData", pythonSchema).alias("payload")) \
            .select("payload.*")
#display(kinesisDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  4. Sink Data to a Delta Table
# MAGIC Delta Lake provides several advantages over regular Parquet. It provides ACID transactions, scalable metadata handling, and unifies streaming and batch data processing.
# MAGIC
# MAGIC Let's sink the data from the stream to a Delta table.

# COMMAND ----------

kinesisDF.writeStream \
  .format("delta") \
  .option("checkpointLocation", cloud_storage_path+"/delta/checkpoints") \
  .table("stock_ticker")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM stock_ticker

# COMMAND ----------

#Add some more data into the Stream. 
generate(kinesisStreamName,client)
# Now check 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM stock_ticker

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 5. Error Handling and Recovery
# MAGIC Structured Streaming can recover from failures and continue processing where it left off. We'll have to specify a checkpoint location in case of failure. In the above example, /delta/checkpoints is the checkpoint location.
# MAGIC
# MAGIC ### 6. Checkpoints and Job Restarts
# MAGIC Checkpoints store the current state of a streaming query, which can be used to restart the query in case of a failure. You can monitor and analyze checkpoints using Databricks’ built-in structured streaming sink, or by inspecting the checkpoint files directly in the file system.
# MAGIC
# MAGIC To restart a failed job from a checkpoint, simply start the query with the same checkpoint location.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Watermarking and Window Aggregations
# MAGIC If your stream processing includes handling late data or performing window-based operations, you can specify a watermark delay threshold and use window functions for aggregations.
# MAGIC
# MAGIC ```
# MAGIC kinesisDF.withWatermark("timestamp", "10 minutes") \
# MAGIC   .groupBy(window(kinesisDF.timestamp, "10 minutes")) \
# MAGIC   .count()
# MAGIC
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Performance Monitoring and Tuning
# MAGIC You can use AWS CloudWatch and Databricks' built-in Spark UI to monitor the performance of the Kinesis ingestion job. Adjust the number of shards in Kinesis and parameters such as maxRecordsPerFetch and maxRatePerShard in Databricks based on your monitoring insights to achieve optimal performance.
# MAGIC
# MAGIC ### 9. Security and Access Control
# MAGIC IAM roles are used to securely access Kinesis Streams from Databricks. Make sure to assign necessary permissions to your IAM role to read from Kinesis and write to S3 (for checkpointing and sinking data to Delta).
# MAGIC
# MAGIC ### 10. Best Practices
# MAGIC Follow these tips for efficiently consuming Kinesis streams with Databricks:
# MAGIC
# MAGIC - Regularly monitor your jobs and tune your Kinesis shards and Databricks parameters for best performance.
# MAGIC - Use a secure IAM role with minimal necessary permissions.
# MAGIC - If you are dealing with late data or require window-based operations, use watermarking and window functions.
# MAGIC - Make use of Delta Lake's features such as ACID transactions and unified batch and streaming processing.

# COMMAND ----------

# DBTITLE 1,Clean up resources
response = client.delete_stream(
    StreamName=kinesisStreamName
)
