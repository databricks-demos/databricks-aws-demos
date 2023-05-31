# Databricks notebook source
#To reset the data and restart the demo from scratch, switch the widget to True and run the "%run ./_resources/00-setup $reset_all_data=$reset_all_data" cell below.
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
dbutils.widgets.text("cloud_storage_path", "s3://{bucket_name}", "S3 Bucket")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Obtain the S3 bucket name from the AWS Console
# MAGIC 
# MAGIC Bucket name will look like : db-workshop-376145009885-ap-southeast-2-0d54ddd0

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=$reset_all_data $cloud_storage_path=$cloud_storage_path

# COMMAND ----------

# MAGIC %run ../_resources/00-basedata $reset_all_data=$reset_all_data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Let's use DBUtils to explore a Bucket
# MAGIC 
# MAGIC Databricks Documentation 
# MAGIC 
# MAGIC Databricks Utils https://docs.databricks.com/dev-tools/databricks-utils.html
# MAGIC 
# MAGIC Working with S3 https://docs.databricks.com/external-data/amazon-s3.html

# COMMAND ----------

# DBTITLE 1,(dbutils) List files using dbutils
# Example
# dbutils.fs.ls({path})

display(dbutils.fs.ls(cloud_storage_path+"/ingest"))

# COMMAND ----------

# DBTITLE 1,(python) Read all files into Dataframe
df = spark.read.format("json").load(cloud_storage_path+"/ingest")

df.display()
df.count()

# COMMAND ----------

# DBTITLE 1,(python) Read specific file into Dataframe
df = spark.read.format("json").load(dbutils.fs.ls(cloud_storage_path+"/ingest")[0][0])

df.display()
df.count()


# COMMAND ----------

# DBTITLE 1,(python) Read specific file into Dataframe and add MetaData

df = spark.read.format("json").load(dbutils.fs.ls(cloud_storage_path+"/ingest")[0][0]).select("*", "_metadata")
df.display()

# COMMAND ----------

# DBTITLE 1,(python) Create a Temporary View
df = spark.read.format("json").load(dbutils.fs.ls(cloud_storage_path+"/ingest")[0][0]).select("*", "_metadata")
df.createOrReplaceTempView('vw_json_files')

# COMMAND ----------

# DBTITLE 1,(sql) Query the Temporary View
# MAGIC %sql
# MAGIC SELECT * FROM vw_json_files LIMIT 10

# COMMAND ----------

# DBTITLE 1,(sql) Read all files
# MAGIC %sql
# MAGIC SELECT * FROM json.`${da.cloud_storage_path}/ingest`

# COMMAND ----------

# DBTITLE 1,(sql) Create a Delta Table from Files
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `iot_data` AS SELECT * FROM json.`${da.cloud_storage_path}/ingest`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(calories_burnt) FROM iot_data

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC # What is Databricks Auto Loader?
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/autoloader/autoloader-edited-anim.gif" style="float:right; margin-left: 10px" />
# MAGIC 
# MAGIC [Databricks Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html) lets you scan a cloud storage folder (S3, ADLS, GS) and only ingest the new data that arrived since the previous run.
# MAGIC 
# MAGIC This is called **incremental ingestion**.
# MAGIC 
# MAGIC Auto Loader can be used in a near real-time stream or in a batch fashion, e.g., running every night to ingest daily data.
# MAGIC 
# MAGIC Auto Loader provides a strong gaurantee when used with a Delta sink (the data will only be ingested once).
# MAGIC 
# MAGIC ## How Auto Loader simplifies data ingestion
# MAGIC 
# MAGIC Ingesting data at scale from cloud storage can be really hard at scale. Auto Loader makes it easy, offering these benefits:
# MAGIC 
# MAGIC 
# MAGIC * **Incremental** & **cost-efficient** ingestion (removes unnecessary listing or state handling)
# MAGIC * **Simple** and **resilient** operation: no tuning or manual code required
# MAGIC * Scalable to **billions of files**
# MAGIC   * Using incremental listing (recommended, relies on filename order)
# MAGIC   * Leveraging notification + message queue (when incremental listing can't be used)
# MAGIC * **Schema inference** and **schema evolution** are handled out of the box for most formats (csv, json, avro, images...)
# MAGIC 
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fauto_loader%2Fnotebook&dt=FEATURE_AUTOLOADER">

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Using Auto Loader
# MAGIC 
# MAGIC In the cell below, a function is defined to demonstrate using Databricks Auto Loader with the PySpark API. This code includes both a Structured Streaming read and write.
# MAGIC 
# MAGIC The following notebook will provide a more robust overview of Structured Streaming. If you wish to learn more about Auto Loader options, refer to the <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html" target="_blank">documentation</a>.
# MAGIC 
# MAGIC Note that when using Auto Loader with automatic <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-schema.html" target="_blank">schema inference and evolution</a>, the 4 arguments shown here should allow ingestion of most datasets. These arguments are explained below.
# MAGIC 
# MAGIC | argument | what it is | how it's used |
# MAGIC | --- | --- | --- |
# MAGIC | **`data_source`** | The directory of the source data | Auto Loader will detect new files as they arrive in this location and queue them for ingestion; passed to the **`.load()`** method |
# MAGIC | **`source_format`** | The format of the source data |  While the format for all Auto Loader queries will be **`cloudFiles`**, the format of the source data should always be specified for the **`cloudFiles.format`** option |
# MAGIC | **`table_name`** | The name of the target table | Spark Structured Streaming supports writing directly to Delta Lake tables by passing a table name as a string to the **`.table()`** method. Note that you can either append to an existing table or create a new table |
# MAGIC | **`checkpoint_directory`** | The location for storing metadata about the stream | This argument is passed to the **`checkpointLocation`** and **`cloudFiles.schemaLocation`** options. Checkpoints keep track of streaming progress, while the schema location tracks updates to the fields in the source dataset |
# MAGIC 
# MAGIC **NOTE**: The code below has been streamlined to demonstrate Auto Loader functionality. We'll see in later lessons that additional transformations can be applied to source data before saving them to Delta Lake.

# COMMAND ----------

# DBTITLE 1,(python) Use Autoloader to Read Cloud Files as a Stream
schema_location = cloud_storage_path + "/ingest/schema"

bronzeDF = (spark.readStream \
                .format("cloudFiles")
                .option("cloudFiles.format", "json")
                .option("cloudFiles.maxFilesPerTrigger", 1)  #demo only, remove in real stream. Default is 1000
                .option("cloudFiles.schemaLocation", schema_location)
                .option("rescuedDataColumn", "_rescue") # data that does not match schema is placed in _rescue column
                #.schema("address string") # you can provide schema hints
                .load(cloud_storage_path+"/ingest")
                .select("*", "_metadata")) # add metadata to bronze so we know the source files etc


# COMMAND ----------

bronzeDF.writeStream \
                .format("delta") \
                .option("checkpointLocation", cloud_storage_path+"/bronze/bronze_iot_stream/checkpoint") \
                .trigger(once=True) \
                .option("mergeSchema", "true") \
                .table("iot_autoloader_demo") # table name

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM iot_autoloader_demo

# COMMAND ----------

# DBTITLE 1,Check Files State in the CheckPoint
# MAGIC %sql
# MAGIC SELECT * FROM cloud_files_state("${da.cloud_storage_path}/bronze/bronze_iot_stream/checkpoint");

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata FROM iot_autoloader_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY iot_autoloader_demo 

# COMMAND ----------

# DBTITLE 1,Add more Data
file_counter = add_data(file_counter)

#Go To CMD #13 to rerun AutoLoader

# COMMAND ----------

# DBTITLE 1,Optimize table
# MAGIC %sql
# MAGIC OPTIMIZE iot_autoloader_demo

# COMMAND ----------

# DBTITLE 1,Analyse table 
# MAGIC %sql
# MAGIC ANALYZE TABLE iot_autoloader_demo COMPUTE STATISTICS 

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Ingesting a high volume of input files
# MAGIC Scanning folders with many files to detect new data is an expensive operation, leading to ingestion challenges and higher cloud storage costs.
# MAGIC 
# MAGIC To solve this issue and support an efficient listing, Databricks autoloader offers two modes:
# MAGIC 
# MAGIC - Incremental listing with `cloudFiles.useIncrementalListing` (recommended), based on the alphabetical order of the file's path to only scan new data: (`ingestion_path/YYYY-MM-DD`)
# MAGIC - Notification system, which sets up a managed cloud notification system sending new file name to a queue (when we can't rely on file name order). See `cloudFiles.useNotifications` for more details.
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/autoloader-mode.png" width="700"/>
# MAGIC 
# MAGIC Use the incremental listing option whenever possible. Databricks Auto Loader will try to auto-detect and use the incremental approach when possible.

# COMMAND ----------

bronzeDF = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{cloud_storage_path}/auto_loader/inferred_schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudfiles.useNotifications", "true")
    .option(
        "cloudFiles.backfillInterval", "1 week"
    )  # Auto Loader can trigger asynchronous backfills at a given interval, e.g. 1 day to backfill once a day, or 1 week
    .load(cloud_storage_path+"/ingest_sns")
)

# COMMAND ----------

bronzeDF.writeStream \
                .format("delta") \
                .option("checkpointLocation", cloud_storage_path+"/bronze/bronze_iot_sns_stream/checkpoint") \
                .trigger(once=True) \
                .option("mergeSchema", "true") \
                .outputMode("append") \
                .table("iot_autoloader_demo_sns") # table name

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cloud_files_state("${da.cloud_storage_path}/bronze/bronze_iot_sns_stream/checkpoint");

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY iot_autoloader_demo_sns 

# COMMAND ----------

# DBTITLE 1,Add More files
file_counter_sns = move_file_sns(file_counter_sns)

# COMMAND ----------

display(dbutils.fs.ls(cloud_storage_path+"/ingest_sns"))

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Support for images
# MAGIC Databricks Auto Loader provides native support for images and binary files.
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/autoloader-images.png" width="800" />
# MAGIC 
# MAGIC Just set the format accordingly and the engine will do the rest: `.option("cloudFiles.format", "binaryFile")`
# MAGIC 
# MAGIC Use-cases:
# MAGIC 
# MAGIC - ETL images into a Delta table using Auto Loader
# MAGIC - Automatically ingest continuously arriving new images
# MAGIC - Easily retrain ML models on new images
# MAGIC - Perform distributed inference using a pandas UDF directly from Delta 
