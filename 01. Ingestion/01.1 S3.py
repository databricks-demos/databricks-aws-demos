# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 1 Overview:
# MAGIC
# MAGIC We'll cover the following methods and functionalities:
# MAGIC
# MAGIC - **Exploring S3 using `dbutils`**: Dbutils is a set of utility functions and a library provided by Databricks that can be used for various tasks including exploring the data and file systems.
# MAGIC
# MAGIC - **Querying S3 using SQL**: Databricks supports SQL for querying data, allowing us to read data directly from S3 using SQL syntax.
# MAGIC
# MAGIC - **Ingestion with Databricks Autoloader**: Autoloader is a Databricks feature that utilizes cloud events for incremental data ingestion from cloud storage.
# MAGIC
# MAGIC - **Ingestion using Spark APIs**: This will include options like using Spark's DataFrameReader API to load data directly from S3 into Databricks.
# MAGIC
# MAGIC - **Ingestion using Databricks Delta Lake**: Delta Lake allows us to make our data lake reliable with ACID transactions, and it also has inbuilt functionality to read data from S3.

# COMMAND ----------

#To reset the data and restart the demo from scratch, switch the widget to True and run the "%run ./_resources/00-setup $reset_all_data=$reset_all_data" cell below.
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=$reset_all_data 

# COMMAND ----------

# MAGIC %run ../_resources/00-basedata $reset_all_data=$reset_all_data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##1. S3 Exploration
# MAGIC ### Using dbutils:
# MAGIC Dbutils is a utility library provided by Databricks which allows interacting with the system, including the underlying filesystem.
# MAGIC
# MAGIC Here is an example of how to use dbutils.fs.ls to list files in S3:
# MAGIC
# MAGIC
# MAGIC ####Databricks Documentation 
# MAGIC
# MAGIC Databricks Utils https://docs.databricks.com/dev-tools/databricks-utils.html
# MAGIC
# MAGIC Working with S3 https://docs.databricks.com/external-data/amazon-s3.html

# COMMAND ----------

# Example
# dbutils.fs.ls({path})

display(dbutils.fs.ls(cloud_storage_path+"/ingest"))

# COMMAND ----------

#You can also use dbutils.fs.head to display the beginning of a file:
print(dbutils.fs.head(cloud_storage_path+"/csv/departuredelays.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Python: Loading and Displaying JSON Data
# MAGIC
# MAGIC The below Python code reads JSON files from a specified path in the cloud storage into a Spark DataFrame using the `spark.read.format("json").load()` function. It then uses `df.display()` to display the content of the DataFrame in a tabular format, showing the structure and the first few rows of data. Finally, `df.count()` is used to provide the total number of rows in the DataFrame, indicating the size of the data.
# MAGIC
# MAGIC ### Key Functions or Methods
# MAGIC
# MAGIC - `spark.read.format("json").load()`: Reads JSON files from a specified path into a DataFrame.
# MAGIC - `df.display()`: Displays the content of the DataFrame in a tabular format.
# MAGIC - `df.count()`: Returns the total number of rows in the DataFrame.
# MAGIC
# MAGIC ### When is it useful
# MAGIC
# MAGIC This is useful when you need to load and analyze JSON data stored in a cloud storage. It provides a quick way to view the data structure, preview the data, and understand the size of the dataset. It is commonly used in data exploration and initial data analysis stages.

# COMMAND ----------

df = spark.read.format("json").load(cloud_storage_path+"/ingest")

df.display()
df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Python: Loading and Displaying JSON Data from a Specific File
# MAGIC
# MAGIC The below Python code reads a JSON file from a specific path in the cloud storage into a Spark DataFrame. It uses `dbutils.fs.ls()` to list the contents of the `/ingest` directory in the cloud storage and selects the first file path from the resulting list. The selected file path is then passed to the `spark.read.format("json").load()` function to load the JSON data into a DataFrame. It further uses `df.display()` to show the content of the DataFrame in a tabular format and `df.count()` to provide the total number of rows in the DataFrame.
# MAGIC
# MAGIC ### Key Functions or Methods
# MAGIC
# MAGIC - `dbutils.fs.ls()`: Lists the files and directories under a specified path in the cloud storage.
# MAGIC - `spark.read.format("json").load()`: Reads JSON data from a specified file path and loads it into a DataFrame.
# MAGIC - `df.display()`: Displays the content of the DataFrame in a tabular format.
# MAGIC - `df.count()`: Returns the total number of rows in the DataFrame.
# MAGIC
# MAGIC ### When is it useful
# MAGIC
# MAGIC This is useful when you want to load and analyze a specific JSON file from a directory in cloud storage. It allows you to select a specific file based on your requirements and load its data into a DataFrame for further analysis. It can be helpful when dealing with large datasets stored in a cloud environment.

# COMMAND ----------

# DBTITLE 1,(python) Read specific file into Dataframe
#The below code uses an path directly to the file
print(cloud_storage_path)
df = spark.read.format("json").load(cloud_storage_path+"/ingest/part-00001.json.gz")

df.display()
df.count()


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Python: Loading, Selecting, and Displaying JSON Data with Additional Metadata
# MAGIC
# MAGIC You can get metadata information for input files with the _metadata column. The _metadata column is a hidden column, and is available for all input file formats. 
# MAGIC
# MAGIC After loading the data, the code uses the `.select()` method on the DataFrame to select all columns (`"*"`) and an additional column named `_metadata`. This allows you to include additional metadata information in the DataFrame.
# MAGIC
# MAGIC These columns are very useful to add as part of your Raw to Bronze ingestion pipeline allowing for reconciliation back to source files.
# MAGIC
# MAGIC | Name                    | Type      | Description                                   | Example                  | Minimum Databricks Runtime release |
# MAGIC |-------------------------|-----------|-----------------------------------------------|--------------------------|-----------------------------------|
# MAGIC | file_path               | STRING    | File path of the input file.                  | file:/tmp/f0.csv         | 10.5                              |
# MAGIC | file_name               | STRING    | Name of the input file along with its extension. | f0.csv                | 10.5                              |
# MAGIC | file_size               | LONG      | Length of the input file, in bytes.           | 628                      | 10.5                              |
# MAGIC | file_modification_time  | TIMESTAMP | Last modification timestamp of the input file.| 2021-12-20 20:05:21     | 10.5                              |
# MAGIC | file_block_start        | LONG      | Start offset of the block being read, in bytes.| 0                     | 13.0                              |
# MAGIC | file_block_length       | LONG      | Length of the block being read, in bytes.     | 628                      | 13.0                              |
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC https://docs.databricks.com/ingestion/file-metadata-column.html#file-metadata-column

# COMMAND ----------

# DBTITLE 1,(python) Read specific file into Dataframe and add MetaData

df = spark.read.format("json").load(cloud_storage_path+"/ingest/part-00001.json.gz").select("*", "_metadata")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Python: Loading JSON Data, Selecting Columns, and Creating a Temporary View
# MAGIC
# MAGIC The below Python code reads a JSON file from a specific path in the cloud storage into a Spark DataFrame. It uses `dbutils.fs.ls()` to list the contents of the `/ingest` directory in the cloud storage and selects the first file path from the resulting list. The selected file path is then passed to the `spark.read.format("json").load()` function to load the JSON data into a DataFrame.
# MAGIC
# MAGIC After loading the data, the code uses the `.select()` method on the DataFrame to select all columns (`"*"`) and an additional column named `_metadata`. This allows you to include additional metadata information in the DataFrame.
# MAGIC
# MAGIC Finally, `df.createOrReplaceTempView('vw_json_files')` is used to create a temporary view named `vw_json_files` from the DataFrame. This view can be used to query the DataFrame using SQL statements.
# MAGIC
# MAGIC ### Key Functions or Methods
# MAGIC
# MAGIC - `dbutils.fs.ls()`: Lists the files and directories under a specified path in the cloud storage.
# MAGIC - `spark.read.format("json").load()`: Reads JSON data from a specified file path and loads it into a DataFrame.
# MAGIC - `DataFrame.select()`: Selects specific columns or applies transformations on a DataFrame.
# MAGIC - `DataFrame.createOrReplaceTempView()`: Creates a temporary view from a DataFrame.
# MAGIC
# MAGIC ### When is it useful
# MAGIC
# MAGIC This is useful when you want to load a specific JSON file from a directory in cloud storage, include additional metadata in the resulting DataFrame, and create a **temporary view to query the data using SQL**. The temporary view provides a convenient way to interact with the DataFrame using SQL statements, which can be useful for complex data manipulations and analysis.

# COMMAND ----------

df = spark.read.format("json").load(dbutils.fs.ls(cloud_storage_path+"/ingest")[0][0]).select("*", "_metadata")
df.createOrReplaceTempView('vw_json_files')

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL: Selecting Rows from a View
# MAGIC
# MAGIC The below SQL statement selects all columns (`*`) from the `vw_json_files` view and limits the result to the first 10 rows using the `LIMIT` clause.
# MAGIC
# MAGIC ### When is it useful
# MAGIC
# MAGIC This is useful when you want to retrieve a limited number of rows from a view in a SQL database. The `LIMIT` clause allows you to control the number of rows returned, which can be helpful for quick data exploration or to preview a subset of the data.

# COMMAND ----------

# DBTITLE 1,(sql) Query the Temporary View
# MAGIC %sql
# MAGIC SELECT * FROM vw_json_files LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL: Selecting Rows from a JSON Data Source
# MAGIC
# MAGIC The below SQL statement selects all columns (`*`) from a JSON data source using the `json` function in Spark SQL. The `${da.cloud_storage_path}/ingest` expression is used to dynamically reference the specified path in the cloud storage.
# MAGIC
# MAGIC ### When is it useful
# MAGIC
# MAGIC This is useful when you want to directly query and select rows from a JSON data source in a SQL database. The `json` function allows you to access and process JSON data using SQL statements, providing flexibility in querying and analyzing JSON datasets.

# COMMAND ----------

# DBTITLE 1,(sql) Read all files
# MAGIC %sql
# MAGIC SELECT * FROM json.`${da.cloud_storage_path}/ingest`

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL: Creating a Table from JSON Data
# MAGIC
# MAGIC The below SQL statement creates or replaces a table named `iot_data` by selecting all columns (`*`) from a JSON data source using the `json` function in Spark SQL. The `${da.cloud_storage_path}/ingest` expression is used to dynamically reference the specified path in the cloud storage.
# MAGIC
# MAGIC ### When is it useful
# MAGIC
# MAGIC This is useful when you want to create a table in a SQL database to store and query JSON data. By selecting columns from a JSON data source, you can define the structure and schema of the table based on the JSON data, making it easier to query and analyze the data using SQL statements.

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

# MAGIC %md
# MAGIC ## Python: Writing Streaming Data to a Delta Table
# MAGIC
# MAGIC The below Python code writes a streaming DataFrame (`bronzeDF`) to a Delta table. It uses the `writeStream` function to initiate the streaming write process. Several options and configurations are specified for the write operation.
# MAGIC
# MAGIC - `.format("delta")`: Sets the output format as Delta, indicating that the data should be written to a Delta table.
# MAGIC - `.option("checkpointLocation", cloud_storage_path+"/bronze/bronze_iot_stream/checkpoint")`: Specifies the checkpoint location where the streaming query state will be stored. This is necessary for fault tolerance and recovery.
# MAGIC - `.trigger(once=True)`: Sets the trigger to "once", meaning the streaming write operation will execute only once and then stop. This is typically used for initial data loading.
# MAGIC - `.option("mergeSchema", "true")`: Enables schema merging, allowing the Delta table schema to be automatically updated if there are any schema changes in the incoming data.
# MAGIC - `.table("iot_autoloader_demo")`: Specifies the name of the Delta table where the data will be written.
# MAGIC
# MAGIC ### When is it useful
# MAGIC
# MAGIC This is useful when you want to continuously write streaming data to a Delta table for real-time processing and analysis. The Delta format provides reliability, scalability, and transactional capabilities, making it suitable for streaming workloads. You can use this approach to ingest and store data in near real-time and enable downstream analytics on the data as it arrives.

# COMMAND ----------

# DBTITLE 1,(python) Use WriteStream to create our Delta Table
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

# MAGIC %md
# MAGIC ## SQL: Querying Cloud Files State
# MAGIC
# MAGIC The below SQL statement queries the cloud files state by using the `cloud_files_state` function in Delta Lake. The `${da.cloud_storage_path}/bronze/bronze_iot_stream/checkpoint` expression is used to reference the checkpoint location where the state is stored.
# MAGIC
# MAGIC ### When is it useful
# MAGIC
# MAGIC This is useful when you want to examine the state of the cloud files checkpoint location in Delta Lake. The `cloud_files_state` function allows you to query information about the files stored in cloud storage, such as their names, sizes, and modification timestamps. This can be helpful for monitoring and troubleshooting purposes.

# COMMAND ----------

# DBTITLE 1,Check Files State in the CheckPoint
# MAGIC %sql
# MAGIC SELECT * FROM cloud_files_state("${da.cloud_storage_path}/bronze/bronze_iot_stream/checkpoint");

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL: Selecting Distinct Metadata Values
# MAGIC
# MAGIC The below SQL statement selects distinct values from the `_metadata` column in the `iot_autoloader_demo` table.
# MAGIC
# MAGIC ### When is it useful
# MAGIC
# MAGIC This is useful when you want to retrieve unique values from the `_metadata` column in a table. It allows you to identify and analyze distinct metadata values associated with the data. This can be valuable for understanding different aspects or properties of the dataset, such as source information, timestamps, or other relevant metadata attributes.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata FROM iot_autoloader_demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL: Describing Table History
# MAGIC
# MAGIC The below SQL statement describes the history of the `iot_autoloader_demo` table. It provides information about the table's version history, including the operations performed, the timestamp of each operation, and any associated metadata changes.
# MAGIC
# MAGIC ### When is it useful
# MAGIC
# MAGIC This is useful when you want to track the historical changes and evolution of a table in Delta Lake. The `DESCRIBE HISTORY` command allows you to view the table's version history, including details about insertions, updates, deletions, and metadata changes. This can be helpful for auditing purposes, understanding data lineage, or investigating data modifications over time.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY iot_autoloader_demo 

# COMMAND ----------

# DBTITLE 1,Add more Data
file_counter = add_data(file_counter)

#Go To CMD #13 to rerun AutoLoader

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL: Optimizing a Delta Table
# MAGIC
# MAGIC The below SQL statement optimizes the `iot_autoloader_demo` table in Delta Lake.
# MAGIC
# MAGIC ### When is it useful
# MAGIC
# MAGIC This is useful when you want to optimize the performance and storage of a Delta table. The `OPTIMIZE` command reorganizes the data files and performs data compaction, improving query performance and reducing storage overhead. It is typically used after a significant amount of data has been added, updated, or deleted from the table, or when you want to reclaim storage space by removing unused files.

# COMMAND ----------

# DBTITLE 1,Optimize table
# MAGIC %sql
# MAGIC OPTIMIZE iot_autoloader_demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL: Computing Table Statistics
# MAGIC
# MAGIC The below SQL statement computes statistics for the `iot_autoloader_demo` table.
# MAGIC
# MAGIC ### When is it useful
# MAGIC
# MAGIC This is useful when you want to compute and update the statistics of a table in Delta Lake. Computing statistics helps the query optimizer make better decisions when generating query plans, resulting in improved query performance. By analyzing the table's data distribution, column min/max values, and other statistics, the query planner can optimize query execution strategies and leverage indexing or partitioning techniques more effectively.

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

# MAGIC %md
# MAGIC ### Describing Table History in Databricks
# MAGIC
# MAGIC #### Objective
# MAGIC In this lab, you will learn how to describe the history of a table in Databricks using the `DESCRIBE HISTORY` command. This command provides a historical record of all operations on the table.
# MAGIC
# MAGIC #### Using `DESCRIBE HISTORY`
# MAGIC
# MAGIC `DESCRIBE HISTORY` is a SQL command provided by Databricks that returns the metadata of a table as well as the history of operations that have been performed on it. This includes operations like creating the table, altering it, and writing to it.
# MAGIC
# MAGIC For more information about the DESCRIBE HISTORY command, refer to the  [Databricks Documentation](https://docs.databricks.com/sql/language-manual/delta-lake/describe-history.html)

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Managing Access with Unity Catalog
# MAGIC With the Unity Catalog, you can manage access permissions to your data tables across multiple workspaces.
# MAGIC
# MAGIC To set up Unity Catalog, you need to follow the Databricks guide.
# MAGIC
# MAGIC After setting up Unity Catalog, you can grant permissions on a table using SQL commands like below:
# MAGIC
# MAGIC ``GRANT SELECT ON TABLE my_table TO `{principal}``

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Best Practices
# MAGIC **Partitioning**: When dealing with large datasets, use partitioning to optimize reads and writes in Databricks.
# MAGIC
# MAGIC **Caching**: Use caching to improve performance of iterative and interactive data tasks.
# MAGIC
# MAGIC **Optimizing data formats**: Use efficient data format, Delta is the best format for your Lakehouse.
# MAGIC
# MAGIC **Delta Lake Maintenance**: Regularly run `OPTIMIZE` and `VACUUM` commands to maintain the health and performance of Delta tables. Optimizing coalesces small files and enables better compression. Vacuuming removes old versions of data no longer needed. Also, remember to compute statistics on your Delta tables using `ANALYZE TABLE COMPUTE STATISTICS`. These operations improve the performance of reads and enable Spark to make better query optimization decisions.
# MAGIC
# MAGIC `# Replace 'my_table' with your actual Delta table name.`
# MAGIC
# MAGIC - `spark.sql("OPTIMIZE my_table")`
# MAGIC - `spark.sql("VACUUM my_table")`
# MAGIC - `spark.sql("ANALYZE TABLE my_table COMPUTE STATISTICS")`
# MAGIC
# MAGIC **Monitoring and debugging**: Always monitor job progress, keep track of logs, and use built-in tools to diagnose and fix issues.
