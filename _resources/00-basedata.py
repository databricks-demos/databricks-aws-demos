# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")


# COMMAND ----------

#comment
file_counter = 1
file_counter_sns = 1
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"

if reset_all_data:
   dbutils.fs.rm(cloud_storage_path+"/ingest/",True)
   dbutils.fs.rm(cloud_storage_path+"/ingest_sns/",True)
   
  

# COMMAND ----------

dbutils.fs.cp(f"/databricks-datasets/flights/departuredelays.csv",cloud_storage_path + f"/csv/departuredelays.csv")

# COMMAND ----------

def move_file(x):
  dbutils.fs.cp(f"/databricks-datasets/iot-stream/data-device/part-{x:05}.json.gz",cloud_storage_path + f"/ingest/part-{x:05}.json.gz")
  x = x+1
  return x

# COMMAND ----------


def move_file_sns(x):
  dbutils.fs.cp(f"/databricks-datasets/iot-stream/data-device/part-{x:05}.json.gz",cloud_storage_path + f"/ingest_sns/part-{x:05}.json.gz")
  x = x+1
  return x

# COMMAND ----------

# Move 
def add_data(x):
  count = x
  for val in range(3):
      count = move_file(count)
  return count  
        


# COMMAND ----------

file_counter = add_data(file_counter)

# COMMAND ----------

file_counter_sns = move_file_sns(file_counter_sns)

# COMMAND ----------

def clean_up():
  spark.sql(f"DROP DATABASE IF EXISTS `{dbName}` CASCADE")
  dbutils.fs.rm(cloud_storage_path+"/ingest/",True)
  dbutils.fs.rm(cloud_storage_path+"/ingest_sns/",True)
  file_counter = add_data(1)
  file_counter_sns = 1  
