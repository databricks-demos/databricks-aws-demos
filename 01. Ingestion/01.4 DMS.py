# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Load data from RDS to Delta Lake using AWS Database Migration Service (DMS)
# MAGIC 
# MAGIC This notebook shows you how to import data from output by DMS. We will show full load and CDC
# MAGIC 
# MAGIC https://aws.amazon.com/dms/

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://cms.databricks.com/sites/default/files/inline-images/db-300-blog-img-1.png"  width="600"/>

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
dbutils.widgets.text("cloud_storage_path", "s3://db-workshop-376145009395-us-east-1-8b79c6d0", "S3 Bucket")
dbutils.widgets.text("region_name", "ap-southeast-2", "AWS Region")
dbutils.widgets.text("secret_name", "SecretsManagerRDSAdmin-6Qr5W3OXPTFG", "AWS Secret Name")
dbutils.widgets.text("rds_endpoint", "aws-lab-01-dms-01-rdsdbinstance-03hj4qaymwpq.cbdjtos45q8c.us-east-1.rds.amazonaws.com", "RDS Endpoint")

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=$reset_all_data $cloud_storage_path=$cloud_storage_path

# COMMAND ----------

# MAGIC %run ../_resources/02-dms-cdc-data

# COMMAND ----------

database_host = dbutils.widgets.get("rds_endpoint")
database_name = 'demodb'
database_port = "3306"
username = 'labuser'
password = get_secret(dbutils.widgets.get("region_name"),dbutils.widgets.get("secret_name"))

url = f"jdbc:mysql://{database_host}:{database_port}/{database_name}"

print(url)

# COMMAND ----------

remote_table = spark.read \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", "customers") \
    .option("user", "labuser") \
    .option("password", password) \
    .load()

remote_table.display()
remote_table.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### run the dms tasks in AWS Console
# MAGIC ### Start Full load and Ongoing replication Tasks

# COMMAND ----------

# DBTITLE 1,Full Load
# MAGIC %sql
# MAGIC SELECT * FROM csv.`${da.cloud_storage_path}/dms-output/demodb/customers/LOAD00000001.csv.gz`

# COMMAND ----------

# MAGIC %run ../_resources/02-dms-cdc-data-generator

# COMMAND ----------

# DBTITLE 1,CDC Data
# MAGIC %sql
# MAGIC SELECT * FROM csv.`${da.cloud_storage_path}/dms-cdc-output/demodb/customers/`
