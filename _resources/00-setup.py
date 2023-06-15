# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
dbutils.widgets.text("db_prefix", "workshop", "Database prefix")
dbutils.widgets.text("cloud_storage_path", "s3://{bucket_name}", "S3 Bucket")
dbutils.widgets.text("region_name", "ap-southeast-2", "AWS Region")
dbutils.widgets.text("stack", "cfn-workspace", "CFN Stack")
dbutils.widgets.text("catalog", "db_workshop", "Catalog")

# COMMAND ----------

# MAGIC %run ../_resources/01-config $region_name=$region_name $stack=$stack

# COMMAND ----------

cloud_storage_path = 's3://'+spark.conf.get("da.workshop_bucket") #dbutils.widgets.get("cloud_storage_path")
print("cloud_storage_path :"+cloud_storage_path)
spark.conf.set("da.cloud_storage_path",cloud_storage_path)

# COMMAND ----------

reset_all_data = dbutils.widgets.get("reset_all_data")

# COMMAND ----------

import re
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
if current_user.rfind('@') > 0:
  current_user_no_at = current_user[:current_user.rfind('@')]
else:
  current_user_no_at = current_user
current_user_no_at = re.sub(r'\W+', '_', current_user_no_at)


# COMMAND ----------

db_prefix = dbutils.widgets.get("db_prefix")
dbName = db_prefix+"_"+current_user_no_at
print(dbName)

# COMMAND ----------

def use_and_create_db(catalog, dbName, cloud_storage_path = None):
  print(f"USE CATALOG `{catalog}`")
  #print(f"""create schema if not exists `{dbName}` MANAGED LOCATION '{cloud_storage_path}/tables' """)
  spark.sql(f"USE CATALOG `{catalog}`")
  spark.sql(f"""create schema if not exists `{dbName}` MANAGED LOCATION '{cloud_storage_path}/tables' """)


# COMMAND ----------

if reset_all_data:
  spark.sql(f"DROP DATABASE IF EXISTS `{dbName}` CASCADE")
  
current_catalog = dbutils.widgets.get("catalog")
catalogs = [r['catalog'] for r in spark.sql("SHOW CATALOGS").collect()]
if len(catalogs) == 1 and catalogs[0] in ['hive_metastore', 'spark_catalog']:
  print(f"UC doesn't appear to be enabled")
  catalog = "hive_metastore"
else:
  if current_catalog not in catalogs:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {current_catalog}")
  catalog = current_catalog
use_and_create_db(catalog, dbName,cloud_storage_path)

print(f"using cloud_storage_path {cloud_storage_path}")
print(f"using catalog.database `{catalog}`.`{dbName}`")
spark.conf.set("da.catalog",catalog)
spark.conf.set("da.dbName",dbName)
#Add the catalog to cloud storage path as we could have 1 checkpoint location different per catalog
if catalog not in ['hive_metastore', 'spark_catalog']:
  #cloud_storage_path+="_"+catalog
  try:
    spark.sql(f"GRANT CREATE, USAGE on DATABASE {catalog}.{dbName} TO `account users`")
    spark.sql(f"ALTER SCHEMA {catalog}.{dbName} OWNER TO `account users`")
  except Exception as e:
    print("Couldn't grant access to the schema to all users:"+str(e))
  
#with parallel execution this can fail the time of the initialization. add a few retry to fix these issues
for i in range(10):
  try:
    spark.sql(f"""USE `{catalog}`.`{dbName}`""")
    break
  except Exception as e:
    time.sleep(1)
    if i >= 9:
      raise e
      
