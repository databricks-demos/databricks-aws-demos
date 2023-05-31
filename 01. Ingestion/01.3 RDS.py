# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Load data from MySQL to Delta Lake
# MAGIC 
# MAGIC This notebook shows you how to import data from JDBC MySQL databases into a Delta Lake table using Python.
# MAGIC 
# MAGIC https://docs.databricks.com/external-data/jdbc.html

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
dbutils.widgets.text("cloud_storage_path", "s3://db-workshop-376145009395-us-east-1-8b79c6d0", "S3 Bucket")
dbutils.widgets.text("region_name", "ap-southeast-2", "AWS Region")
dbutils.widgets.text("secret_name", "SecretsManagerRDSAdmin-6Qr5W3OXPTFG", "AWS Secret Name")
dbutils.widgets.text("rds_endpoint", "aws-lab-01-dms-01-rdsdbinstance-03hj4qaymwpq.cbdjtos45q8c.us-east-1.rds.amazonaws.com", "RDS Endpoint")

# COMMAND ----------

# MAGIC %run ../_resources/01-config

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=$reset_all_data $cloud_storage_path=$cloud_storage_path

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step 1: Connection information
# MAGIC 
# MAGIC First define some variables to programmatically create these connections.

# COMMAND ----------

#table_name = 'pg_foreign_table'
database_host = dbutils.widgets.get("rds_endpoint")
database_name = 'demodb'
database_port = "3306"
username = 'labuser'
password = get_secret(dbutils.widgets.get("region_name"),dbutils.widgets.get("secret_name"))

url = f"jdbc:mysql://{database_host}:{database_port}/{database_name}"

print(url)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The full URL printed out above should look something like:
# MAGIC 
# MAGIC ```
# MAGIC jdbc:postgresql://localhost:3306/my_database
# MAGIC jdbc:mysql://localhost:3306/my_database
# MAGIC ```
# MAGIC 
# MAGIC ### Check connectivity
# MAGIC 
# MAGIC Depending on security settings for your Postgres database and Databricks workspace, you may not have the proper ports open to connect.
# MAGIC 
# MAGIC Replace `<database-host-url>` with the universal locator for your Postgres implementation. If you are using a non-default port, also update the 5432.
# MAGIC 
# MAGIC Run the cell below to confirm Databricks can reach your Postgres database.

# COMMAND ----------

# MAGIC %sh 
# MAGIC nc -vz "<database-host-url>" 3306

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step 2: Write data
# MAGIC 
# MAGIC With the jdbc connection defined. We can use this to connect to the database and write some data for testing.
# MAGIC 
# MAGIC First, create a DataFrame in Python. This DataFrame contains names and ages.

# COMMAND ----------

people = spark.createDataFrame( [ ("Bilbo",     50), 
                                  ("Gandalf", 1000), 
                                  ("Thorin",   195),  
                                  ("Balin",    178), 
                                  ("Kili",      77),
                                  ("Dwalin",   169), 
                                  ("Oin",      167), 
                                  ("Gloin",    158), 
                                  ("Fili",      82), 
                                  ("Bombur",  None)
                                ], 
                                ["name", "age"] 
                              )

# COMMAND ----------

# DBTITLE 1,Write Dataframe to RDS
( people.write 
    .format( "jdbc" ) 
    .option( "url"    , url ) 
    .option( "dbtable", "people" ) 
    .option("user", "labuser") 
    .option("password", password)
    .mode("overwrite")
    .save()
)

# COMMAND ----------

people = spark.createDataFrame( [ (1, "Anne", 1) ], 
                                ["id", "name", "age"] 
                              )

# COMMAND ----------

( people.write 
    .format( "jdbc" ) 
    .option( "url"    , url ) 
    .option( "dbtable", "people_new" ) 
    .option("user", "labuser") 
    .option("password", password)
    .mode("append")
    .save()
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step 3: Reading the data
# MAGIC 
# MAGIC Now that you've specified the file metadata, you can create a DataFrame. Use an *option* to infer the data schema from the file. You can also explicitly set this to a particular schema if you have one already.
# MAGIC 
# MAGIC First, create a DataFrame in Python, referencing the variables defined above.

# COMMAND ----------

# DBTITLE 1,Read RDS Table
remote_table = spark.read \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", "people") \
    .option("user", "labuser") \
    .option("password", password) \
    .load()

remote_table.display()
remote_table.printSchema()

# COMMAND ----------

n = 5 # Number of rows to take
sql = "(SELECT * FROM people order by age LIMIT {0} ) AS tmp".format(int(n))

remote_table = spark.read \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", sql) \
    .option("user", "labuser") \
    .option("password", password) \
    .load()

remote_table.display()
remote_table.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step 3: Create a Delta table
# MAGIC 
# MAGIC The DataFrame defined and displayed above is a temporary connection to the remote database.
# MAGIC 
# MAGIC To ensure that this data can be accessed by relevant users throughout your workspace, save it as a Delta Lake table using the code below.

# COMMAND ----------

target_table_name = "`people`"
remote_table.write.mode("overwrite").saveAsTable(target_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This table will persist across cluster sessions, notebooks, and personas throughout your organization.
# MAGIC 
# MAGIC The code below demonstrates querying this data with Python and SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `people`

# COMMAND ----------

display(spark.table(target_table_name))
