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

# MAGIC %md
# MAGIC # Before we start we need the RDS DNS
# MAGIC
# MAGIC Log in to the AWS Management Console - Open your preferred web browser and navigate to https://aws.amazon.com/console/. Enter your account details to log in.
# MAGIC
# MAGIC 1. **Navigate to the RDS Service** - Once you're logged into the AWS Management Console, look for the "Services" dropdown in the top left corner of the screen. Click on it and search for "RDS" in the search bar, then select "RDS" to go to the Amazon RDS Dashboard.
# MAGIC
# MAGIC 2. **Open RDS Instances Dashboard** - In the RDS Dashboard, look for the "Databases" option in the left-hand navigation pane. Clicking on "Databases" will take you to a list of all your RDS instances.
# MAGIC
# MAGIC 3. **Select the Desired RDS Instance** - In the list of RDS instances, find the instance for which you want to get the DNS name. Click on the instance name to open its details page.
# MAGIC
# MAGIC 4. **Get the DNS Name** - In the details page of the selected RDS instance, look for the "Endpoint & port" section. The endpoint listed here is the DNS name of your RDS instance. You will use this DNS name to establish a connection from your Databricks environment to your RDS instance.

# COMMAND ----------

# MAGIC %run ../_resources/01-config

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=$reset_all_data $cloud_storage_path=$cloud_storage_path

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Lab 3: Consuming Amazon RDS
# MAGIC **1. Connecting to Amazon RDS MySQL Database**
# MAGIC
# MAGIC To interact with Amazon RDS from Databricks, we need to establish a connection using JDBC. We create a JDBC URL, which includes the RDS endpoint, port, and database name. Along with this, we specify the connection properties which include the username, password, and driver details.
# MAGIC
# MAGIC ```
# MAGIC jdbcHostname = "<RDS-endpoint>"
# MAGIC jdbcDatabase = "<database-name>"
# MAGIC jdbcPort = 3306
# MAGIC jdbcUrl = f"jdbc:mysql://{jdbcHostname}:{jdbcPort}/{jdbcDatabase}"
# MAGIC
# MAGIC connectionProperties = {
# MAGIC   "user" : "<username>",
# MAGIC   "password" : "<password>",
# MAGIC   "driver" : "com.mysql.jdbc.Driver",
# MAGIC   "ssl" : "true"   # SSL for secure connection
# MAGIC }
# MAGIC ```

# COMMAND ----------

jdbcHostname = dbutils.widgets.get("rds_endpoint")
jdbcDatabase = 'demodb'
jdbcPort = "3306"
username = 'labuser'
password = get_secret(dbutils.widgets.get("region_name"),dbutils.widgets.get("secret_name"))

jdbcUrl = f"jdbc:mysql://{database_host}:{database_port}/{database_name}"

connectionProperties = {
  "user" : username,
  "password" : password,
  "driver" : "com.mysql.jdbc.Driver",
  "ssl" : "true"   # SSL for secure connection
}
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
# MAGIC ### 2. Writing to an RDS Table
# MAGIC We can write data from a Spark DataFrame back to an RDS table using the `write.jdbc` function. We need to specify the JDBC URL, the name of the destination table, the write mode (overwrite, append, etc.), and the connection properties.

# COMMAND ----------

#create a dataframe to write to the Database
df = spark.createDataFrame( [ ("Bilbo",     50), 
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

# DBTITLE 0,Write Dataframe to RDS
df.write.jdbc(url=jdbcUrl, table="people", mode="overwrite", properties=connectionProperties)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2. Reading from an RDS Table
# MAGIC To read from an RDS table, we can use the `spark.read.jdbc` function. This function takes the JDBC URL, table name, and connection properties as parameters. We can either perform a full table read or a filtered read. For a filtered read, we specify the SQL query as the table parameter.

# COMMAND ----------

# DBTITLE 0,Read RDS Table
# Full table read
df = spark.read.jdbc(url=jdbcUrl, table="people", properties=connectionProperties)

df.display()
df.printSchema()

# COMMAND ----------

# Filtered read
n = 5 # Number of rows to take
sql = "(SELECT * FROM people order by age LIMIT {0} ) AS tmp".format(int(n))
df = spark.read.jdbc(url=jdbcUrl, table=sql, properties=connectionProperties)

df.display()
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 3: Create a Delta table
# MAGIC
# MAGIC Delta Lake is a powerful technology that brings reliability, performance, and lifecycle management to data lakes. We can convert our DataFrame to a Delta table to leverage these benefits. The Delta table can be queried using SQL and provides ACID transaction guarantees.

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
