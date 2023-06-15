# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Load data from MySQL to Delta Lake
# MAGIC
# MAGIC This notebook shows you how to import data from JDBC MySQL databases into a Delta Lake table using Python.
# MAGIC
# MAGIC https://docs.databricks.com/external-data/jdbc.html

# COMMAND ----------

dbutils.widgets.text("region_name", 'ap-southeast-2', "AWS Region")
dbutils.widgets.text("stack", "cfn-workspace", "CFN Stack")
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=$reset_all_data $stack=$stack

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

jdbcHostname = spark.conf.get("da.rds_endpoint")
jdbcDatabase = 'demodb'
jdbcPort = "3306"
username = spark.conf.get("da.rds_user")
password = spark.conf.get("da.rds_password")

jdbcUrl = f"jdbc:mysql://{jdbcHostname}:{jdbcPort}/{jdbcDatabase}"

connectionProperties = {
  "user" : username,
  "password" : password,
  "ssl" : "true"   # SSL for secure connection
}
print(jdbcUrl)

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
