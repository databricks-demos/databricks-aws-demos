-- Databricks notebook source
-- MAGIC %run ../_resources/01-config

-- COMMAND ----------

create connection rds_mysql type mysql 
options (
  host '${da.rds_endpoint}',
  port '3306',
  user 'labuser',
  password secret('q_fed','mysql')
);


-- COMMAND ----------

show connections;

-- COMMAND ----------

describe connection extended rds_mysql; 

-- COMMAND ----------

create foreign catalog mysql_external using connection rds_mysql 


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Now use SQL Warehouse to query##
