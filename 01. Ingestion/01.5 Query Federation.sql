-- Databricks notebook source
-- MAGIC %run ../_resources/01-config

-- COMMAND ----------

create connection rds_mysql type mysql 
options (
  host `${da.rds_endpoint}`,
  port '3306',
  user `${da.rds_user}`,
  password `${da.rds_password}`
);


-- COMMAND ----------

show connections;

-- COMMAND ----------

describe connection extended rds_mysql; 

-- COMMAND ----------

create foreign catalog mysql_catalog using connection rds_mysql 

