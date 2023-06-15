# Databricks notebook source
dbutils.widgets.text("secret_name", "SecretsManagerRDSAdmin-PXdUV6inVRtw", "AWS Secret Name")
dbutils.widgets.text("rds_endpoint", "rds-03-dbcluster-tcw38kgtdnct.cluster-cwmkorvgxwf8.ap-southeast-2.rds.amazonaws.com", "RDS Endpoint")
dbutils.widgets.text("region_name", "ap-southeast-2", "AWS Region")

# COMMAND ----------

pip install faker

# COMMAND ----------

#%run ./01-config

# COMMAND ----------

# database_host = dbutils.widgets.get("rds_endpoint")
# database_name = 'demodb'
# database_port = "3306"
# username = 'labuser'
# password = get_secret(dbutils.widgets.get("region_name"),dbutils.widgets.get("secret_name"))


# url = f"jdbc:mysql://{database_host}:{database_port}/{database_name}"

# print(url)

# COMMAND ----------

# import mysql.connector

# mydb = mysql.connector.connect(
#   host=database_host,
#   user=username,
#   password=password,
#   database=database_name
# )

# mycursor = mydb.cursor()


# COMMAND ----------

sql_drop = """DROP TABLE IF EXISTS `customers`;"""
# mycursor.execute(sql_drop)
# mydb.commit()

# COMMAND ----------

sql_create = """CREATE TABLE `customers` (
  `customerNumber` int NOT NULL AUTO_INCREMENT,
  `customerName` varchar(50) NOT NULL,
  `contactLastName` varchar(50) NOT NULL,
  `contactFirstName` varchar(50) NOT NULL,
  `phone` varchar(50) NOT NULL,
  `addressLine1` varchar(50) NOT NULL,
  `addressLine2` varchar(50) DEFAULT NULL,
  `city` varchar(50) NOT NULL,
  `state` varchar(50) DEFAULT NULL,
  `postalCode` varchar(15) DEFAULT NULL,
  `country` varchar(50) NOT NULL,
  `salesRepEmployeeNumber` int(11) DEFAULT NULL,
  `creditLimit` decimal(10,2) DEFAULT NULL,
  PRIMARY KEY (`customerNumber`),
  KEY `salesRepEmployeeNumber` (`salesRepEmployeeNumber`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;"""
# mycursor.execute(sql_create)
# mydb.commit()

# COMMAND ----------


from faker import Faker
fake = Faker()
fake.seed_locale('en_US', 0)
sql_inserter = ''
for i in range(100):
    fake_firstname = fake.first_name()
    fake_lastname = fake.last_name()
    fake_name = fake.name()
    fake_email = fake.ascii_company_email()
    fake_address = fake.street_address()
    fake_city = fake.city()
    fake_phone = fake.phone_number()
    fake_zip = fake.zipcode()
    fake_country = fake.current_country_code()
    fake_state = fake.state_abbr()
    fake_rep_id = fake.unique.random_int(min=400, max=100000)
    fake_credit_limit = fake.unique.random_int(min=400, max=100000)
    sql_insert = f"insert  into `customers`(`customerName`,`contactLastName`,`contactFirstName`,`phone`,`addressLine1`,`addressLine2`,`city`,`state`,`postalCode`,`country`,`salesRepEmployeeNumber`,`creditLimit`) values ('{fake_name}','{fake_lastname}','{fake_firstname}','{fake_phone}','{fake_address}',NULL,'{fake_city}','{fake_state}','{fake_zip}','{fake_country}',{fake_rep_id},'{fake_credit_limit}');"
    sql_inserter += sql_insert
    #print(sql_insert)
    # mycursor.execute(sql_insert)
    # mydb.commit()

# COMMAND ----------

print(sql_drop + sql_create + sql_inserter)
