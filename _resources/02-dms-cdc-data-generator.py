# Databricks notebook source
dbutils.widgets.text("secret_name", "SecretsManagerRDSAdmin-PXdUV6inVRtw", "AWS Secret Name")
dbutils.widgets.text("rds_endpoint", "rds-03-dbcluster-tcw38kgtdnct.cluster-cwmkorvgxwf8.ap-southeast-2.rds.amazonaws.com", "RDS Endpoint")
dbutils.widgets.text("region_name", "ap-southeast-2", "AWS Region")

# COMMAND ----------



# COMMAND ----------

#%run ./01-config

# COMMAND ----------

database_host = dbutils.widgets.get("rds_endpoint")
database_name = 'demodb'
database_port = "3306"
username = 'labuser'
password = get_secret(dbutils.widgets.get("region_name"),dbutils.widgets.get("secret_name"))


url = f"jdbc:mysql://{database_host}:{database_port}/{database_name}"

print(url)

# COMMAND ----------

import mysql.connector

mydb = mysql.connector.connect(
  host=database_host,
  user=username,
  password=password,
  database=database_name
)

mycursor = mydb.cursor()

#sql_create = "CREATE TABLE IF NOT EXISTS Customers (ID INT PRIMARY KEY AUTO_INCREMENT, Name VARCHAR(255), City VARCHAR(255), ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP);"

#sql_insert = "INSERT INTO Customers (Name,City,CreationDate) VALUES ('John','Perth','2023-01-01'),('Susan','Brisbane','2023-01-02'),('Brett','Sydney','2023-01-03')"
#sql_update = "UPDATE Customers SET Name = 'Susan S Sullivan' WHERE Name = 'Susan'"
# sql_del ="DELETE FROM Customers Where ID = 1"
# mycursor.execute(sql_create)
# mydb.commit()
# mycursor.execute(sql_insert)
# mydb.commit()
# mycursor.execute(sql_update)
# mydb.commit()
# mycursor.execute(sql_del)
# mydb.commit()

# print(mycursor.rowcount, "record(s) affected")

# COMMAND ----------



# COMMAND ----------


from faker import Faker
fake = Faker()
fake.seed_locale('en_US', 0)
for i in range(5):
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
    sql_insert = f"insert  into `customers`(`customerName`,`contactLastName`,`contactFirstName`,`phone`,`addressLine1`,`addressLine2`,`city`,`state`,`postalCode`,`country`,`salesRepEmployeeNumber`,`creditLimit`) values ('{fake_name}','{fake_lastname}','{fake_firstname}','{fake_phone}','{fake_address}',NULL,'{fake_city}','{fake_state}','{fake_zip}','{fake_country}',{fake_rep_id},'{fake_credit_limit}')"
    #print(sql_insert)
    mycursor.execute(sql_insert)
    mydb.commit()

# COMMAND ----------


    
# sql_insert = """insert  into `customers`(`customerNumber`,`customerName`,`contactLastName`,`contactFirstName`,`phone`,`addressLine1`,`addressLine2`,`city`,`state`,`postalCode`,`country`,`salesRepEmployeeNumber`,`creditLimit`) values 

# (${idcount},'Mini Creations Ltd.','Huang','Wing','5085559555','4575 Hillside Dr.',NULL,'New Bedford','MA','50553','USA',1188,'94500.00'),

# (321,'Corporate Gift Ideas Co.','Brown','Julie','6505551386','7734 Strong St.',NULL,'San Francisco','CA','94217','USA',1165,'105000.00'),

# (323,'Down Under Souveniers, Inc','Graham','Mike','+64 9 312 5555','162-164 Grafton Road','Level 2','Auckland  ',NULL,NULL,'New Zealand',1612,'88000.00'),

# (324,'Stylish Desk Decors, Co.','Brown','Ann ','(171) 555-0297','35 King George',NULL,'London',NULL,'WX3 6FW','UK',1501,'77000.00'),

# (328,'Tekni Collectables Inc.','Brown','William','2015559350','7476 Moss Rd.',NULL,'Newark','NJ','94019','USA',1323,'43000.00'),

# (333,'Australian Gift Network, Co','Calaghan','Ben','61-7-3844-6555','31 Duncan St. West End',NULL,'South Brisbane','Queensland','4101','Australia',1611,'51600.00'),

# (334,'Suominen Souveniers','Suominen','Kalle','+358 9 8045 555','Software Engineering Center','SEC Oy','Espoo',NULL,'FIN-02271','Finland',1501,'98800.00'),

# (335,'Cramer Spezialitäten, Ltd','Cramer','Philip ','0555-09555','Maubelstr. 90',NULL,'Brandenburg',NULL,'14776','Germany',NULL,'0.00'),

# (339,'Classic Gift Ideas, Inc','Cervantes','Francisca','2155554695','782 First Street',NULL,'Philadelphia','PA','71270','USA',1188,'81100.00'),

# (344,'CAF Imports','Fernandez','Jesus','+34 913 728 555','Merchants House','27-30 Merchants Quay','Madrid',NULL,'28023','Spain',1702,'59600.00')"""

# sql_insert = sql_insert.format(shepherd, age)
# mycursor.execute(sql_insert)
# mydb.commit()

# COMMAND ----------

fake = Faker()
fake.seed_locale('en_US', 0)
fake_id = fake.unique.random_int(min=1, max=350)
fake_name = fake.name()
sql_update = f"UPDATE `customers` SET customerName = '{fake_name}' WHERE customerNumber = {fake_id}"
mycursor.execute(sql_update)
mydb.commit()

# COMMAND ----------

sql_del = "DELETE FROM `customers` LIMIT 1"
mycursor.execute(sql_del)
mydb.commit()

# COMMAND ----------

# import boto3
# from botocore.exceptions import ClientError
# import json

# def get_secret(region_name,secret_name):
#     # Create a Secrets Manager client
#     session = boto3.session.Session()
#     client = session.client(
#         service_name='secretsmanager',
#         region_name=region_name
#     )

#     try:
#         get_secret_value_response = client.get_secret_value(
#             SecretId=secret_name
#         )
#     except ClientError as e:
#         # For a list of exceptions thrown, see
#         # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
#         raise e

#     # Decrypts secret using the associated KMS key.
#     secret = get_secret_value_response['SecretString']
#     password = json.loads(secret)["password"] 
#     return password

# COMMAND ----------

# database_host = "ee-dms-rdsdbcluster-ddcytnpxp8li.cluster-cwmkorvgxwf8.ap-southeast-2.rds.amazonaws.com"
# database_name = 'demodb'
# database_port = "3306"
# username = 'labuser'
# password = get_secret(dbutils.widgets.get("region_name"),dbutils.widgets.get("secret_name"))
# import mysql.connector

# mydb = mysql.connector.connect(
#   host=database_host,
#   user=username,
#   password=password,
#   database=database_name
# )

# mycursor = mydb.cursor()

# url = f"jdbc:mysql://{database_host}:{database_port}/{database_name}"

# print(url)
# sql = "SELECT TOP 1 customerNumber FROM `customers`"
# mycursor.execute(sql_del)
# mydb.commit()

# COMMAND ----------


