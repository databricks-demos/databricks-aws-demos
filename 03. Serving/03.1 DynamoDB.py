# Databricks notebook source
# DBTITLE 1,Create a DynamoDB Table using boto3
import boto3
# Create a boto3 client for DynamoDB
dynamodb = boto3.client('dynamodb',region_name='ap-southeast-2')
table_name = 'LookUpTable'
table = dynamodb.create_table(
    TableName=table_name,
    KeySchema=[
        {
            'AttributeName': 'brand',
            'KeyType': 'HASH'  #Partition key
        },
        {
            'AttributeName': 'model',
            'KeyType': 'RANGE'  #Sort key
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'brand',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'model',
            'AttributeType': 'S'
        },

    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 1,
        'WriteCapacityUnits': 1
    }
)

# COMMAND ----------

# DBTITLE 1,Insert record into Table
# Put an item in the table
table_name = 'LookUpTable'
dynamodb.put_item(
    TableName=table_name,
    Item={
        'brand': {'S': 'BMW'},
        'model': {'S': '3 Series'}
    }
)

# COMMAND ----------

# DBTITLE 1,Retrieve record from Table
# Define the key of the item to retrieve
key = {'brand': {'S': 'BMW'},'model':{'S':'3 Series'}}

# Get the item from the table
response = dynamodb.get_item(TableName=table_name, Key=key)

# Print the item
print(response['Item'])


# COMMAND ----------

# DBTITLE 1,Retrieve record into Spark Dataframe
item = response['Item']
item_dict = {k:v['S'] if 'S' in v else v['N'] for k,v in item.items()}

# Create a DataFrame from the dictionary
df = spark.createDataFrame([item_dict])

# Show the DataFrame
df.display()

# COMMAND ----------

# DBTITLE 1,Clean Up
response = dynamodb.delete_table(TableName='LookUpTable')
