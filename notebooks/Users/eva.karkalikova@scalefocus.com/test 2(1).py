# Databricks notebook source
# DBTITLE 1,Make some imports
import pandas as pd  
import numpy as np

# COMMAND ----------

# DBTITLE 1,Define dropdown widget for the secret key for database name
dbutils.widgets.dropdown("p_KEY_DB_NAME", "db-name-ingestion", ("db-name-ingestion", "db-name-curation", "db-name-golden"))
key_db_name = dbutils.widgets.get("p_KEY_DB_NAME")
print("ℹ️ SQL DB secret key name : [" + key_db_name + "]")

# COMMAND ----------

# DBTITLE 1,Unmount directories
#dbutils.fs.unmount("/mnt/dimensions")
#dbutils.fs.unmount("/mnt/payments")

# COMMAND ----------

# DBTITLE 1,Mount dimensions container inside sftrainingstorageaccount
# bcb11899-a36f-4c22-92e7-0e8c745d07b5 - the Application (client) ID when creating App (client)
# databricks-sf-training-secret-scope  - the scope name, which you create trough databricks platform (go to https://adb-4174698215592367.7.azuredatabricks.net/#secrets/createScope)
# sf-training-app                      - the client secret, saved inside key vault, created within the App (client)  (certificates & secrets > new client secret)
# 204cb148-e6fd-421f-825f-cabf6a4a4a55 - the Directory (tenant) ID when creating the App (client)
# dimensions                             - the name of the container
# sftrainingstorageaccount             - the name of the storage account

#configs = {"fs.azure.account.auth.type": "OAuth",
#          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
#          "fs.azure.account.oauth2.client.id": "bcb11899-a36f-4c22-92e7-0e8c745d07b5",
#          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="databricks-sf-training-secret-scope",key="sf-training-app"),
#          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/204cb148-e6fd-421f-825f-cabf6a4a4a55/oauth2/token"}
#
## Optionally, you can add <directory-name> to the source URI of your mount point.
#dbutils.fs.mount(
#  source = "abfss://dimensions@sftrainingstorageaccount.dfs.core.windows.net/",
#  mount_point = "/mnt/dimensions/",
#  extra_configs = configs)

# COMMAND ----------

# DBTITLE 1,Mount payments container inside sftrainingstorageaccount
# bcb11899-a36f-4c22-92e7-0e8c745d07b5 - the Application (client) ID when creating App (client)
# databricks-sf-training-secret-scope  - the scope name, which you create trough databricks platform (go to https://adb-4174698215592367.7.azuredatabricks.net/#secrets/createScope)
# sf-training-app                      - the client secret, saved inside key vault, created within the App (client)  (certificates & secrets > new client secret)
# 204cb148-e6fd-421f-825f-cabf6a4a4a55 - the Directory (tenant) ID when creating the App (client)
# payments                             - the name of the container
# sftrainingstorageaccount             - the name of the storage account

#configs = {"fs.azure.account.auth.type": "OAuth",
#          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
#          "fs.azure.account.oauth2.client.id": "bcb11899-a36f-4c22-92e7-0e8c745d07b5",
#          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="databricks-sf-training-secret-scope",key="sf-training-app"),
#          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/204cb148-e6fd-421f-825f-cabf6a4a4a55/oauth2/token"}
#
#dbutils.fs.mount(
#  source = "abfss://payments@sftrainingstorageaccount.dfs.core.windows.net/",
#  mount_point = "/mnt/payments/",
#  extra_configs = configs)

# COMMAND ----------

# DBTITLE 1,Read a csv file from ADLS
df = sqlContext.read.format('csv').options(header='true', inferSchema='true')\
                                  .option("delimiter", ",")\
                                  .load("/mnt/dimensions/paymentstatuses/PaymentStatuses.csv")
display(df)

# COMMAND ----------

# DBTITLE 1,Read an excel file from ADLS
excel_df = pd.read_excel("/dbfs/mnt/payments/test_excel.xlsx")
excel_df2 = pd.DataFrame(excel_df)
display(excel_df2)

# COMMAND ----------

# DBTITLE 1,Read JSON file
df = pd.read_json("/dbfs/mnt/payments/payments/ingestion/payments.json")
display(df)

# COMMAND ----------

data = {
    'company': 'XYZ pvt ltd',
    'location': 'London',
    'info': {
        'president': 'Rakesh Kapoor',
        'contacts':  'contact@xyz.com'
          
        
    }
}

k = pd.json_normalize(data)
display(k)

# COMMAND ----------

import json

with open('/dbfs/mnt/payments/payments/ingestion/payments_nested.json') as data_file:    
    data = json.load(data_file) 
    
nested_json = pd.json_normalize(data, 'Payment', ['PaymentStatusName', 'PaymentAmount', 'PaymentDate'], 
                    record_prefix='pnts_')
display(nested_json)    

# COMMAND ----------

from pandas.io.json import json_normalize

# COMMAND ----------

nested_json = pd.json_normalize("/dbfs/mnt/payments/payments/ingestion/payments_nested.json")
display(nested_json)

# COMMAND ----------

nested_json = pd.json_normalize("/dbfs/mnt/payments/payments/ingestion/payments_nested.json", max_level=1)
display(nested_json)

# COMMAND ----------

# DBTITLE 1,List the file directories to see the available files in ADLS
listFiles = np.array(dbutils.fs.ls("dbfs:/mnt/dimensions/paymentstatuses/"))
print(listFiles)
print("")
listFiles2 = np.array(dbutils.fs.ls("dbfs:/mnt/payments/"))
print(listFiles2)

# COMMAND ----------

# DBTITLE 1,Connect to the azure sql database and get some tables
# host-name, key_db_name,  db-user, db-password  - settings values, saved inside a key vault in the resource group

# SQL Settings
jdbc_Hostname = dbutils.secrets.get(scope = "databricks-sf-training-secret-scope", key = "host-name")
jdbc_Database = dbutils.secrets.get(scope = "databricks-sf-training-secret-scope", key = key_db_name)
jdbc_User = dbutils.secrets.get(scope = "databricks-sf-training-secret-scope", key = "db-user")
jdbc_Password =  dbutils.secrets.get(scope = "databricks-sf-training-secret-scope", key = "db-password")
jdbc_Port = 1433

# JDBC URLs for different layers
jdbc_Url = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbc_Hostname,jdbc_Port,jdbc_Database)

# Connection string
connection_Properties = {"user": jdbc_User,"password":jdbc_Password,"driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver"}
 
# Get the tables from SQL server    
CustomerDim = spark.read.jdbc(url=jdbc_Url,table='am.CustomerDim',properties=connection_Properties)
BankStatuses = spark.read.jdbc(url=jdbc_Url,table='am.BankStatuses',properties=connection_Properties)
Payments = spark.read.jdbc(url=jdbc_Url,table='am.Payments',properties=connection_Properties)
PaymentStatuses = spark.read.jdbc(url=jdbc_Url,table='am.PaymentStatuses',properties=connection_Properties)


# COMMAND ----------

# DBTITLE 1,Review CustomerDim table
display(CustomerDim)

# COMMAND ----------

# DBTITLE 1,Review BankStatuses table
display(BankStatuses)

# COMMAND ----------

# DBTITLE 1,Review Payments table
display(Payments)

# COMMAND ----------

# DBTITLE 1,Review PaymentStatuses table
display(PaymentStatuses)