# Databricks notebook source
# DBTITLE 1,Make Imports
#you will need to have numpy( library for arrays), pandas ( for dataframes), re (for regex expressions), pyspark sql functions – in general , pyspark sql functions – lit (specific) and from pyspark sql – types.


# COMMAND ----------

# DBTITLE 1,Create a drop-down widget to hold the database key name and text widget for user initials
#For the drop-down menu, you have 3 options "db-name-ingestion", "db-name-curation", "db-name-golden" The one, which is by default is “db-name-ingestion”


#Hint - https://docs.databricks.com/notebooks/widgets.html

# COMMAND ----------

# MAGIC %md #Start of Ingestion Process

# COMMAND ----------

# DBTITLE 1,Create connection to the database, using the secret scope
#For the scope use : hostkey = ‘host-name’
#database = inherited from the drop-down widget that you have created in Step 2
#user = ‘db-user’
#password = ‘db-passwoed’
#port = 1433
#driver = ‘com.microsoft.sqlserver.jdbc.SQLServerDriver’

 #Hint - https://docs.databricks.com/data/data-sources/sql-databases.html

# COMMAND ----------

# DBTITLE 1,Get [PaymentStatuses] and [BankStatuses] tables from SQL server and display data for [PaymentStatuses]
#You have to get these tables, which are created within your db schema (your initials), that have been created in the Data factory part.

#Hint - https://stackoverflow.com/questions/30983982/how-to-use-jdbc-source-to-write-and-read-data-in-pyspark

# COMMAND ----------

# DBTITLE 1,Display data for [BankStatuses]


# COMMAND ----------

# DBTITLE 1,Get CustomerDim.csv from ADLS Blob container
#With the following options – header = true, inferSchema = true, delimiter = ,
#From the following location – “/mnt/dimensions/customerdim/ingestion/CustomerDim.csv”

#Hint - https://spark.apache.org/docs/latest/sql-data-sources-csv.html

# COMMAND ----------

# DBTITLE 1,Get payments.json from ADLS Blob container in a pandas dataframe
#Here you should use Pandas and read the JSON file from location “/dbfs/mnt/payments/payments/ingestion/payments.json”

#Hint - https://pandas.pydata.org/docs/reference/api/pandas.read_json.html

# COMMAND ----------

# MAGIC %md #End of Ingestion Process

# COMMAND ----------

# DBTITLE 0, 
# MAGIC %md #Start of Curation Process

# COMMAND ----------

# DBTITLE 1,Convert from pandas to pyspark dataframes [payments]
#Here you can Create PySpark SparkSession and create PySpark Dataframe from Pandas

#Hint - https://sparkbyexamples.com/pyspark/convert-pandas-to-pyspark-dataframe/

# COMMAND ----------

# DBTITLE 1,Convert dynamically column names for [payment] to have each word separated by underscore (e.g. PaymentID = payment_id)
#Here you can use the re library

#Hint - https://stackoverflow.com/questions/7322028/how-to-replace-uppercase-with-underscore

# COMMAND ----------

# DBTITLE 1,Convert dynamically column names for [customerDim] to have each word separated by underscore (e.g. PaymentID = payment_id)
#Here you can use the re library

#Hint - https://stackoverflow.com/questions/7322028/how-to-replace-uppercase-with-underscore

# COMMAND ----------

# DBTITLE 1,Convert dynamically column names for [BankStatuses] to have each word separated by underscore (e.g. PaymentID = payment_id)
#Here you can use the re library

#Hint - https://stackoverflow.com/questions/7322028/how-to-replace-uppercase-with-underscore  

# COMMAND ----------

# DBTITLE 1,Convert dynamically column names for [PaymentStatuses] to have each word separated by underscore (e.g. PaymentID = payment_id)
#Here you can use the re library

#Hint - https://stackoverflow.com/questions/7322028/how-to-replace-uppercase-with-underscore

# COMMAND ----------

# DBTITLE 1,Add to [spark_payments] dataframe additional columns for year and month and cast them to integer type (also [unique_customer_numer])
#Hint - https://sparkbyexamples.com/spark/spark-dataframe-withcolumn/

# COMMAND ----------

# DBTITLE 1,In [customerDim] cast [unique_customer_number] as integer type
#Hint - https://sparkbyexamples.com/pyspark/pyspark-cast-column-type/

# COMMAND ----------

# DBTITLE 1,Save [spark_payments] as a parquet file partitioned by [user_initilas], [year] and [week] columns inside ADLS curated folder
#The write options should be : format – parqet; mode = overwrite; mergeSchema= true. The saving location is "dbfs:/mnt/payments/payments/curated/"+ your user initials (again from the widget

#Hint - https://community.cloudera.com/t5/Support-Questions/Write-dataframe-into-parquet-hive-table-ended-with-c000-file/m-p/219972

# COMMAND ----------

# DBTITLE 1,Convert [BankStatuses] and [PaymentStatuses] to SQL views
#In order to save them in the next steps as a databricks tables. You can use createOrReplaceTempView function.

#Hint - https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.createOrReplaceTempView.html

# COMMAND ----------

# DBTITLE 1,Save [bank_statuses] view as a databricks sql table inside [curated_your_initials] database
#First, drop the table if exists [curated_user_initials].[bank.statuses]
#Then create db if not exists [curated_user_initials]
#Finally create the table as selecting everything from the converted pyspark df to SQL view [bank_statuses]

#Hint - you can use "%sql" at the beggining of the cell

# COMMAND ----------

# DBTITLE 1,Save [payment_statuses] view as a databricks sql table inside [curated_your_initials] database
#In general do the same as in Step 17, but this time for the [payment_statuses] view

# COMMAND ----------

# DBTITLE 1,Delete the old [customer_dim] file inside ADLS
#You can use dbutils.fs.rm option and specify the location, which is “/mnt/dimensions/customerdim/curated/” + your initials passed from the widget

#Hint - https://community.databricks.com/s/question/0D53f00001HKHXJCA5/how-to-delete-a-folder-in-databricks-mnt

# COMMAND ----------

# DBTITLE 1,Save the new [customer_dim] file inside curated folder in ADLS
#The write options should be : format – parquet; mode = append; mergeSchema= true. The saving location is " dbfs:/mnt/dimensions/customerdim/curated/"+ your user initials (again from the widget


# COMMAND ----------

# MAGIC %md
# MAGIC #End of Curation Process

# COMMAND ----------

# DBTITLE 1,Call another notebook for generating golden data inside your [user/xxx@yyy.com/] folder
#Hint - https://docs.databricks.com/notebooks/notebook-workflows.html