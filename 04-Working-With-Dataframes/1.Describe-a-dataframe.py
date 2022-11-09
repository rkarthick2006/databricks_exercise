# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Describe a DataFrame
# MAGIC 
# MAGIC Your data processing in Azure Databricks is accomplished by defining Dataframes to read and process the Data.
# MAGIC 
# MAGIC This notebook will introduce how to read your data using Azure Databricks Dataframes.

# COMMAND ----------

# MAGIC %md
# MAGIC #Introduction
# MAGIC 
# MAGIC ** Data Source **
# MAGIC * One hour of Pagecounts from the English Wikimedia projects captured August 5, 2016, at 12:00 PM UTC.
# MAGIC * Size on Disk: ~23 MB
# MAGIC * Type: Compressed Parquet File
# MAGIC * More Info: <a href="https://dumps.wikimedia.org/other/pagecounts-raw" target="_blank">Page view statistics for Wikimedia projects</a>
# MAGIC 
# MAGIC **Technical Accomplishments:**
# MAGIC * Develop familiarity with the `DataFrame` APIs
# MAGIC * Introduce the classes...
# MAGIC   * `SparkSession`
# MAGIC   * `DataFrame` (aka `Dataset[Row]`)
# MAGIC * Introduce the actions...
# MAGIC   * `count()`

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) **The Data Source**
# MAGIC 
# MAGIC * In this notebook, we will be using a compressed parquet "file" called **pagecounts** (~23 MB file from Wikipedia)
# MAGIC * We will explore the data and develop an understanding of it as we progress.
# MAGIC * You can read more about this dataset here: <a href="https://dumps.wikimedia.org/other/pagecounts-raw/" target="_blank">Page view statistics for Wikimedia projects</a>.
# MAGIC 
# MAGIC We can use **dbutils.fs.ls()** to view our data on the DBFS.

# COMMAND ----------

(source, sasEntity, sasToken) = getAzureDataSource()
print((source, sasEntity, sasToken))
spark.conf.set(sasEntity, sasToken)

# COMMAND ----------

path = source + "/wikipedia/pagecounts/staging_parquet_en_only_clean/"
files = dbutils.fs.ls(path)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC As we can see from the files listed above, this data is stored in <a href="https://parquet.apache.org" target="_blank">Parquet</a> files which can be read in a single command, the result of which will be a `DataFrame`.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Create a DataFrame
# MAGIC * We can read the Parquet files into a `DataFrame`.
# MAGIC * We'll start with the object **spark**, an instance of `SparkSession` and the entry point to Spark 2.0 applications.
# MAGIC * From there we can access the `read` object which gives us an instance of `DataFrameReader`.

# COMMAND ----------

A PySpark DataFrame can be created via pyspark.sql.SparkSession.createDataFrame typically by passing a list of lists, tuples, dictionaries and pyspark.sql.Rows, a pandas DataFrame and an RDD consisting of such a list. pyspark.sql.SparkSession.createDataFrame takes the schema argument to specify the schema of the DataFrame. When it is omitted, PySpark infers the corresponding schema by taking a sample from the data.

# COMMAND ----------

#Firstly, you can create a PySpark DataFrame from a list of rows
from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row

df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
])
df

# COMMAND ----------

#Create a PySpark DataFrame from an RDD consisting of a list of tuples.
rdd = spark.sparkContext.parallelize([
    (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
    (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
])
df = spark.createDataFrame(rdd, schema=['a', 'b', 'c', 'd', 'e'])
df

# COMMAND ----------

df.show()
df.printSchema()
df.columns
df.select("a", "b", "c").describe().show()


# COMMAND ----------

parquetDir = source + "/wikipedia/pagecounts/staging_parquet_en_only_clean/"

# COMMAND ----------

pagecountsEnAllDF = (spark  # Our SparkSession & Entry Point
  .read                     # Our DataFrameReader
  .parquet(parquetDir)      # Returns an instance of DataFrame
)
print(pagecountsEnAllDF)    # Python hack to see the data type

# COMMAND ----------

pagecountsEnAllDF.take(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) count()
# MAGIC 
# MAGIC If you look at the API docs, `count()` is described like this:
# MAGIC > Returns the number of rows in the Dataset.
# MAGIC 
# MAGIC `count()` will trigger a job to process the request and return a value.
# MAGIC 
# MAGIC We can now count all records in our `DataFrame` like this:

# COMMAND ----------

total = pagecountsEnAllDF.count()

print("Record Count: {0:,}".format( total ))

# COMMAND ----------

# MAGIC %md
# MAGIC That tells us that there are around 2 million rows in the `DataFrame`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC 
# MAGIC Start the next lesson, [Use common DataFrame methods]($./2.Use-common-dataframe-methods)
