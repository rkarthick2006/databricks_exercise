# Databricks notebook source
# MAGIC %md
# MAGIC #![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Key Vault-Backed Secret Scopes
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of these lessons, you should be able to:
# MAGIC * Configure Databricks to access Key Vault secrets
# MAGIC * Read and write data directly from Blob Storage using secrets stored in Key Vault
# MAGIC * Set different levels of access permission using SAS at the Storage service level
# MAGIC * Mount Blob Storage into DBFS
# MAGIC * Describe how mounting impacts secure access to data
# MAGIC 
# MAGIC The overall goal of these three notebooks is to read and write data directly from Blob Storage using secrets stored in a Key Vault, accessed securely through the Databricks Secrets utility. 
# MAGIC 
# MAGIC This goal has been broken into 3 notebooks to make each step more digestible:
# MAGIC 1. `1 - Blob Storage` - In the first notebook, we will add a file to a Blob on a Storage Account and generate SAS tokens with different permissions levels
# MAGIC 1. `2 - Key Vault` - In the second notebook, we will configure an Azure Key Vault Access Policy and add text-based credentials as secrets
# MAGIC 1. `3 - Key Vault` Backed Secret Scopes - In the third notebook, we will define a Secret Scope in Databircks by linking to the Key Vault and use the previously stored credentials to read and write from the Storage Container
# MAGIC 
# MAGIC ### Online Resources
# MAGIC 
# MAGIC - [Azure Databricks Secrets](https://docs.azuredatabricks.net/user-guide/secrets/index.html)
# MAGIC - [Azure Key Vault](https://docs.microsoft.com/en-us/azure/key-vault/key-vault-whatis)
# MAGIC - [Azure Databricks DBFS](https://docs.azuredatabricks.net/user-guide/dbfs-databricks-file-system.html)
# MAGIC - [Introduction to Azure Blob storage](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction)
# MAGIC - [Databricks with Azure Blob Storage](https://docs.databricks.com/spark/latest/data-sources/azure/azure-storage.html)
# MAGIC - [Azure Data Lake Storage Gen1](https://docs.azuredatabricks.net/spark/latest/data-sources/azure/azure-datalake.html#mount-azure-data-lake)
# MAGIC - [Azure Data Lake Storage Gen2](https://docs.databricks.com/spark/latest/data-sources/azure/azure-datalake-gen2.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) 3 - Key Vault Backed Secret Scopes
# MAGIC 
# MAGIC In this notebook, we will use the Secret Scopes API to securely connect to the Key Vault. The Secret Scopes API will allow us to use the Blob Storage SAS tokens, stored as Secrets in the Key Vault, to read and write data from Blob Storage. 
# MAGIC 
# MAGIC ### Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC - Create a Secret Scope connected to Azure Key Vault
# MAGIC - Mount Blob Storage to DBFS using a SAS token
# MAGIC - Write data to Blob using a SAS token in Spark Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ### Classroom setup
# MAGIC 
# MAGIC A quick script to define a username variable in Python and Scala.

# COMMAND ----------

# MAGIC %run ./Includes/User-Name

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Access Azure Databricks Secrets UI
# MAGIC 
# MAGIC Now that you have an instance of Azure Key Vault up and running, it is time to let Azure Databricks know how to connect to it.
# MAGIC 
# MAGIC The first step is to open a new web browser tab and navigate to `https://&lt;your_azure_databricks_url&gt;#secrets/createScope` 
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The number after the `?o=` is the unique workspace identifier; append `#secrets/createScope` to this.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/db-secrets.png" width=800px />

# COMMAND ----------

# MAGIC %md
# MAGIC ## Link Azure Databricks to Key Vault
# MAGIC We'll be copy/pasting some values from the Azure Portal to this UI.
# MAGIC 
# MAGIC In the Azure Portal on your Key Vault tab:
# MAGIC 1. Go to properties
# MAGIC 2. Copy and paste the DNS Name
# MAGIC 3. Copy and paste the Resource ID
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/properties.png" width=800px />

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Add configuration values to the Databricks Secret Scope UI that you copied from the Azure Key Vault
# MAGIC 
# MAGIC 
# MAGIC In the Databricks Secrets UI:
# MAGIC 
# MAGIC 1. Enter the name of the secret scope; here, we'll use `students`.
# MAGIC 2. Paste the DNS Name
# MAGIC 3. Paste the Resource ID
# MAGIC 4. Click "Create"
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/db-secrets-complete.png" />
# MAGIC 
# MAGIC   > MANAGE permission allows users to read and write to this secret scope, and, in the case of accounts on the Azure Databricks Premium Plan, to change permissions for the scope.
# MAGIC 
# MAGIC   > Your account must have the Azure Databricks Premium Plan for you to be able to select Creator. This is the recommended approach: grant MANAGE permission to the Creator when you create the secret scope, and then assign more granular access permissions after you have tested the scope.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Apply Changes
# MAGIC 
# MAGIC After a moment, you will see a dialog verifying that the secret scope has been created. Click "Ok" to close the box.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/db-secrets-confirm.png" />

# COMMAND ----------

# MAGIC %md
# MAGIC ### List Secret Scopes
# MAGIC 
# MAGIC To list the existing secret scopes the `dbutils.secrets` utility can be used.
# MAGIC 
# MAGIC You can list all scopes currently available in your workspace with:

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.secrets.listScopes()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### List Secrets within a specific scope
# MAGIC 
# MAGIC 
# MAGIC To list the secrets within a specific scope, you can supply that scope name.

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.secrets.list("students")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Using your Secrets
# MAGIC 
# MAGIC To use your secrets, you supply the scope and key to the `get` method.
# MAGIC 
# MAGIC Run the following cell to retrieve and print a secret.

# COMMAND ----------

# MAGIC %python
# MAGIC print(dbutils.secrets.get(scope="students", key="storageread"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Secrets are not displayed in clear text
# MAGIC 
# MAGIC Notice that the value when printed out is `[REDACTED]`. This is to prevent your secrets from being exposed.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Mount Azure Blob Container - Read/List
# MAGIC 
# MAGIC In this section, we'll demonstrating using a `SASTOKEN` that only has list and read permissions managed at the Storage Account level.
# MAGIC 
# MAGIC **This means:**
# MAGIC - Any user within the workspace can view and read the files mounted using this key
# MAGIC - This key can be used to mount any container within the storage account with these privileges

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Unmount directory if previously mounted.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC MOUNTPOINT = "/mnt/commonfiles"
# MAGIC 
# MAGIC if MOUNTPOINT in [mnt.mountPoint for mnt in dbutils.fs.mounts()]:
# MAGIC   dbutils.fs.unmount(MOUNTPOINT)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Add the Storage Account, Container, and reference the secret to pass the SAS Token
# MAGIC STORAGE_ACCOUNT = dbutils.secrets.get(scope="students", key="storageaccount")
# MAGIC CONTAINER = "commonfiles"
# MAGIC SASTOKEN = dbutils.secrets.get(scope="students", key="storageread")
# MAGIC 
# MAGIC # Do not change these values
# MAGIC SOURCE = "wasbs://{container}@{storage_acct}.blob.core.windows.net/".format(container=CONTAINER, storage_acct=STORAGE_ACCOUNT)
# MAGIC URI = "fs.azure.sas.{container}.{storage_acct}.blob.core.windows.net".format(container=CONTAINER, storage_acct=STORAGE_ACCOUNT)
# MAGIC 
# MAGIC try:
# MAGIC   dbutils.fs.mount(
# MAGIC     source=SOURCE,
# MAGIC     mount_point=MOUNTPOINT,
# MAGIC     extra_configs={URI:SASTOKEN})
# MAGIC except Exception as e:
# MAGIC   if "Directory already mounted" in str(e):
# MAGIC     pass # Ignore error if already mounted.
# MAGIC   else:
# MAGIC     raise e
# MAGIC print("Success.")

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.ls(MOUNTPOINT)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Define and display a Dataframe that reads a file from the mounted directory

# COMMAND ----------

salesDF = (spark.read
              .option("header", True)
              .option("inferSchema", True)
              .csv(MOUNTPOINT + "/sales.csv"))

display(salesDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Filter the Dataframe and display the results

# COMMAND ----------

from pyspark.sql.functions import col

sales2004DF = (salesDF
                  .filter((col("ShipDateKey") > 20031231) &
                          (col("ShipDateKey") <= 20041231)))
display(sales2004DF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Details....
# MAGIC 
# MAGIC 
# MAGIC While we can list and read files with this token, our job will abort when we try to write.

# COMMAND ----------

try:
  sales2004DF.write.mode("overwrite").parquet(MOUNTPOINT + "/sales2004")
except Exception as e:
  print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Review
# MAGIC 
# MAGIC At this point you should see how to:
# MAGIC * Use Secrets to access blobstorage
# MAGIC * Mount the blobstore to dbfs (Data Bricks File System)
# MAGIC 
# MAGIC Mounting data to dbfs makes that content available to anyone in that workspace. 
# MAGIC 
# MAGIC If you want to access blob store directly without mounting the rest of the notebook demonstrate that process.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing Directly to Blob using SAS token
# MAGIC 
# MAGIC Note that when you mount a directory, by default, all users within the workspace will have the same privileges to interact with that directory. Here, we'll look at using a SAS token to directly write to a blob (without mounting). This ensures that only users with the workspace that have access to the associated key vault will be able to write.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC CONTAINER = "commonfiles"
# MAGIC SASTOKEN = dbutils.secrets.get(scope="students", key="storagewrite")
# MAGIC 
# MAGIC # Redefine the source and URI for the new container
# MAGIC SOURCE = "wasbs://{container}@{storage_acct}.blob.core.windows.net/".format(container=CONTAINER, storage_acct=STORAGE_ACCOUNT)
# MAGIC URI = "fs.azure.sas.{container}.{storage_acct}.blob.core.windows.net".format(container=CONTAINER, storage_acct=STORAGE_ACCOUNT)
# MAGIC                
# MAGIC # Set up container SAS
# MAGIC spark.conf.set(URI, SASTOKEN)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Listing Directory Contents and writing using SAS token
# MAGIC 
# MAGIC Because the configured container SAS gives us full permissions, we can interact with the blob storage using our `dbutils.fs` methods.

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.ls(SOURCE)

# COMMAND ----------

# MAGIC %md
# MAGIC We can write to this blob directly, without exposing this mount to others in our workspace.

# COMMAND ----------

sales2004DF.write.mode("overwrite").parquet(SOURCE + "/sales2004")

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.ls(SOURCE)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Deleting using SAS token
# MAGIC 
# MAGIC This scope also has delete permissions.

# COMMAND ----------

# ALL_NOTEBOOK
dbutils.fs.rm(SOURCE + "/sales2004", True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Cleaning up mounts
# MAGIC 
# MAGIC If you don't explicitly unmount, the read-only blob that you mounted at the beginning of this notebook will remain accessible in your workspace.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC if MOUNTPOINT in [mnt.mountPoint for mnt in dbutils.fs.mounts()]:
# MAGIC   dbutils.fs.unmount(MOUNTPOINT)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Congratulations!
# MAGIC 
# MAGIC You should now be able to use the following tools in your workspace:
# MAGIC 
# MAGIC * Databricks Secrets
# MAGIC * Azure Key Vault
# MAGIC * SAS token
# MAGIC * dbutils.mount