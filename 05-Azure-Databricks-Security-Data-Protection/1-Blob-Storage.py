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

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) 1 - Blob Storage
# MAGIC 
# MAGIC This notebook is focused on configuring the blob storage required for the ADB Core partner training, but should provide general enough instructions to be useful in other settings.
# MAGIC  
# MAGIC  
# MAGIC  ### Learning Objectives
# MAGIC  By the end of this lesson, you should be able to:
# MAGIC  
# MAGIC  - Create blob storage containers
# MAGIC  - Load data into a container
# MAGIC  - Create a read/list SAS token
# MAGIC  - Create a SAS token with full privileges

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create container and upload a file
# MAGIC 
# MAGIC Follow the screenshots below to create a container and upload a file. 
# MAGIC 
# MAGIC You will be using the Azure Portal for these operations.
# MAGIC 
# MAGIC The Azure Portal can be accessed from your workspace by clicking on the **PORTAL** link, top right next to your name.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Access your Storage Account in the Azure Portal
# MAGIC 
# MAGIC 
# MAGIC A storage account has been provided for you, but you can follow instruction here to [create a new Storage Account in your Resource Group](https://docs.microsoft.com/en-us/azure/storage/common/storage-quickstart-create-account?tabs=azure-portal).
# MAGIC 
# MAGIC 1. Click on "All resources"
# MAGIC 2. Click on the storage account starting with `g1`
# MAGIC 
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-blob/resources.png" width=800px />

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## View the Blobs, if any stored with this storage account
# MAGIC 
# MAGIC A storage account can have multiple containers. 
# MAGIC 
# MAGIC We will upload our file as a blob into a Container for this storage account. 
# MAGIC 
# MAGIC First, see what containers -- if any -- exist in this storage account.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-blob/storage1.png" width=800px />

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Add a Container to hold Blobs
# MAGIC 
# MAGIC Currently, we have no containers defined in our blob. Click the indicated button to add a container.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-blob/blobs-empty.png" width=800px />

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Name the container
# MAGIC 
# MAGIC We'll name our first container `commonfiles`.
# MAGIC 
# MAGIC Note that the container name is hardcoded in the following notebooks, if you use a name besides `commonfiles` you will have to edit the following notebooks to reflect the name you chose in place of `commonfiles`
# MAGIC 
# MAGIC Click "OK" to continue.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-blob/new-blob.png" width=800px />

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Download a file to your computer
# MAGIC 
# MAGIC Now that we have created a container named `commonfiles`, let's upload a file to the container. 
# MAGIC 
# MAGIC Click the following link to download a csv to your computer:
# MAGIC ### Download [this file](https://files.training.databricks.com/courses/adbcore/commonfiles/sales.csv) to your local machine. 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Select the container and upload some data
# MAGIC 
# MAGIC We will upload the sales.csv file we just downloaded.
# MAGIC 
# MAGIC #### Select the container commonfiles
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-blob/blobs-1.png" width=800px />
# MAGIC 
# MAGIC #### Select Upload to prepare to upload Data
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-blob/blob-empty.png" width=800px />
# MAGIC 
# MAGIC #### Upload the file into the container
# MAGIC 
# MAGIC 1. Select the downloaded file "sales.csv" from the file picker.
# MAGIC 2. Click "Upload"
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-blob/file-upload.png"/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Congratulations! You have uploaded a file into Azure Blob Storage
# MAGIC 
# MAGIC Once you have content in Azure Blob Storage you can access that content in an Azure Databricks Notebook. 
# MAGIC 
# MAGIC For further reading you can see the [Documentation](https://docs.databricks.com/data/data-sources/azure/azure-storage.html)
# MAGIC 
# MAGIC One way to access the content in a container is to generate an SAS Token
# MAGIC 
# MAGIC **In the next steps you will generate an SAS Token for this container**

# COMMAND ----------

# MAGIC %md
# MAGIC # Copy Down Variables to Be Used in Next Lesson
# MAGIC 
# MAGIC You'll need a few values that we'll be loading into a Key Vault in the next lesson. We'll copy them into cells below.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Record Storage Account Name
# MAGIC 
# MAGIC Copy/paste the name of your storage account below.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-blob/account-name.png"/>

# COMMAND ----------

## YOUR STORAGE ACCOUNT NAME HERE ####
## NOTE WE DO NOT RUN ANY CODE HERE, THIS IS JUST SAVED FOR USE IN THE FOLLOWING NOTEBOOK

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Generate a SAS token with all privileges
# MAGIC 
# MAGIC In the Storage Container View in the Azure Portal
# MAGIC 
# MAGIC 1. Click "Shared access signature"
# MAGIC 2. Select all the permissions
# MAGIC 3. Click "Generate SAS and connection string" to generate the SAS Token.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-blob/sas-write.png"/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Retrieve the SAS Token generated
# MAGIC 
# MAGIC You will use the SAS token in a later notebook.
# MAGIC 
# MAGIC For now copy and paste the SAS token into the cell below. 
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-blob/sas-write-secrets.png" />

# COMMAND ----------

## YOUR FULL PERMISSIONS TOKEN HERE ####
## NOTE WE DO NOT RUN ANY CODE HERE, THIS IS JUST SAVED FOR USE IN THE FOLLOWING NOTEBOOK

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Generate a SAS token with only Read and List privileges
# MAGIC 
# MAGIC An SAS token is one way to gain access to data in your container. 
# MAGIC 
# MAGIC You will create an SAS token with read and list permissions for the container.
# MAGIC 
# MAGIC In the Storage Container View in the Azure Portal
# MAGIC 
# MAGIC 1. Click "Shared access signature"
# MAGIC 2. Deselect the appropriate permissions to create a "Read-Only" Token.
# MAGIC 3. Make sure you retained the list permission, the list permission is useful to view the contents of the container
# MAGIC 3. Click "Generate SAS and connection string" to generate the SAS Token.
# MAGIC 
# MAGIC #### **Warning a common mistake is to fail to select the list privilege, please verify you have selected read and list checkbox**
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-blob/sas-read.png" width=800px />

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Retrieve the SAS Token generated
# MAGIC 
# MAGIC You will use the SAS token in a later notebook.
# MAGIC 
# MAGIC For now copy and paste the SAS token into the cell below. 
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-blob/sas-write-secrets.png" />

# COMMAND ----------

## YOUR READ ONLY TOKEN HERE ####
## NOTE WE DO NOT RUN ANY CODE HERE, THIS IS JUST SAVED FOR USE IN THE FOLLOWING NOTEBOOK

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Obscuring your SAS Tokens
# MAGIC 
# MAGIC In this series of Notebooks we use our SAS Token to access the blob store. In the cells above you store the SAS token in plain text, this is **bad practice and only done for educational purposes**. Your SAS Token in a production environment should be stored in Secrets/KeyVault to prevent it from being displayed in plain text inside a notebook.
# MAGIC 
# MAGIC We will see how to do this in the next notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Congratulations!
# MAGIC 
# MAGIC You have:
# MAGIC  
# MAGIC  - Created a blob storage container
# MAGIC  - Loaded data into a container
# MAGIC  - Created a read/list SAS token
# MAGIC  - Created a SAS token with full permissions
# MAGIC  - Saved the SAS tokens for later use
# MAGIC  
# MAGIC In this notebook, we uploaded a file to a blob storage container and generated SAS tokens with different access permissions to the storage account. In the next notebook we will see how to store the SAS tokens securely in the Key Vault.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC 
# MAGIC Start the next lesson, [2-Key-Vault]($./2-Key-Vault)