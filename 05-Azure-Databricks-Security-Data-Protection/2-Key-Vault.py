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
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) 2 - Key Vault
# MAGIC 
# MAGIC [Azure Key Vault](https://docs.microsoft.com/en-us/azure/key-vault/key-vault-whatis) provides us with a number of options for storing and sharing secrets and keys between Azure applications, and has direct integration with Azure Databricks. In this notebook, we'll focus on configuring an access policy and creating Secrets. These instructions are based around configurations and settings for the ADB Core partner training, but should be adaptable to production requirements.
# MAGIC 
# MAGIC **This is something that will generally be handled by the workspace adminstrator.** Only individuals with proper permissions in the Azure Active Directory will be able to link a Key Vault to the Databricks workspace. (Each Key Vault will map to a "scope" in Databricks, so enterprise solutions may have many different Key Vaults for different teams/personas who need different permissions.)
# MAGIC 
# MAGIC ### Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC - Configure Key Vault Access Policies
# MAGIC - Create Secrets that store SAS Tokens in a Key Vault

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> **PLEASE** open a new browser tab and navigate to <https://portal.azure.com>.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure Key Vault Access Policies
# MAGIC 
# MAGIC 1. Go to "All resources"
# MAGIC 2. Click on the Key Vault resource
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/resources-kv.png" width=800px />

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Navigate to Access Policies
# MAGIC 
# MAGIC First, click on `Access policies` in the left-side plane.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/keyvault-home.png" width=800px />

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Access Policy to Key Vault
# MAGIC 
# MAGIC While our user is a "Contributor" on this resource, we must add an access policy to add/list/use secrets.
# MAGIC 
# MAGIC Click "Add access policy"
# MAGIC 
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/access-none.png" width=800px />

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Select "Key, Secret, & Certificate Mangement" from the dropdown
# MAGIC 2. Click to select a principal
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/access-template.png" />

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Search for your user ID
# MAGIC 2. Click on the matching result to select
# MAGIC 3. Click "Select"
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/access-principal.png" />

# COMMAND ----------

# MAGIC %md
# MAGIC Now you'll need to click "Add" and then...
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/access-not-added.png" />

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Save Configuration Changes
# MAGIC 
# MAGIC ... you'll click "Save" to finalize the configurations.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/access-not-saved.png" />

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Congratulations!
# MAGIC 
# MAGIC **At this point you have**
# MAGIC * Modified Access Policies in the Azure Key Vault
# MAGIC 
# MAGIC ### Next Steps
# MAGIC 
# MAGIC **Your next steps are to:** 
# MAGIC * Create Secrets in the Key Vault

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create secrets in Key Vault
# MAGIC 
# MAGIC To create secrets in Key Vault that can be accessed from your new secret scope in Databricks, you need to either use the Azure portal or the Key Vault CLI. For simplicity's sake, we will use the Azure portal:
# MAGIC 
# MAGIC 1. Select **Secrets** in the left-hand menu.
# MAGIC 2. Select **+ Generate/Import** in the Secrets toolbar.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/secrets-none.png" width=800px />

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create a storageread Secret
# MAGIC 
# MAGIC In the next blade:
# MAGIC 
# MAGIC * Enter the name of the secret
# MAGIC   * For the `Name` field, enter **storageread**
# MAGIC   * This will be the key to access the secret value; this will be visible in plain text
# MAGIC * Paste/enter the value for the secret 
# MAGIC    * For the `Value` field, enter the **read-only SAS token** from the previous notebook.
# MAGIC    * This will be the value that is stored as a secret; this will be `[REDACTED]`.
# MAGIC * Click "Create"
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/storageread.png" />

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create a storagewrite Secret
# MAGIC 
# MAGIC You should see one secret now in your vault.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/secrets-1.png" width=800px />
# MAGIC 
# MAGIC You want to "Generate/Import" another secret.
# MAGIC 
# MAGIC * Enter the name of the secret
# MAGIC   * For the `Name` field, enter **storagewrite**
# MAGIC   * This will be the key to access the secret value; this will be visible in plain text
# MAGIC * Paste/enter the value for the secret 
# MAGIC    * For the `Value` field, enter the **full permissions SAS token** from the previous notebook.
# MAGIC    * This will be the value that is stored as a secret; this will be `[REDACTED]`.
# MAGIC * Click "Create"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create a storageaccount Secret
# MAGIC 
# MAGIC Finally, you'll create one more secret.
# MAGIC 
# MAGIC 1. Name: `storageaccount`
# MAGIC 2. Value: copy/paste the name of your storage account
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-blob/account-name.png"/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Return to the list view in the Azure Portal
# MAGIC 
# MAGIC When you're done, you should see the following keys:
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/config-keyvault/secrets-all.png" width=800px/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Congratulations!
# MAGIC 
# MAGIC You have:
# MAGIC * Modified Access Policies in the Azure Key Vault
# MAGIC * Create Secrets in the Key Vault that use SAS tokens
# MAGIC 
# MAGIC In this notebook, we stored the SAS tokens from the first notebook as Secrets in the Key Vault. In the next notebook, we will see how to connect Databricks to the Key Vault and access the SAS tokens to read and write from Blob Storage.