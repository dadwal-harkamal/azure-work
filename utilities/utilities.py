# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("hpl-kv-scope")

# COMMAND ----------



# COMMAND ----------

def accessAzureAdlsGen2Storage():
    '''
     pre-requisite: Azure Service Principal with proper access to the respective storage account.
     Before using this function please create the below secrtes in the azure key vault.
     You also need to create the secret scope in your databricks workspace, need help pl. refer to below URL to create secret scopes in databricks.
     https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes
    '''        
    service_credential = dbutils.secrets.get(scope="hpl-kv-scope",key="sp-hpl-secret")
    service_client_id = dbutils.secrets.get(scope="hpl-kv-scope",key="sp-hpl-client-id")
    tenant_directory_id = dbutils.secrets.get(scope="hpl-kv-scope",key="tenant-directory-id")
    storage_account = dbutils.secrets.get(scope="hpl-kv-scope",key="storage-account")
    
    spark.conf.set("fs.azure.account.auth.type." + storage_account + ".dfs.core.windows.net", "OAuth")
    spark.conf.set("fs.azure.account.oauth.provider.type." + storage_account +".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set("fs.azure.account.oauth2.client.id." + storage_account + ".dfs.core.windows.net", service_client_id)
    spark.conf.set("fs.azure.account.oauth2.client.secret." + storage_account +".dfs.core.windows.net", service_credential)
    spark.conf.set("fs.azure.account.oauth2.client.endpoint." + storage_account + ".dfs.core.windows.net", "https://login.microsoftonline.com/" + tenant_directory_id + "/oauth2/token") 


# COMMAND ----------

accessAzureAdlsGen2Storage

# COMMAND ----------

order_path = "abfss://bronze@hpldevarmdlsuw02.dfs.core.windows.net/orders.csv"
orders_df = spark.read.csv(order_path,header=True,inferSchema=True)
display(orders_df)

