# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("hpl-kv-scope")

# COMMAND ----------



# COMMAND ----------

service_credential = dbutils.secrets.get(scope="hpl-kv-scope",key="sp-hpl-secret")
spark.conf.set("fs.azure.account.auth.type.hpldevarmdlsuw02.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.hpldevarmdlsuw02.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.hpldevarmdlsuw02.dfs.core.windows.net", "ec22a7b9-8444-44aa-89e6-6a8516b31a4e")
spark.conf.set("fs.azure.account.oauth2.client.secret.hpldevarmdlsuw02.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.hpldevarmdlsuw02.dfs.core.windows.net", "https://login.microsoftonline.com/b1aa5d7c-c737-43a3-a8b2-6f42d289b689/oauth2/token") # tenant id


# COMMAND ----------

order_path = "abfss://bronze@hpldevarmdlsuw02.dfs.core.windows.net/orders.csv"
orders_df = spark.read.csv(order_path,header=True,inferSchema=True)
display(orders_df)

