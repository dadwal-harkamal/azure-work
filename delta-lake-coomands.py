# Databricks notebook source
# Load a file into a dataframe
df = spark.read.load('/data/mydata.csv', format='csv', header=True)

# Save the dataframe as a delta table
delta_table_path = "/delta/mydata"
df.write.format("delta").save(delta_table_path)

# COMMAND ----------

new_df.write.format("delta").mode("overwrite").save(delta_table_path)

# COMMAND ----------

new_rows_df.write.format("delta").mode("append").save(delta_table_path)

# COMMAND ----------

# MAGIC %md
# MAGIC # Make conditional updates
# MAGIC

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

# Create a deltaTable object
deltaTable = DeltaTable.forPath(spark, delta_table_path)

# Update the table (reduce price of accessories by 10%)
deltaTable.update(
    condition = "Category == 'Accessories'",
    set = { "Price": "Price * 0.9" })

# COMMAND ----------

# MAGIC %md
# MAGIC # Querying a previous version of a table:
# MAGIC

# COMMAND ----------

df = spark.read.format("delta").option("versionAsOf", 0).load(delta_table_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Alternatively, you can specify a timestamp by using the timestampAsOf option:

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf", '2022-01-01').load(delta_table_path)

# COMMAND ----------

bikes_df = df.select("ProductName", "ListPrice").where((df["Category"]=="Mountain Bikes") | (df["Category"]=="Road Bikes"))
display(bikes_df)

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

counts_df = df.select("ProductID", "Category").groupBy("Category").count()
display(counts_df)

# COMMAND ----------

# MAGIC %md output of aboce comman is :
# MAGIC The results from this code example would look something like this:
# MAGIC
# MAGIC Category	    count
# MAGIC Headsets	      3
# MAGIC Wheels	        14
# MAGIC Mountain Bikes  32
# MAGIC ...	...

# COMMAND ----------

# MAGIC %md
# MAGIC Selecting a subset of columns from a dataframe is a common operation, which can also be achieved by using the following shorter syntax:
# MAGIC
# MAGIC pricelist_df = df["ProductID", "ListPrice"]

# COMMAND ----------

# MAGIC %md
# MAGIC #Making conditional updates
# MAGIC While you can make data modifications in a dataframe and then replace a Delta Lake table by overwriting it, a more common pattern in a database is to insert, update or delete rows in an existing table as discrete transactional operations. To make such modifications to a Delta Lake table, you can use the DeltaTable object in the Delta Lake API, which supports update, delete, and merge operations. For example, you could use the following code to update the price column for all rows with a category column value of "Accessories":

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

# Create a deltaTable object
deltaTable = DeltaTable.forPath(spark, delta_table_path)

# Update the table (reduce price of accessories by 10%)
deltaTable.update(
    condition = "Category == 'Accessories'",
    set = { "Price": "Price * 0.9" })
