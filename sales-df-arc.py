# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws,year,col,month,to_date,unix_timestamp,from_unixtime,lit

# Create a SparkSession
spark = SparkSession.builder.appName("sales_example").getOrCreate()

# Sample sales data with salespersons' names
data = [
    (1, "Product A", "2023-01-01", 100, 10, "John", "Doe", "Smith"),
    (2, "Product B", "2023-01-01", 150, 15, "Jane", "Smith", "Johnson"),
    (3, "Product A", "2023-01-02", 120, 12, "Michael", "Johnson", "Brown"),
    (4, "Product C", "2023-01-02", 200, 20, "Emily", "Davis", "Wilson"),
    (5, "Product B", "2023-01-03", 180, 18, "David", "Wilson", "Anderson"),
    (6, "Product A", "2023-01-03", 90, 9, "Maria", "Anderson", "Lee"),
    (7, "Product C", "2023-01-04", 210, 21, "Robert", "Lee", "Garcia"),
    (8, "Product B", "2023-01-04", 160, 16, "Patricia", "Garcia", "Martinez"),
    (9, "Product A", "2023-01-05", 110, 11, "William", "Martinez", "Miller"),
    (10, "Product C", "2023-01-05", 220, 22, "Linda", "Miller", "Moore")
]

# Create a DataFrame
df = spark.createDataFrame(data, ["OrderID", "Product", "Date", "Price", "Quantity", "FirstName", "MiddleName", "LastName"])
df = df.withColumn("Year", year(col("Date"))).withColumn("month",month(col("date")))

# Show the DataFrame

df.show()

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.hpldevarmdlsuw02.dfs.core.windows.net","oW1G5qzJDuQdK1dlRDunh2UbJXIja+tQC1d6FpAwJVaTYIgahR2V4v+1KtOzjmnFEpbh9TpsiJBx+AStHqdW0w=="
    )

# COMMAND ----------

sales_path = "abfss://bronze@hpldevarmdlsuw02.dfs.core.windows.net/sales"
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
sales_df = spark.read.csv(sales_path,inferSchema=True,header=True)
# sales_df = sales_df.withColumn("ORDERDATE",to_date(unix_timestamp(col("ORDERDATE"), "MM/dd/yyyy H:mm").cast("timestamp")))
sales_df = sales_df.withColumn("ORDERDATE",to_date(col("ORDERDATE"), "MM/dd/yyyy H:mm"))
sales_df = sales_df.withColumn("MONTH", month(col("ORDERDATE")))
sales_df = sales_df.groupby("YEAR_ID","MONTH").sum("SALES").withColumnRenamed("sum(SALES)","Total_Sales")
sales_df
# sales_df = sales_df.withColumn("YEAR",year(col("ORDERDATE")))
# print(sales_df.count())
display(sales_df)

# COMMAND ----------

df = sales_df.groupby()

# COMMAND ----------


