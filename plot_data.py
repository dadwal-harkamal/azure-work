# Databricks notebook source
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import month, year, sum
import pandas as pd

# Create a SparkSession
spark = SparkSession.builder.appName("sales_example").getOrCreate()

# Create a sample DataFrame with sales data
data = [
    ("2023-01-01", 100),
    ("2023-02-01", 150),
    ("2023-03-01", 120),
    ("2023-04-01", 200),
    ("2023-05-01", 180),
    ("2023-06-01", 90),
    ("2023-07-01", 210),
    ("2023-08-01", 160),
    ("2023-09-01", 110),
    ("2023-10-01", 220),
]

schema = ["Date", "SalesAmount"]
sales_data = spark.createDataFrame(data, schema)

# Extract year and month from the "Date" column
sales_data = sales_data.withColumn("Year", year("Date"))
sales_data = sales_data.withColumn("Month", month("Date"))

# Group the data by year and month and calculate the total sales for each month
monthly_sales = sales_data.groupBy("Year", "Month").agg(sum("SalesAmount").alias("TotalSales"))

# Convert the result to a Pandas DataFrame for plotting
monthly_sales_pd = monthly_sales.toPandas()

# Plot the total sales
plt.figure(figsize=(12, 6))
plt.plot(monthly_sales_pd["Month"], monthly_sales_pd["TotalSales"])
plt.xlabel("Month")
plt.ylabel("Total Sales")
plt.title("Total Sales by Month")
plt.xticks(range(1, 13), ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])
plt.grid(True)

# Display the plot
plt.show()



# COMMAND ----------

display(monthly_sales)

# COMMAND ----------


