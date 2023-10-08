# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws,year,col

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
df = df.withColumn("Year", year(col("Date")))
# Show the DataFrame
df.show()
