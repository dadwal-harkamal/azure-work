# Databricks notebook source
from pyspark.sql.functions import udf 
from pyspark.sql.types import StructType,StructField,IntegerType
# Alternatively, you can declare the same UDF using annotation syntax:
@udf("long")
def squared_udf(s):
  return s * s

val = [(10000000,"ist"),(2,"2nd"),(3,"3rd")]
# sch = StructType([StructField("id",IntegerType(),True)])
cols = ["id","name"]
# del df
df_1 = spark.createDataFrame(val,cols)
display(df_1.select("id", squared_udf("id").alias("id_squared")))

def cubed_udf(s):
    return s *s *s
cubed_function = udf(cubed_udf,BIGINT)
display(df_1.select("id", cubed_function("id").alias("id_squared")))


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Initialize a Spark session
spark = SparkSession.builder.appName("UDFExample").getOrCreate()

# Sample data
data = [("John", 15000), ("Alice", 5000), ("Bob", 25000)]
schema = ["Name", "Salary"]

# Create a DataFrame
df = spark.createDataFrame(data, schema=schema)

# Define a UDF to categorize values
def categorize_salary(salary):
    if salary > 20000:
        return "Large"
    elif salary > 10000:
        return "Medium"
    else:
        return "Low"

# Register the UDF
categorize_udf = udf(categorize_salary, StringType())

# Apply the UDF to the DataFrame to create a new column
df = df.withColumn("SalaryCategory", categorize_udf(df["Salary"]))

# Display the DataFrame
df.show()
