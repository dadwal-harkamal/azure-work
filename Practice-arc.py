# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.hpldevarmdlsuw02.dfs.core.windows.net","oW1G5qzJDuQdK1dlRDunh2UbJXIja+tQC1d6FpAwJVaTYIgahR2V4v+1KtOzjmnFEpbh9TpsiJBx+AStHqdW0w=="
    )

# COMMAND ----------

emp_path = "abfss://bronze@hpldevarmdlsuw02.dfs.core.windows.net/employeeData.csv"
employee_df = spark.read.csv(emp_path,header=True,inferSchema=True)
display(employee_df)


# COMMAND ----------

emp_mod_path = "abfss://bronze@hpldevarmdlsuw02.dfs.core.windows.net/employeeData_Modified.csv"
employee_mod_df = spark.read.csv(emp_mod_path,header=True,inferSchema=True)
display(employee_mod_df)

# COMMAND ----------

combined_data = employee_df.union(employee_mod_df)

combined_data = combined_data.withColumn("StartDate", combined_data["StartDate"].cast("date"))
combined_data = combined_data.withColumn("EndDate", combined_data["EndDate"].cast("date"))
display(combined_data)

# COMMAND ----------


from pyspark.sql.functions import lit,col
from delta.tables import DeltaTable
dimension_table_path = "abfss://bronze@hpldevarmdlsuw02.dfs.core.windows.net/scd-gpt"
delta_table = DeltaTable.forPath(dimension_table_path)

# Merge data into the dimension table using Delta Lake's MERGE statement
delta_table.alias("dim").merge(
    combined_data.alias("src"),
    "dim.EmployeeID = src.EmployeeID AND dim.EndDate IS NULL",
).whenMatchedUpdate(
    condition=combined_data["IsCurrent"] == lit("false"),
    set={
        "dim.EndDate": combined_data["StartDate"] - lit(1),
        "dim.IsCurrent": False,
    }
).whenNotMatchedInsert(
    values={
        "EmployeeID": combined_data["EmployeeID"],
        "EmployeeName": combined_data["EmployeeName"],
        "Email": combined_data["Email"],
        "StartDate": combined_data["StartDate"],
        "EndDate": None,
        "IsCurrent": combined_data["IsCurrent"],
        "Source": combined_data["Source"],
    }
).execute()

# Display the dimension table
delta_table.toDF().show()

# Stop the Spark session





# COMMAND ----------

# val= [(1, "elon musk", "south africa", "pretoria", "true", "1971-06-28", "null"),\
#   (2, "jeff bezos", "us", "albuquerque", "true", "1964-01-12", "null"),\
#   (3, "bill gates", "us", "seattle", "false", "1955-10-28", "1973-09-01")]
# cols = ["personId", "personName", "country", "region", "isCurrent", "effectiveDate", "endDate"]
# df = spark.createDataFrame(val,cols)
delta_path = "abfss://bronze@hpldevarmdlsuw02.dfs.core.windows.net/scd-two"
# val path = os.pwd/"tmp"/"tech_celebs"
# df.write.format("delta").mode("overwrite").save(delta_path)
df = spark.read.format("delta").load(delta_path)
df.show()

#   df_initial = spark.createDataFrame([['0001', 'Raymond'], ['0002', 'Kontext'], ['0003', 'John']
                                        # ], ['customer_no', 'name'])

# COMMAND ----------

up_val = [(1, "elon musk", "canada", "montreal", "1989-06-01"),\
  (4, "dhh", "us", "chicago", "2005-11-01")]
up_cols = ("personId", "personName", "country", "region", "effectiveDate")
up_df = spark.createDataFrame(up_val,up_cols)
up_df.show()


# COMMAND ----------

from pyspark.sql.functions import lit,col
stagedPart1 = up_df.join(df, "personId")\
  .where( (df.isCurrent == True ) & (up_df.country != df.country) | (up_df.region != df.region))\
  .select(up_df['*'])
stagedPart1 = stagedPart1.withColumn('mergeKey',lit('null'))
stagedPart1.show()

# COMMAND ----------

stagedPart2 = up_df.withColumn('mergeKey',col('personId'))
stagedPart2.show()

# COMMAND ----------

stagedUpdates = stagedPart1.union(stagedPart2)
stagedUpdates.show()

# COMMAND ----------

from delta.tables import *
# delta_path = 
df_del = DeltaTable.forPath(spark,delta_path)
df_del.alias("tech_celebs")\
  .merge(stagedUpdates.alias("staged_updates"), "tech_celebs.personId = mergeKey")\
  .whenMatchedUpdate(condition = "tech_celebs.isCurrent == true  AND staged_updates.country  <> tech_celebs.country OR staged_updates.region != tech_celebs.region", set = {
      "isCurrent" : "false",
      "endDate" : "staged_updates.effectiveDate"
      
  })\
  .whenNotMatchedInsert(values = {
    "personId" : "staged_updates.personId",
    "personName" : "staged_updates.personName",
    "country" : "staged_updates.country",
    "region" : "staged_updates.region",
    "isCurrent" : "true",
    "effectiveDate" :"staged_updates.effectiveDate",
    "endDate" : "null"
  }).execute()

# COMMAND ----------

scd_ii_df = spark.read.format("delta").load(delta_path).orderBy('personId')
display(scd_ii_df)
