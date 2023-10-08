# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.hpldevarmdlsuw02.dfs.core.windows.net","oW1G5qzJDuQdK1dlRDunh2UbJXIja+tQC1d6FpAwJVaTYIgahR2V4v+1KtOzjmnFEpbh9TpsiJBx+AStHqdW0w=="
    )

# COMMAND ----------

emp_path = "abfss://bronze@hpldevarmdlsuw02.dfs.core.windows.net/employeeData.csv"
scd_gpt_del_path = "abfss://bronze@hpldevarmdlsuw02.dfs.core.windows.net/scd-gpt"
employee_df = spark.read.csv(emp_path,header=True,inferSchema=True)
employee_df= employee_df.withColumn("StartDate", employee_df["StartDate"].cast("date")).withColumn("EndDate", employee_df["EndDate"].cast("date"))
# employee_df.write.format("delta").mode("overwrite").save(scd_gpt_del_path)
display(employee_df)


# COMMAND ----------

emp_mod_path = "abfss://bronze@hpldevarmdlsuw02.dfs.core.windows.net/employeeData_Modified.csv"
employee_mod_df = spark.read.csv(emp_mod_path,header=True,inferSchema=True)
employee_mod_df = employee_mod_df.distinct()
display(employee_mod_df)

# COMMAND ----------

update = employee_df.join(employee_mod_df,'EmployeeID').where( employee_mod_df['EndDate'].isNotNull()  ).select(employee_mod_df['*'])
update = update.withColumn("mergeKey",col('EmployeeID')).withColumn("StartDate", update["StartDate"].cast("date")).withColumn("EndDate", update["EndDate"].cast("date"))
display(update)

# COMMAND ----------

# insert = employee_df.join(employee_mod_df,'EmployeeID','outer').where( (employee_df['EmployeeID'].isNull() ) & (employee_mod_df['IsCurrent']) == 1  ).select(employee_mod_df['*'])
insert = employee_df.join(employee_mod_df,'EmployeeID','outer').where( employee_mod_df['IsCurrent'] == 1  ).select(employee_mod_df['*'])
insert = insert.withColumn("mergeKey",lit("null")).withColumn("StartDate", insert["StartDate"].cast("date")).withColumn("EndDate", insert["EndDate"].cast("date"))
display(insert)

# COMMAND ----------

staged_df = insert.union(update)
display(staged_df)

# COMMAND ----------

from delta.tables import *
# delta_path = 
df_del = DeltaTable.forPath(spark,scd_gpt_del_path)
df_del.alias("emp")\
  .merge(staged_df.alias("staged_updates"), "emp.EmployeeID = staged_updates.mergeKey")\
  .whenMatchedUpdate(condition = "staged_updates.isCurrent == false AND staged_updates.EndDate > emp.StartDate ", set = {
      "isCurrent" : "false",
      "endDate" : "staged_updates.EndDate"      
  })\
  .whenNotMatchedInsert(condition = "staged_updates.isCurrent == true", values = {
    "EmployeeID" : "staged_updates.EmployeeID",
    "EmployeeName" : "staged_updates.EmployeeName",
    "Email" : "staged_updates.Email",
    "StartDate" : "staged_updates.StartDate",
    "EndDate" : "staged_updates.EndDate",
    "IsCurrent" :"staged_updates.IsCurrent"    
  }).execute()

# COMMAND ----------

display(spark.read.format("delta").load(scd_gpt_del_path))

# COMMAND ----------

final_df = spark.read.format("delta").load(scd_gpt_del_path).select('*').distinct()
final_df.write.format("delta").mode("overwrite").save(scd_gpt_del_path)
display(spark.read.format("delta").load(scd_gpt_del_path))

# COMMAND ----------

combined_data = employee_df.union(employee_mod_df)

combined_data = combined_data.withColumn("StartDate", combined_data["StartDate"].cast("date"))
combined_data = combined_data.withColumn("EndDate", combined_data["EndDate"].cast("date"))
display(combined_data)

# COMMAND ----------



# COMMAND ----------



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
  .whenMatchedUpdate(condition = "tech_celebs.isCurrent == true  AND (staged_updates.country  <> tech_celebs.country OR staged_updates.region != tech_celebs.region)", set = {
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

# COMMAND ----------

scd_ii_df_uni = spark.read.format("delta").load(delta_path).distinct().orderBy('personId')
scd_ii_df_uni.write.format("delta").mode("overwrite").save(delta_path)

# display(scd_ii_df_uni)

# COMMAND ----------


from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
enf_schema = StructType([
    StructField("name",StringType(),True),
    StructField("age", IntegerType(), True)
                ])

data = [("harkamal",50),("seema",46)]
df = spark.createDataFrame(data,schema=enf_schema)
display(df)

# COMMAND ----------

def enforce_schema(data):
    nul_sch = StructType(
    [
        StructField("name", StringType(),True),
        StructField("age", IntegerType(),False)
    ]
)
    # data = [("harkamal",50),("seema","50")]
    df_no = spark.createDataFrame(data,schema=nul_sch)
    display(df_no)

# COMMAND ----------

correct_data = [("harkamal",50),("seema",50)]
err_data = [("harkamal",50),("seema","50")]
null_field = [("harkamal",50),("seema",None)]


# COMMAND ----------

enforce_schema(correct_data)

# COMMAND ----------

enforce_schema(err_data)

# COMMAND ----------

enforce_schema(null_field)

# COMMAND ----------



# COMMAND ----------

columns = ["first_name", "age"]
data = [("bob", 47), ("li", 23), ("leonard", 51)]
# rdd = spark.sparkContext.parallelize(data)
# df = rdd.toDF(columns)
df = spark.createDataFrame(data,columns)
df.write.format("delta").save("/FileStore/delta_table1")

# COMMAND ----------

columns = ["first_name", "favorite_color"]
data = [("sal", "red"), ("cat", "pink")]
# rdd = spark.sparkContext.parallelize(data)
# df = rdd.toDF(columns)
df1 = spark.createDataFrame(data,columns)
df1.write.mode("append").format("delta").save("/FileStore/delta_table1")

# COMMAND ----------

## below code will take care of schema evolution
df1.write.option("mergeSchema", "true").mode("append").format("delta").save(
    "/FileStore/delta_table1"
)

# COMMAND ----------

mod_df = spark.read.format("delta").load("/FileStore/delta_table1")
display(mod_df)

# COMMAND ----------

# MAGIC %fs ls /FileStore/

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets/samples'))
