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

up_val = [(1, "elon musk", "canada", "Calgary", "1990-06-01"),\
  (6, "fhh", "us", "chicago", "2005-11-01")]
up_cols = ("personId", "personName", "country", "region", "effectiveDate")
up_df = spark.createDataFrame(up_val,up_cols)
up_df.show()


# COMMAND ----------

from pyspark.sql.functions import lit,col
stagedPart1 = up_df.join(df, "personId")\
  .where( (df.isCurrent == True ) & (up_df.country != df.country) | (up_df.region != df.region))\
  .select(up_df['*']).distinct()
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

deltaTable = DeltaTable.forPath(spark, delta_path)
deltaTable.delete("effectiveDate == '1990-06-01'")

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

# MAGIC %md 
# MAGIC First see if there is already a table at the respective path, if theer is already a tbel then drop it to show the Delta lke default schem enfrcement feature
# MAGIC yu can drop teh table using  linux rm -rf command

# COMMAND ----------

dbutils.fs.rm("dbfs/FileStore/delta_table1/*",True) 

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf  /FileStore/delta_table1/_delta_log/

# COMMAND ----------

# MAGIC %fs ls /FileStore/delta_table1/

# COMMAND ----------

# MAGIC %fs ls /

# COMMAND ----------

from delta.tables import DeltaTable
columns = ["first_name", "age"]
data = [("bob", 47), ("li", 23), ("leonard", 51)]
# rdd = spark.sparkContext.parallelize(data)
# df = rdd.toDF(columns)
df = spark.createDataFrame(data,columns)
delta_table = DeltaTable.forPath(spark, "/FileStore/delta_table1")

# Drop the Delta Lake table
delta_table.delete()
df.write.format("delta").mode("overwrite").save("/FileStore/delta_table1")

# COMMAND ----------



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

# COMMAND ----------

data = [{"emp_id":1,"first_name":"Melissa","last_name":"Parks","Address":"19892 Williamson Causeway Suite 737\nKarenborough, IN 11372","phone_number":"001-372-612-0684","isContractor":"false"},
{"emp_id":2,"first_name":"Laura","last_name":"Delgado","Address":"93922 Rachel Parkways Suite 717\nKaylaville, GA 87563","phone_number":"001-759-461-3454x80784","isContractor":"false"},
{"emp_id":3,"first_name":"Luis","last_name":"Barnes","Address":"32386 Rojas Springs\nDicksonchester, DE 05474","phone_number":"127-420-4928","isContractor":"false"},
{"emp_id":4,"first_name":"Jonathan","last_name":"Wilson","Address":"682 Pace Springs Apt. 011\nNew Wendy, GA 34212","phone_number":"761.925.0827","isContractor":"true"},
{"emp_id":5,"first_name":"Kelly","last_name":"Gomez","Address":"4780 Johnson Tunnel\nMichaelland, WI 22423","phone_number":"+1-303-418-4571","isContractor":"false"},
{"emp_id":6,"first_name":"Robert","last_name":"Smith","Address":"04171 Mitchell Springs Suite 748\nNorth Juliaview, CT 87333","phone_number":"261-155-3071x3915","isContractor":"true"},
{"emp_id":7,"first_name":"Glenn","last_name":"Martinez","Address":"4913 Robert Views\nWest Lisa, ND 75950","phone_number":"001-638-239-7320x4801","isContractor":"false"},
{"emp_id":8,"first_name":"Teresa","last_name":"Estrada","Address":"339 Scott Valley\nGonzalesfort, PA 18212","phone_number":"435-600-3162","isContractor":"false"},
{"emp_id":9,"first_name":"Karen","last_name":"Spencer","Address":"7284 Coleman Club Apt. 813\nAndersonville, AS 86504","phone_number":"484-909-3127","isContractor":"true"},
{"emp_id":10,"first_name":"Daniel","last_name":"Foley","Address":"621 Sarah Lock Apt. 537\nJessicaton, NH 95446","phone_number":"457-716-2354x4945","isContractor":"true"},
{"emp_id":11,"first_name":"Amy","last_name":"Stevens","Address":"94661 Young Lodge Suite 189\nCynthiamouth, PR 01996","phone_number":"241.375.7901x6915","isContractor":"true"},
{"emp_id":12,"first_name":"Nicholas","last_name":"Aguirre","Address":"7474 Joyce Meadows\nLake Billy, WA 40750","phone_number":"495.259.9738","isContractor":"true"},
{"emp_id":13,"first_name":"John","last_name":"Valdez","Address":"686 Brian Forges Suite 229\nSullivanbury, MN 25872","phone_number":"+1-488-011-0464x95255","isContractor":"false"},
{"emp_id":14,"first_name":"Michael","last_name":"West","Address":"293 Jones Squares Apt. 997\nNorth Amandabury, TN 03955","phone_number":"146.133.9890","isContractor":"true"},
{"emp_id":15,"first_name":"Perry","last_name":"Mcguire","Address":"2126 Joshua Forks Apt. 050\nPort Angela, MD 25551","phone_number":"001-862-800-3814","isContractor":"true"},
{"emp_id":16,"first_name":"James","last_name":"Munoz","Address":"74019 Banks Estates\nEast Nicolefort, GU 45886","phone_number":"6532485982","isContractor":"false"},
{"emp_id":17,"first_name":"Todd","last_name":"Barton","Address":"2795 Kelly Shoal Apt. 500\nWest Lindsaytown, TN 55404","phone_number":"079-583-6386","isContractor":"true"},
{"emp_id":18,"first_name":"Christopher","last_name":"Noble","Address":"Unit 7816 Box 9004\nDPO AE 29282","phone_number":"215-060-7721","isContractor":"true"},
{"emp_id":19,"first_name":"Sandy","last_name":"Hunter","Address":"7251 Sarah Creek\nWest Jasmine, CO 54252","phone_number":"8759007374","isContractor":"false"},
{"emp_id":20,"first_name":"Jennifer","last_name":"Ballard","Address":"77628 Owens Key Apt. 659\nPort Victorstad, IN 02469","phone_number":"+1-137-420-7831x43286","isContractor":"true"},
{"emp_id":21,"first_name":"David","last_name":"Morris","Address":"192 Leslie Groves Apt. 930\nWest Dylan, NY 04000","phone_number":"990.804.0382x305","isContractor":"false"},
{"emp_id":22,"first_name":"Paula","last_name":"Jones","Address":"045 Johnson Viaduct Apt. 732\nNorrisstad, AL 12416","phone_number":"+1-193-919-7527x2207","isContractor":"true"},
{"emp_id":23,"first_name":"Lisa","last_name":"Thompson","Address":"1295 Judy Ports Suite 049\nHowardstad, PA 11905","phone_number":"(623)577-5982x33215","isContractor":"true"},
{"emp_id":24,"first_name":"Vickie","last_name":"Johnson","Address":"5247 Jennifer Run Suite 297\nGlenberg, NC 88615","phone_number":"708-367-4447x9366","isContractor":"false"},
{"emp_id":25,"first_name":"John","last_name":"Hamilton","Address":"5899 Barnes Plain\nHarrisville, NC 43970","phone_number":"341-467-5286x20961","isContractor":"false"}]
df = spark.createDataFrame(data)

# COMMAND ----------

display(df)

# COMMAND ----------


from pyspark.sql.functions import sha2,concat_ws,lit,col,when
df = df.withColumn("emp_key", sha2(concat_ws("||", col("emp_id"), col("first_name"), col("last_name"), col("Address"),
            col("phone_number"), col("isContractor")), 256))
display(df)

# COMMAND ----------

df = df.withColumn("isCurrent", lit('true')).withColumn("end_date",lit("Null")).withColumn("delete_flag", lit("false"))
display(df)

# COMMAND ----------

df = df.withColumn("isContractor", when( df["emp_id"] == lit(12), lit("false")).otherwise(df["isContractor"])    )
display(df)
