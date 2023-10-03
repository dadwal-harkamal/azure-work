# Databricks notebook source
# MAGIC %md
# MAGIC #Stream-Stream Joins using Structured Streaming (Python)
# MAGIC This notebook illustrates different ways of joining streams.
# MAGIC
# MAGIC We are going to use the the canonical example of ad monetization, where we want to find out which ad impressions led to user clicks. Typically, in such scenarios, there are two streams of data from different sources - ad impressions and ad clicks. Both type of events have a common ad identifier (say, adId), and we want to match clicks with impressions based on the adId. In addition, each event also has a timestamp, which we will use to specify additional conditions in the query to limit the streaming state.

# COMMAND ----------

df = spark.readStream.format("rate").option("rowsPerSecond", "5").option("numPartitions", "1").load().where((rand() * 100).cast("integer") < 10) 
display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC In absence of actual data streams, we are going to generate fake data streams using our built-in "rate stream", that generates data at a given fixed rate.

# COMMAND ----------

from pyspark.sql.functions import rand

spark.conf.set("spark.sql.shuffle.partitions", "1")

impressions = (
  spark
    .readStream.format("rate").option("rowsPerSecond", "5").option("numPartitions", "1").load()
    .selectExpr("value AS adId", "timestamp AS impressionTime")
)

clicks = (
  spark
  .readStream.format("rate").option("rowsPerSecond", "5").option("numPartitions", "1").load()
  .where((rand() * 100).cast("integer") < 10)      # 10 out of every 100 impressions result in a click
  .selectExpr("(value - 50) AS adId ", "timestamp AS clickTime")      # -50 so that a click with same id as impression is generated later (i.e. delayed data).
  .where("adId > 0")
)    
display(clicks)

# COMMAND ----------

display(impressions)

# COMMAND ----------

display(impressions.join(clicks, "adId"))

# COMMAND ----------

from pyspark.sql.functions import expr

# Define watermarks
impressionsWithWatermark = impressions \
  .selectExpr("adId AS impressionAdId", "impressionTime") \
  .withWatermark("impressionTime", "10 seconds ")
clicksWithWatermark = clicks \
  .selectExpr("adId AS clickAdId", "clickTime") \
  .withWatermark("clickTime", "20 seconds")        # max 20 seconds late


# Inner join with time range conditions
display(
  impressionsWithWatermark.join(
    clicksWithWatermark,
    expr(""" 
      clickAdId = impressionAdId AND 
      clickTime >= impressionTime AND 
      clickTime <= impressionTime + interval 1 minutes    
      """
    )
  )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Outer Joins with Watermarking
# MAGIC Let's extend this use case to illustrate outer joins. Not all ad impressions will lead to clicks and you may want to keep track of impressions that did not produce clicks. This can be done by applying a left outer join on the impressions and clicks. The joined output will not have the matched clicks, but also the unmatched ones (with clicks data being NULL).
# MAGIC
# MAGIC While the watermark + event-time constraints is optional for inner joins, for left and right outer joins they must be specified. This is because for generating the NULL results in outer join, the engine must know when an input row is not going to match with anything in future. Hence, the watermark + event-time constraints must be specified for generating correct results.

# COMMAND ----------

from pyspark.sql.functions import expr

# Inner join with time range conditions
display(
  impressionsWithWatermark.join(
    clicksWithWatermark,
    expr(""" 
      clickAdId = impressionAdId AND 
      clickTime >= impressionTime AND 
      clickTime <= impressionTime + interval 1 minutes    
      """
    ),
    "leftOuter"
  )
)
