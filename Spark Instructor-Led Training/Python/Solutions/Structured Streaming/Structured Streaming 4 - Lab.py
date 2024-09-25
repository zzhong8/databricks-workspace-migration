# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img src="http://curriculum-release.s3-website-us-west-2.amazonaws.com/images/Apache-Spark-Logo_TM_200px.png" style="float: left: margin: 20px"/>
# MAGIC
# MAGIC # Structured Streaming Lab

# COMMAND ----------

# MAGIC %md
# MAGIC ## Introduction
# MAGIC
# MAGIC Some of the log messages have IP addresses in them. 
# MAGIC
# MAGIC But how many log messages are coming from each IP address?
# MAGIC
# MAGIC To get you started, we have created the `initialDF` for you - just run the next two cells below to get started - all of which is just a copy-and-paste from a previous notebook:

# COMMAND ----------

# MAGIC %run "../Includes/Classroom Setup"

# COMMAND ----------

from pyspark.sql.functions import *
spark.conf.set("spark.sql.shuffle.partitions", 8)

# This lab requires Apache Spark 2.1 or better.
assertSparkVersion(2,1)

# Create our initial DataFrame
initialDF = (spark.readStream
  .format("socket")
  .option("host", "server1.databricks.training")
  .option("port", 9001)
  .load()
  .withColumn("ts_string", col("value").substr(2, 23))
  .withColumn("epoc", unix_timestamp("ts_string", "yyyy/MM/dd HH:mm:ss.SSS"))
  .withColumn("capturedAt", col("epoc").cast("timestamp"))
  .withColumn("logData", regexp_extract("value", """^.*\]\s+(.*)$""", 1))
  .select("capturedAt", "logData")
)

# This is the regular expression pattern that we will use later.
IP_REG_EX = """^.*\s+(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}).*$"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC To solve this problem, you need to:
# MAGIC
# MAGIC 0. Parse the IP address from the column `logData` with the `regexp_extract()` function.
# MAGIC   * You will need a regular expression which we have already provided above as `IP_REG_EX`.
# MAGIC   * Hint: take the 1st matching value.
# MAGIC 0. Filter out the records that don't contain IP addresses,
# MAGIC   * How can you tell that an IP address wasn't parsed? 
# MAGIC   * Hint: You'll have to read the docs for `regexp_extract()`.
# MAGIC 0. Perform an aggregation over a window of time, grouping by the `capturedAt` window and `ip`.
# MAGIC 0. Count the unique IP addresses within the window.
# MAGIC
# MAGIC For this lab, use a 10-second window.
# MAGIC
# MAGIC Your final `DataFrame`, the one you pass to `display()`, should have three columns:
# MAGIC
# MAGIC * `ip`: The IP address
# MAGIC * `count`: The number of times that IP address appeared in the window
# MAGIC * `window`: The window column
# MAGIC
# MAGIC For additional information see the `regexp_extract()`, `length()` and `window()` functions in
# MAGIC * <a href="http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$" target="_blank">org.apache.spark.sql.functions (Scala)</a> or
# MAGIC * <a href="http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions" target="_blank">pyspark.sql.function (Python)</a>

# COMMAND ----------

# ANSWER

ipDF = (
  initialDF
    .withColumn("ip", regexp_extract("logData", IP_REG_EX, 1))
    .filter(length("ip") > 0)
    .groupBy( window("capturedAt", "10 second"), "ip")
    .count()
)

# COMMAND ----------

# TEST - Run this cell to test your solution.

display(
  ipDF
    .select( col("window.end").alias("time"), col("ip"), col("count") )
    .orderBy("time", "ip")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC * [Structured Streaming 1 - Intro]($./Structured Streaming 1 - Intro)
# MAGIC * [Structured Streaming 2 - TCP/IP]($./Structured Streaming 2 - TCPIP)
# MAGIC * [Structured Streaming 3 - Kafka]($./Structured Streaming 3 - Kafka)
# MAGIC * Structured Streaming 4 - Lab

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
