# Databricks notebook source
df = spark.range(1, 1000)

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------


