# Databricks notebook source
'''
The code change is pretty easy.  Instead of doing something like "df.write.format("parquet").mode("overwrite").save(...)",
we would just do:  df.insertInto(hiveTableName, , overwrite = False)
'''

# COMMAND ----------

# # Loading all variables and constants
# dbutils.widgets.text("RETAILER_PARAM", "WALMART", "Retailer")
# dbutils.widgets.text("CLIENT_PARAM", "", "Client")

# COMMAND ----------

from pyspark.sql import functions as pyf
from pyspark.sql.types import *
from pyspark.sql.window import Window
import datetime as dtm
import uuid

#CONSTANTS
current_datetime = dtm.datetime.now()

# COMMAND ----------

# Collect all current champions and extract the schema for controlling the fields made available
current_champions = spark.sql("SELECT * FROM retail_forecast_engine.champion_models limit 1")

# Collect schema column names to filter the model training results table
# Omit the MODEL_PATH variable as it doesn't exist in the model training results table

# COMMAND ----------

schema_champion = [element.name for element in current_champions.schema if element.name != "MODEL_PATH"]

# Select all records from MODEL_TRAINING_RESULTS and union the current champions for comparison
# TODO Consider eliminating the need for the COLUMN_NAMES column to be a string and just use the array
# in the inference driver
working_data = spark.read.parquet("/mnt/artifacts/training_results/")\
  .withColumn("MADE_CHAMPION_DATE", pyf.lit(current_datetime))\
  .withColumn("TEST_SET_MSE_PERFORMANCE", pyf.col("METRICS.mse_test"))\
  .withColumn("COLUMN_NAMES", pyf.array_join("COLUMN_NAMES",","))\
  .filter("MODEL_ID is not null")\
  .select(*schema_champion)\
  .withColumn("RETAILER", pyf.upper(pyf.col("RETAILER")))\
  .withColumn("CLIENT", pyf.upper(pyf.col("CLIENT")))

# COMMAND ----------

# working_data = working_data.filter("CLIENT == 'CAMPBELLSSNACK'")

# display(working_data.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Logic to determine Champions

# COMMAND ----------

# Aggregate to the best perfomer for Client Retailer, Store, UPC
## One approach is to do an index across 
window_mse_rank = Window.partitionBy().orderBy("TEST_SET_MSE_PERFORMANCE")
window_count_rank = Window.partitionBy().orderBy("TEST_DAYS_COUNT")

window_in_parition_weighted_rank = Window.partitionBy("RETAILER_ITEM_ID","ORGANIZATION_UNIT_NUM","RETAILER","CLIENT")\
  .orderBy(pyf.col("TEST_DAYS_COUNT").desc())

non_zero_error = 0.000001
df_mse_ranked = working_data.withColumn("mse_rank", 1 - (pyf.percent_rank().over(window_mse_rank) + non_zero_error))
df_counts_ranked = df_mse_ranked.withColumn("count_rank", (pyf.percent_rank().over(window_count_rank) + non_zero_error))

weight_mse = 0.5
weight_count = 0.5

df_final_rank = df_counts_ranked.withColumn( "weighted_rank", 
                                            (pyf.col("mse_rank") * weight_mse)\
                                            + (pyf.col("count_rank") * weight_count)
                                           )

df_new_champion = df_final_rank\
  .withColumn("in_partition_rank", pyf.row_number().over(window_in_parition_weighted_rank))\
  .filter("in_partition_rank == 1")\
  .select(*schema_champion)\
  .alias("new")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Logging Replacements

# COMMAND ----------

# Write out those that aren't the best and log them
# TODO: Consider writing out those that are completely gone from champion model
# TODO: Consider writing out those that are new to the champions file
champions_being_replaced = current_champions.select(
  "RETAILER_ITEM_ID","ORGANIZATION_UNIT_NUM","RETAILER","CLIENT", pyf.col("MADE_CHAMPION_DATE").alias("MADE_CHAMPION_DATE_DROPPED"), pyf.col("MODEL_ID").alias("MODEL_ID_DROPPED")
  )\
  .join(
    df_new_champion.select("RETAILER_ITEM_ID","ORGANIZATION_UNIT_NUM","RETAILER","CLIENT", pyf.col("MODEL_ID").alias("MODEL_ID_REPLACEMENT")),
    ["RETAILER_ITEM_ID","ORGANIZATION_UNIT_NUM","RETAILER","CLIENT"],
    "left"
  )\
  .filter("MODEL_ID_REPLACEMENT != MODEL_ID_DROPPED")


champions_being_replaced\
  .write\
  .mode("append")\
  .format("parquet")\
  .save("/mnt/artifacts/audit/champion_changes/{year}/{month}/{day}/{hour}/{minute}".format(
    year = current_datetime.year, month = current_datetime.month, day = current_datetime.day, 
    hour = current_datetime.hour, minute = current_datetime.minute
  ))

# COMMAND ----------

# TODO If this ever becomes a bottleneck, we can change the code .partitionBy("RETAILER", "CLIENT")\ 
#      to partition by ORGANIZATION_UNIT_NUM or RETAILER_ITEM_ID instead so that there is more than just the one partition.

# Replace the champions models
df_new_champion\
  .repartition("RETAILER", "CLIENT")\
  .write\
  .mode("overwrite")\
  .format("parquet")\
  .partitionBy("RETAILER", "CLIENT")\
  .save("/mnt/artifacts/champion_models/")

# COMMAND ----------


