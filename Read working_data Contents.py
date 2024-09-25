# Databricks notebook source
from pyspark.sql import functions as pyf
from pyspark.sql.types import *
from pyspark.sql.window import Window
import datetime as dtm
import uuid

#CONSTANTS
current_datetime = dtm.datetime.now()

# COMMAND ----------

# Loading all variables and constants
dbutils.widgets.text("RETAILER_PARAM", "WALMART", "Retailer(s)")
dbutils.widgets.text("CLIENT_PARAM", "DOLE", "Client(s)")

# COMMAND ----------

# Parameters
retailer = dbutils.widgets.get("RETAILER_PARAM").upper()
client = dbutils.widgets.get("CLIENT_PARAM").upper()

# COMMAND ----------

# Collect all current champions and extract the schema for controlling the fields made available
current_champions = spark.sql("SELECT * FROM retail_forecast_engine.champion_models limit 1")
schema_champion = [element.name for element in current_champions.schema if element.name != "MODEL_PATH"]

# Collect schema column names to filter the model training results table
# Omit the MODEL_PATH variable as it doesn't exist in the model training results table

# COMMAND ----------

print(schema_champion)

# COMMAND ----------

working_data = spark.read.parquet("/mnt/artifacts/training_results/")\
.filter("RETAILER = '{}' and CLIENT = '{}'".format(retailer, client))\
.withColumn("MADE_CHAMPION_DATE", pyf.lit(current_datetime))\
.withColumn("TEST_SET_MSE_PERFORMANCE", pyf.col("METRICS.mse_test"))\
.withColumn("COLUMN_NAMES", pyf.array_join("COLUMN_NAMES",","))\
.filter("MODEL_ID is not null")\
.select(*schema_champion)\
.withColumn("RETAILER", pyf.upper(pyf.col("RETAILER")))\
.withColumn("CLIENT", pyf.upper(pyf.col("CLIENT")))

# COMMAND ----------

display(working_data)

# COMMAND ----------


