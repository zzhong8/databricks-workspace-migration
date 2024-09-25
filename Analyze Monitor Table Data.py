# Databricks notebook source
from pyspark.sql import functions as pyf
from pyspark.sql import types as pyt

# COMMAND ----------

campbellssnack_table = spark.sql("SELECT * FROM walmart_campbellssnack_dv.vw_sat_retailer_item_unit_price")

# COMMAND ----------

display(campbellssnack_table.describe())

# COMMAND ----------

monitor_table = spark.sql("SELECT * FROM retail_forecast_engine.monitoring")

# COMMAND ----------

display(monitor_table.describe())

# COMMAND ----------


