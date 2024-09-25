# Databricks notebook source
import pyspark.sql.functions as pyf
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# COMMAND ----------

# MAGIC %md
# MAGIC ## Call data (all actions)

# COMMAND ----------

call_data_path = '/mnt/processed/ari/measurement_poc/kens_walmart/data/df_call_data'
df_call_data = spark.read.format('delta').load(call_data_path)

# COMMAND ----------

display(df_call_data)

# COMMAND ----------

df_call_data = df_call_data.toPandas()

# COMMAND ----------

print(f"Earliest date: {df_call_data['calendar_key'].min()}")
print(f"Latest date: {df_call_data['calendar_key'].max()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Measurement data (only measured actions)

# COMMAND ----------

time_and_measurement_path = '/mnt/processed/ari/measurement_poc/kens_walmart/data/df_time_and_measurement'
df_time_and_measurement = spark.read.format('delta').load(time_and_measurement_path).toPandas()

# COMMAND ----------

print(f"Earliest date: {df_time_and_measurement['call_date'].min()}")
print(f"Latest date: {df_time_and_measurement['call_date'].max()}")

# COMMAND ----------

display(df_time_and_measurement)

# COMMAND ----------

# MAGIC %md
# MAGIC NOTE:
# MAGIC - Joining df_call_data with df_time_and_measurement will result in many NULL values, since df_time_and_measurement only includes measurable actions.
# MAGIC - We only included responses in df_call_data that are considered meaningful actions. All "non-actions" were removed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time Standards (all actions)

# COMMAND ----------

time_standards_path = '/mnt/processed/ari/measurement_poc/kens_walmart/data/df_time_standards'
df_time_standards = spark.read.format('delta').load(time_standards_path).toPandas()
# df_time_standards = df_time_standards.drop(columns=['xerr', 'error_95pct'])

# COMMAND ----------

display(df_time_standards)

# COMMAND ----------

# MAGIC %md
# MAGIC Note: duration_error is the 95% confidence interval.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dashboard ideas
# MAGIC - Call data by action (optional filtering on geography/time/etc.)
# MAGIC - Call data by action by duration
# MAGIC - Bucket calls by duration, show distribution of actions per bucket (do shorter calls tend to have different actions than longer calls?)
# MAGIC - Measured value and duration by action
# MAGIC - Explore measured value by day of the week, month
# MAGIC - Average Number of each Actions per Call by Action
# MAGIC - Value vs Average Number of each Actions performed each Call
# MAGIC - How many times is each action performed per call?
# MAGIC - Actions by "level" (item_entity_id: e.g. PRDE, PRSK, etc.)
# MAGIC - Ask similar questions, but by OBJECTIVE not action

# COMMAND ----------


