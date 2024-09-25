# Databricks notebook source
import numpy as np
import pandas as pd

import pyspark.sql.functions as pyf
import pyspark.sql.types as pyt
from databricks.feature_store import FeatureStoreClient, feature_table

# COMMAND ----------

import_path = '/mnt/artifacts/reference/cost_to_serve_poc.csv'

# Load tables with business rules and join it to the main table
df_cost_to_serve = spark.read.format('csv') \
    .options(header='true', inferSchema='true') \
    .load(import_path)

display(df_cost_to_serve)

# COMMAND ----------

df_cost_to_serve_filtered = df_cost_to_serve.where(pyf.col('num_stores_P3M') > 0)
df_cost_to_serve_filtered = df_cost_to_serve.where(pyf.col('total_num_alerts') > 0)

display(df_cost_to_serve_filtered)

# COMMAND ----------

# Let's build the features
df_cost_by_store_variables = df_cost_to_serve_filtered.select("retailer", 
                                                              "country", 
                                                              "team", 
                                                              "num_stores_P3M", 
                                                              "num_items_P3M", 
                                                              "DRFE_forecast_cost",
                                                              "value_measurement_cost",
                                                              "DLA_alert_gen_cost",
                                                              "team_alerts_cost",
                                                              "data_ingestion_cost",
                                                              "total_cost")

df_cost_by_store_variables = df_cost_by_store_variables.withColumn("id", pyf.monotonically_increasing_id())
df_cost_by_store_variables = df_cost_by_store_variables.withColumn("num_stores_items_P3M", pyf.col('num_stores_P3M') * pyf.col('num_items_P3M'))

df_cost_by_store_variables = df_cost_by_store_variables.select("id", 
                                                               "retailer", 
                                                               "country", 
                                                               "team", 
                                                               "num_stores_P3M",
                                                               "num_items_P3M", 
                                                               "num_stores_items_P3M",
                                                               "DRFE_forecast_cost",
                                                               "value_measurement_cost",
                                                               "DLA_alert_gen_cost",
                                                               "team_alerts_cost",
                                                               "data_ingestion_cost",
                                                               "total_cost")

display(df_cost_by_store_variables)

# COMMAND ----------

df_cost_by_store_variables_na = df_cost_by_store_variables.where((pyf.col('country') == 'us') | (pyf.col('country') == 'ca'))
df_cost_by_store_variables_na = df_cost_by_store_variables_na.where((pyf.col('retailer') == 'walmart') | (pyf.col('retailer') == 'kroger'))

display(df_cost_by_store_variables_na)

# COMMAND ----------

df_cost_by_store_variables_us = df_cost_by_store_variables.where(pyf.col('country') == 'us')
df_cost_by_store_variables_us = df_cost_by_store_variables_us.where((pyf.col('retailer') == 'walmart') | (pyf.col('retailer') == 'kroger'))

display(df_cost_by_store_variables_us)

# COMMAND ----------

df_cost_by_store_variables_uk_four_retailers = df_cost_by_store_variables.where(pyf.col('country') == 'uk')
df_cost_by_store_variables_uk_four_retailers = df_cost_by_store_variables_uk_four_retailers.where((((pyf.col('retailer') == 'asda') | (pyf.col('retailer') == 'morrisons')) | (pyf.col('retailer') == 'sainsburys')) | (pyf.col('retailer') == 'tesco'))

display(df_cost_by_store_variables_uk_four_retailers)

# COMMAND ----------

df_cost_by_store_variables_uk = df_cost_by_store_variables.where(pyf.col('country') == 'uk')
df_cost_by_store_variables_uk = df_cost_by_store_variables_uk.where(((pyf.col('retailer') == 'asda') | (pyf.col('retailer') == 'morrisons')) | (pyf.col('retailer') == 'tesco'))

display(df_cost_by_store_variables_uk)

# COMMAND ----------

database_name = 'retail_forecast_engine'

# COMMAND ----------

# register as features set
fs = FeatureStoreClient()

try:
    fs.drop_table(f'{database_name}.cost_to_serve_attributes')
except:
    pass
feature_table = fs.create_table(
    name=f'{database_name}.cost_to_serve_attributes',
    primary_keys=['id'],
    schema=df_cost_by_store_variables.schema,
    description="This table contains cost to serve attributes."
)

try:
    fs.drop_table(f'{database_name}.cost_to_serve_attributes_na')
except:
    pass
feature_table = fs.create_table(
    name=f'{database_name}.cost_to_serve_attributes_na',
    primary_keys=['id'],
    schema=df_cost_by_store_variables.schema,
    description="This table contains cost to serve attributes for Walmart and Kroger in the US and Canada."
)

try:
    fs.drop_table(f'{database_name}.cost_to_serve_attributes_us')
except:
    pass
feature_table = fs.create_table(
    name=f'{database_name}.cost_to_serve_attributes_us',
    primary_keys=['id'],
    schema=df_cost_by_store_variables.schema,
    description="This table contains cost to serve attributes for Walmart and Kroger in the US."
)

try:
    fs.drop_table(f'{database_name}.cost_to_serve_attributes_uk')
except:
    pass
feature_table = fs.create_table(
    name=f'{database_name}.cost_to_serve_attributes_uk',
    primary_keys=['id'],
    schema=df_cost_by_store_variables.schema,
    description="This table contains cost to serve attributes for Asda, Morrisons, and Tesco in the UK."
)

try:
    fs.drop_table(f'{database_name}.cost_to_serve_attributes_uk_four_retailers')
except:
    pass
feature_table = fs.create_table(
    name=f'{database_name}.cost_to_serve_attributes_uk_four_retailers',
    primary_keys=['id'],
    schema=df_cost_by_store_variables.schema,
    description="This table contains cost to serve attributes for Asda, Morrisons, Sainsburys, and Tesco in the UK."
)

# COMMAND ----------

fs.write_table(df=df_cost_by_store_variables, name=f"{database_name}.cost_to_serve_attributes", mode="overwrite")
fs.write_table(df=df_cost_by_store_variables_na, name=f"{database_name}.cost_to_serve_attributes_na", mode="overwrite")
fs.write_table(df=df_cost_by_store_variables_us, name=f"{database_name}.cost_to_serve_attributes_us", mode="overwrite")
fs.write_table(df=df_cost_by_store_variables_uk, name=f"{database_name}.cost_to_serve_attributes_uk", mode="overwrite")
fs.write_table(df=df_cost_by_store_variables_uk_four_retailers, name=f"{database_name}.cost_to_serve_attributes_uk_four_retailers", mode="overwrite")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_forecast_engine.cost_to_serve_attributes_us

# COMMAND ----------


