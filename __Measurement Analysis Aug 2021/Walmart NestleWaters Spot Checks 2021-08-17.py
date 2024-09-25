# Databricks notebook source
import pandas as pd

import matplotlib.pyplot as graph
import seaborn as sns

import pyspark.sql.functions as pyf
import pyspark.sql.types as pyt

from acosta.measurement import process_notebook_inputs

# COMMAND ----------

# Inputs get required inputs
dbutils.widgets.text('country_id', '-1', 'Country ID')
dbutils.widgets.text('client_id', '-1', 'Client ID')
dbutils.widgets.text('holding_id', '-1', 'Holding ID')
dbutils.widgets.text('banner_id', '-1', 'Banner ID')

input_names = ('country_id', 'client_id', 'holding_id', 'banner_id')

country_id, client_id, holding_id, banner_id = [process_notebook_inputs(dbutils.widgets.get(s)) for s in input_names]

print('Country ID =', country_id)
print('Client ID =', client_id)
print('Holding ID =', holding_id)
print('Banner ID =', banner_id)

# COMMAND ----------

# Load required data
df_results = spark.read.format('delta').load(
    f'/mnt/processed/measurement/results/{client_id}-{country_id}-{holding_id}-{banner_id}'
)

# COMMAND ----------

print(f'{df_results.cache().count():,}')

# COMMAND ----------

display(df_results)

# COMMAND ----------


