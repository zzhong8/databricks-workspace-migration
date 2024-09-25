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

df_results_display = df_results.filter(
    (pyf.col('standard_response_cd') == 'display') &
    (
      (pyf.col('SALES_DT') >= pyf.lit("2021-05-15")) |
      (pyf.col('SALES_DT') <= pyf.lit("2021-05-07"))
    )
)

# COMMAND ----------

df_results_display.count()

# COMMAND ----------

display(df_results_display)

# COMMAND ----------

df_results_display_agg = df_results_display.groupBy('RETAILER_ITEM_ID', 
                                   'ORGANIZATION_UNIT_NUM',
                                   'response_id',
                                   'call_id',
                                   'standard_response_cd',
                                   'intervention_group').agg(
                                           pyf.sum("POS_ITEM_QTY").alias('TOTAL_POS_ITEM_QTY'), 
                                           pyf.sum("BASELINE").alias('TOTAL_BASELINE'),
                                           pyf.sum("EXPECTED").alias('TOTAL_EXPECTED'),
                                           pyf.sum("INTERVENTION_EFFECT_NEW").alias('TOTAL_INTERVENTION_EFFECT_NEW'))

# COMMAND ----------

display(df_results_display_agg)

# COMMAND ----------

df_results_filtered = df_results.filter(
    pyf.col('SALES_DT') >= pyf.lit("2021-07-01")
)

# COMMAND ----------

df_results_filtered.count()

# COMMAND ----------

display(df_results_filtered)

# COMMAND ----------

display(df_results_filtered)

# COMMAND ----------

df_results = df_results.withColumn(
    'INTERVENTION_EFFECT_NEW', 
    df_results['EXPECTED'] - df_results['BASELINE']
)

# COMMAND ----------

df_results_agg = df_results.groupBy('RETAILER_ITEM_ID', 
                                   'ORGANIZATION_UNIT_NUM',
                                   'response_id',
                                   'call_id',
                                   'standard_response_cd',
                                   'intervention_group').agg(
                                           pyf.sum("POS_ITEM_QTY").alias('TOTAL_POS_ITEM_QTY'), 
                                           pyf.sum("BASELINE").alias('TOTAL_BASELINE'),
                                           pyf.sum("EXPECTED").alias('TOTAL_EXPECTED'),
                                           pyf.sum("INTERVENTION_EFFECT_NEW").alias('TOTAL_INTERVENTION_EFFECT_NEW'))

# COMMAND ----------

display(df_results_agg)

# COMMAND ----------

df_results_filtered_agg = df_results_filtered.groupBy('RETAILER_ITEM_ID', 
                                   'ORGANIZATION_UNIT_NUM',
                                   'response_id',
                                   'call_id',
                                   'standard_response_cd',
                                   'intervention_group').agg(
                                           pyf.sum("POS_ITEM_QTY").alias('TOTAL_POS_ITEM_QTY'), 
                                           pyf.sum("BASELINE").alias('TOTAL_BASELINE'),
                                           pyf.sum("EXPECTED").alias('TOTAL_EXPECTED'),
                                           pyf.sum("INTERVENTION_EFFECT_NEW").alias('TOTAL_INTERVENTION_EFFECT_NEW'))

# COMMAND ----------

display(df_results_filtered_agg)

# COMMAND ----------

# %sql

# select retailer_item_id, retailer_item_desc
# from retaillink_walmart_milos_us_dv.sat_retailer_item

# where
# retailer_item_id in (9051156, 9051163, 9064477, 553615326, 552237723)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select retailer_item_id, retailer_item_desc
# MAGIC from retaillink_walmart_edgewellpersonalcare_us_dv.sat_retailer_item
# MAGIC
# MAGIC where
# MAGIC retailer_item_id in (553707161)

# COMMAND ----------


