# Databricks notebook source
spark.conf.set('hive.exec.dynamic.partition.mode', 'nonstrict')
spark.conf.set('hive.exec.dynamic.partition', 'true')
spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

# COMMAND ----------

import pandas as pd

import matplotlib.pyplot as graph
import seaborn as sns

import pyspark.sql.functions as pyf
import pyspark.sql.types as pyt

from delta.tables import DeltaTable
from itertools import chain

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

client_config = spark.sql(f'''
  SELECT * FROM acosta_retail_analytics_im.interventions_retailer_client_config_gen2_migration
  WHERE
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  coalesce(mdm_banner_id, -1) = {banner_id}
''')
assert client_config.cache().count() == 1

# Create config dict
config_dict = client_config.toPandas().T
config_dict = dict(zip(config_dict.index, config_dict[0]))

# COMMAND ----------

# Load required data
df_results = spark.read.format('delta').load(
    f'/mnt/processed/measurement/cache/{client_id}-{country_id}-{holding_id}-{banner_id}-processed-clone'
)
total_baseline = df_results.agg(pyf.sum('BASELINE')).toPandas().values[0][0]
df_interventions = spark.read.format('delta').load(
    f'/mnt/processed/measurement/cache/{client_id}-{country_id}-{holding_id}-{banner_id}-intervention'
)
df_unique_interventions = df_interventions.select(
    'mdm_country_nm', 'mdm_holding_nm', 'mdm_banner_nm', 'mdm_client_nm',
    'store_acosta_number', 'epos_organization_unit_num', 'epos_retailer_item_id',
    'objective_typ', 'call_id', 'response_id',
    'nars_response_text', 'standard_response_text',
    'duration', 'call_date'
).distinct()

print(f'{df_results.cache().count():,}')
print(f'{df_interventions.cache().count():,}')
print(f'{df_unique_interventions.cache().count():,}')

# COMMAND ----------

display(df_interventions)

# COMMAND ----------

df_results = df_results.filter('standard_response_cd != "none"')
print(f'{df_results.cache().count():,}', 'N')
print(df_results.select('response_id').distinct().count(), 'N interventions')
print(df_results.select('call_id').distinct().count(), 'N visits')

# COMMAND ----------

# value & impact calculation
df_results = df_results.withColumn('INTERVENTION_VALUE', df_results['INTERVENTION_EFFECT'] * df_results['PRICE'])
print(f'{df_results.cache().count():,}')

# COMMAND ----------

display(df_results)

# COMMAND ----------

def nonnegative_rule_udf(df):
    if df['INTERVENTION_EFFECT'].sum() <= 0 or df['INTERVENTION_VALUE'].sum() <= 0:
        df['INTERVENTION_EFFECT'] = 0.0
        df['QINTERVENTION_EFFECT'] = 0.0
        df['INTERVENTION_VALUE'] = 0.0
    return df

# Execute UDFs
df_results_non_neg = df_results.groupby('response_id').applyInPandas(nonnegative_rule_udf, schema=df_results.schema)
print(f'{df_results_non_neg.cache().count():,}')

# COMMAND ----------

display(df_results_non_neg)

# COMMAND ----------


