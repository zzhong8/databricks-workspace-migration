# Databricks notebook source
# MAGIC %md
# MAGIC # Measurement Results
# MAGIC
# MAGIC This notebook computes all the _post hoc_ corrections and busniess rules that needs to occur the measurement results.
# MAGIC
# MAGIC Corrections made are
# MAGIC - 
# MAGIC
# MAGIC ## TODOs
# MAGIC 1. Load intervention data for relative date range vs. current date; intended to limit the join sizes for next step
# MAGIC 2. Join intervention data to measurement data table; keep only rows without a measured value
# MAGIC 3. Apply measurement model to remaining unscored interventions
# MAGIC 4. Write scored data to measurement table

# COMMAND ----------

from pprint import pprint

import itertools

import numpy as np
import pandas as pd

import matplotlib.pyplot as graph
import seaborn as sns

import pyspark.sql.functions as pyf
import pyspark.sql.types as pyt

# COMMAND ----------

# Inputs for Retailer, Client and RunID
dbutils.widgets.text('retailer', '', 'Retailer')
dbutils.widgets.text('client', '', 'Client')
dbutils.widgets.text('country_code', '', 'Country Code')
dbutils.widgets.text('run_id', '', 'Run ID')

required = ('retailer', 'client', 'country_code', 'run_id')

def process_notebook_inputs(name):
    value = dbutils.widgets.get(name).strip().lower()
    if name in required and value == '':
        raise ValueError(f'"{name}" is required')
        
    return value

retailer, client, country_code, run_id = [process_notebook_inputs(s) for s in ('retailer', 'client', 'country_code', 'run_id')]

_ = [print(inputs) for inputs in (retailer, client, country_code, run_id)]

# COMMAND ----------

df = spark.read.format('delta').load(f'/mnt/processed/causal_results/retailer={retailer}/client={client}/country_code={country_code}')
 
total_baseline = df.agg(pyf.sum('BASELINE')).toPandas().values[0][0]
 
# df = df.filter('INTERVENTION_TYPE != "None"')
 
print(f'{df.cache().count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Make _Post Hoc_ Adjustments

# COMMAND ----------

path_engineered_results = f'/mnt/processed/training/{run_id}/engineered/'
df_pos = spark.read.format('delta').load(path_engineered_results)

df_subset_meets_threshold = df_pos\
    .select('RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM', 'POS_ITEM_QTY')\
    .filter('POS_ITEM_QTY > 0')\
    .groupBy('RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM')\
    .count()\
    .filter('count >= 31')\
    .drop('count')

df_pos = df_pos.join(
    pyf.broadcast(df_subset_meets_threshold),
    ['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'],
    'leftsemi'
)

df_pos = df_pos.fillna({
    'ON_HAND_INVENTORY_QTY': 0,
    'RECENT_ON_HAND_INVENTORY_QTY': 0,
    'RECENT_ON_HAND_INVENTORY_DIFF': 0
})

print(f'POS Size = {df_pos.cache().count():,}')
display(df_pos)

# COMMAND ----------

df_results = df.join(
    df_pos.select('RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM', 'SALES_DT', 'UNIT_PRICE'),
    ['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM', 'SALES_DT']
)
df_results = df_results.filter('INTERVENTION_TYPE != "None"')

print(f'{df_results.cache().count():,}')
display(df_results)

# COMMAND ----------

schema = df_results.schema
@pyf.pandas_udf(schema, pyf.PandasUDFType.GROUPED_MAP)
def book_stock_rule_udf(df):
    if 'Book Stock Error' in df['INTERVENTION_TYPE'].unique()[0]:
        claimed_selector = (df['DIFF_DAY'] >= 2) & (df['DIFF_DAY'] <= 6)

        # Set to zero
        df['INTERVENTION_EFFECT'] = 0.0
        df['QINTERVENTION_EFFECT'] = 0.0

        # Set values
        df.loc[claimed_selector, 'INTERVENTION_EFFECT'] = df[claimed_selector]['POS_ITEM_QTY']
        df['QINTERVENTION_EFFECT'] = df['INTERVENTION_EFFECT']
    return df

@pyf.pandas_udf(schema, pyf.PandasUDFType.GROUPED_MAP)
def nonnegative_rule_udf(df):
    if df['INTERVENTION_EFFECT'].sum() <= 0:
        df['INTERVENTION_EFFECT'] = 0.0
        df['QINTERVENTION_EFFECT'] = 0.0
    return df


# Execute UDFs
df_results = df_results.groupby('INTERVENTION_ID').apply(book_stock_rule_udf)
print('{:,}'.format(df_results.cache().count()))

df_results = df_results.groupby('INTERVENTION_ID').apply(nonnegative_rule_udf)
print('{:,}'.format(df_results.cache().count()))

# COMMAND ----------

df_results = df_results.withColumn('INTERVENTION_VALUE', df_results['INTERVENTION_EFFECT'] * df_results['UNIT_PRICE'])
print(df_results.cache().count())

display(df_results)

# COMMAND ----------

# MAGIC %md
# MAGIC # Hugh Magic

# COMMAND ----------


