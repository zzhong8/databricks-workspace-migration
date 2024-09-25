# Databricks notebook source
# MAGIC %md
# MAGIC # Cache Measurement Data
# MAGIC
# MAGIC Write the data that `2.0_Fitting` notebook needs to run

# COMMAND ----------

from pprint import pprint

import numpy as np
import pandas as pd

import pyspark.sql.functions as pyf
import pyspark.sql.types as pyt

from causalgraphicalmodels import CausalGraphicalModel

from acosta.alerting.helpers import universal_encoder, universal_decoder, check_path_exists
from acosta.alerting.helpers.features import get_lag_column_name

import acosta
import pyarrow

print(acosta.__version__)
print(pyarrow.__version__)

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

cgm = CausalGraphicalModel(
    nodes=['time of year', 'store', 'display', 'price', 'inventory', 'inventory correction', 'order placement', 'shelf stock', 'shelf changes', 'placed pos', 'N sold'],
    edges=[
        ('time of year', 'inventory'), ('time of year', 'display'), ('time of year', 'display-store'), ('time of year', 'price'), ('time of year', 'N sold'),
        ('store', 'display'), ('store', 'inventory'), ('store', 'price'), ('store', 'N sold'),
        ('display', 'price'), ('display', 'inventory'), ('display', 'N sold'),
        ('price', 'N sold'),
        ('inventory', 'N sold'), ('inventory', 'shelf stock'),
        ('inventory correction', 'inventory'),
        ('order placement', 'inventory'),
        ('shelf stock', 'N sold'), 
        ('shelf changes', 'shelf stock'), ('shelf changes', 'N sold'),
        ('placed pos', 'N sold')
    ]
)

pprint(cgm.get_all_backdoor_adjustment_sets('display', 'N sold'))

# COMMAND ----------

# Simplified causal diagram
simple_cgm = CausalGraphicalModel(
    nodes=['time of year', 'store', 'intervention', 'price', 'inventory', 'shelf stock', 'N sold'],
    edges=[
        ('time of year', 'inventory'), ('time of year', 'intervention'), ('time of year', 'price'), ('time of year', 'N sold'),
        ('store', 'intervention'), ('store', 'inventory'), ('store', 'price'), ('store', 'N sold'),
        ('intervention', 'price'), ('intervention', 'inventory'), ('intervention', 'N sold'), ('intervention', 'shelf stock'),
        ('price', 'N sold'),
        ('inventory', 'N sold'), ('inventory', 'shelf stock'),
        ('shelf stock', 'N sold')
    ]
)

pprint(simple_cgm.get_all_backdoor_adjustment_sets('intervention', 'N sold'))

# COMMAND ----------

# Required paths
path_engineered_results = f'/mnt/processed/training/{run_id}/engineered/'
# path_intervention_data = f'/mnt/processed/temp/khz_{retailer}_inv_epos'  # TODO change for production
path_intervention_data = f'/mnt/processed/temp/{client}_{retailer}_inv_epos'

# print(path_results)
print(path_engineered_results)
print(path_intervention_data)

for path in [path_intervention_data, path_engineered_results]:
    check_path_exists(path, 'delta')

# COMMAND ----------

# Load POS data (Intermediate Data & Engineer Features needs to be run before this notebook can run)
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

# print(df_pos.cache().count())
# display(df_pos)

# COMMAND ----------

# Load intervention callfile data
df_intervention = spark.read.format('delta').load(path_intervention_data)
df_intervention = df_intervention\
    .withColumn('ChainRefExternal', df_intervention['ChainRefExternal'].cast('long'))\
    .withColumn('DiffDay', pyf.datediff('SALES_DT', 'InterventionDate'))\
    .drop('POS_ITEM_QTY', 'BASELINE_POS_ITEM_QTY')

display(df_intervention)

# COMMAND ----------

# Merge datasets
min_date = df_intervention.select(pyf.min('SALES_DT')).collect()[0].asDict().values()
max_date = df_intervention.select(pyf.max('SALES_DT')).collect()[0].asDict().values()

min_date, max_date = list(min_date)[0], list(max_date)[0]
print(min_date, '|', max_date)

df_pos = df_pos.filter(
    (pyf.col('SALES_DT') >= min_date) &
    (pyf.col('SALES_DT') <= max_date)
)
print('Merging Datasets')

df_merged = df_pos.join(
    df_intervention,
    (df_pos['SALES_DT'] == df_intervention['SALES_DT']) & (df_pos['RETAILER_ITEM_ID'] == df_intervention['RefExternal']) & (df_pos['ORGANIZATION_UNIT_NUM'] == df_intervention['ChainRefExternal']),
    how='full'
).drop(df_intervention['SALES_DT'])  # This removes duplicate column names

# Clean data
df_merged = df_merged.withColumn('is_intervention', pyf.col('Intervention').isNotNull().cast('float'))
df_merged = df_merged.fillna({'Intervention': 'None'})
df_merged = df_merged.withColumn('InterventionStnd', df_merged['Intervention'])  # TODO remove this line once we have InterventionStnd column

# Filter out nonsense products
df_merged = df_merged.filter(df_merged['RETAILER_ITEM_ID'].isNotNull())

# Filter out products with no interventions
pdf_mean_intervention = df_merged.select('RETAILER_ITEM_ID', 'is_intervention').groupby('RETAILER_ITEM_ID').mean().toPandas()
pdf_mean_intervention = pdf_mean_intervention[pdf_mean_intervention['avg(is_intervention)'] > 0]
allowed_item_set = set(pdf_mean_intervention['RETAILER_ITEM_ID'])
df_merged = df_merged.filter(df_merged['RETAILER_ITEM_ID'].isin(allowed_item_set))

# Filter out useless colunms
# Baseline
predictor_cols = ['ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID']
predictor_cols += ['WEEK_SEASONALITY', 'YEAR_SEASONALITY']
predictor_cols += ['DOW']
predictor_cols += [toy for toy in df_merged.columns if 'TIME_OF_YEAR' in toy]  # Radial basis functions time feature
predictor_cols += ['InterventionStnd']  # TODO NOTE this may no be the final column name
predictor_cols = list(set(predictor_cols))

# Expected
forecast_cols = predictor_cols.copy()
forecast_cols += ['RECENT_ON_HAND_INVENTORY_QTY', 'PRICE', 'SNAPINDEX', 'NONSNAPINDEX']
forecast_cols += get_lag_column_name(x=[i * 7 for i in range(1, 4)])  # lags quesi loess features
forecast_cols += get_lag_column_name(x=range(1, 7))  # necessary lags
forecast_cols = list(set(forecast_cols))

# Order columns
predictor_cols.sort()
forecast_cols.sort()

extra_cols = [
    'POS_ITEM_QTY', 'is_intervention', 'SALES_DT',
    'InterventionId', 'CallfileVisitId', 'InterventionGroup', 'DiffDay',
    'RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID'
]
select_cols = list(set(extra_cols + predictor_cols + forecast_cols))
select_cols.sort()
print(select_cols)

df_merged = df_merged.select(*select_cols)

# Cast types to to float
cat_feautres_list = ['ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID', 'DOW', 'InterventionStnd']
for col_name, col_type in df_merged.dtypes:
    if (col_type == 'bigint' or col_type == 'long' or col_type == 'double') and col_name not in cat_feautres_list:
        df_merged = df_merged.withColumn(
            col_name,
            df_merged[col_name].cast('float')
        )

print(f'{df_merged.cache().count():,}')

# COMMAND ----------

display(df_merged)

# COMMAND ----------

print('Intervention Summary Statistics')
df_merged.select('is_intervention').describe().show()

# COMMAND ----------

# Write data
df_merged.write.format('delta').mode('overwrite').save(f'/mnt/processed/temp/cache/{run_id}')  # TODO RENAME PATH
