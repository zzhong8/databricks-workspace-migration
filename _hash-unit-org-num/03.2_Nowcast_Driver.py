# Databricks notebook source
# MAGIC %md
# MAGIC # Nowcast Driver
# MAGIC
# MAGIC A notebook for running inference on historical data.

# COMMAND ----------

import uuid
import numpy as np
import pandas as pd

from time import time

import seaborn as sns
import matplotlib.pyplot as graph

from pyspark.sql import functions as pyf
from pyspark.sql import types as pyt

from datetime import datetime
from datetime import date

# Models
from catboost import CatBoostRegressor

# Acosta.Alerting package imports
from acosta.alerting.helpers import universal_encoder, universal_decoder, check_path_exists
from acosta.alerting.helpers.features import get_lag_column_name
from acosta.alerting.training import distributed_model_fit, get_partial_distributed_train_func, \
    training_schema_col_name_list, TRAINING_SCHEMA_LIST

from acosta.alerting.forecast import inference_schema, inference_schema_col_names, distributed_inference

import acosta
import pyarrow

print(acosta.__version__)
print(pyarrow.__version__)

auto_model_prefix = 'data'

is_nowcast = True

current_timestamp = datetime.now().strftime('%Y-%m-%d')

# COMMAND ----------

dbutils.widgets.text('retailer', 'walmart', 'Retailer')
dbutils.widgets.text('client', 'clorox', 'Client')
dbutils.widgets.text('countrycode', 'us', 'Country Code')

dbutils.widgets.text('store', '', 'Organization Unit Num')
dbutils.widgets.text('item', '', 'Retailer Item ID')
dbutils.widgets.text('runid', 'auto', 'Run ID')

dbutils.widgets.dropdown('ENVIRONMENT', 'prod', ['dev', 'prod'], 'Environment')
dbutils.widgets.dropdown('MODEL_SOURCE', 'prod', ['local', 'prod'], 'Model Source')

dbutils.widgets.text('start_date', '', 'Start Date (YYYYMMDD)')
dbutils.widgets.text('end_date', '', 'End Date (YYYYMMDD)')

# COMMAND ----------

RETAILER = dbutils.widgets.get('retailer').strip().lower()
CLIENT = dbutils.widgets.get('client').strip().lower()
COUNTRY_CODE = dbutils.widgets.get('countrycode').strip().lower()

START_DATE = datetime.strptime(dbutils.widgets.get('start_date'), '%Y%m%d')
END_DATE = datetime.strptime(dbutils.widgets.get('end_date'), '%Y%m%d')
RUN_ID = dbutils.widgets.get('runid').strip()

MODEL_SOURCE = dbutils.widgets.get('MODEL_SOURCE').upper()
MODEL_SOURCE = 'LOCAL' if MODEL_SOURCE.startswith('LOCAL') else 'PROD'

INCLUDE_DISCOUNT_FEATURES = False

ENVIRONMENT = dbutils.widgets.get('ENVIRONMENT').upper()
ENVIRONMENT = 'PROD' if ENVIRONMENT.startswith('PROD') else 'DEV'

try:
    STORE = int(dbutils.widgets.get('store').strip())
except ValueError:
    STORE = None
try:
    ITEM = str(dbutils.widgets.get('item').strip())
except ValueError:
    ITEM = None

if RETAILER == '':
    raise ValueError('\'retailer\' is a required parameter.  Please provide a value.')

if CLIENT == '':
    raise ValueError('\'client\' is a required parameter.  Please provide a value.')

if COUNTRY_CODE == '':
    raise ValueError('\'countrycode\' is a required parameter. Please provide a value.')
    
if START_DATE == '':
    raise ValueError('\'start_date\' is a required parameter. Please provide a value.')
    
if END_DATE == '':
    raise ValueError('\'end_date\' is a required parameter. Please provide a value.')

if RUN_ID == '':
    RUN_ID = '-'.join([auto_model_prefix, RETAILER, CLIENT, COUNTRY_CODE, END_DATE.strftime('%Y-%m-%d')])

# elif RUN_ID.lower() == 'auto':
#     RUN_ID = '-'.join([auto_model_prefix, RETAILER, CLIENT, COUNTRY_CODE, '(' + END_DATE.strftime('%Y-%m-%d') + ')', str((END_DATE - START_DATE).days + 1), 'days'])
    
elif RUN_ID.lower() == 'auto':
    RUN_ID = '-'.join([auto_model_prefix, RETAILER, CLIENT, COUNTRY_CODE, END_DATE.strftime('%Y-%m-%d')])

# PATHS (new!)
PATH_RESULTS_OUTPUT = '/mnt{mod}/artifacts/country_code/training_results/retailer={retailer}/client={client}/country_code={country_code}/'.format(
    mod='' if MODEL_SOURCE == 'LOCAL' else '/prod-ro',
    retailer=RETAILER,
    client=CLIENT,
    country_code=COUNTRY_CODE
)

PATH_ENGINEERED_FEATURES_OUTPUT = '/mnt{mod}/processed/training/{run_id}/engineered/'.format(
    mod='' if MODEL_SOURCE == 'LOCAL' else '/prod-ro',
    run_id=RUN_ID
)

print(RUN_ID)

# COMMAND ----------

# MAGIC %run "./01.1_Data_Preprocessing"

# COMMAND ----------

check_path_exists(PATH_ENGINEERED_FEATURES_OUTPUT, file_format='delta', errors='raise')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Setup

# COMMAND ----------

# Filtering of data set
loaded_data = spark.read.format('delta').load(PATH_ENGINEERED_FEATURES_OUTPUT)

if STORE:
    loaded_data = loaded_data.filter('ORGANIZATION_UNIT_NUM == "{}"'.format(STORE))
if ITEM:
    loaded_data = loaded_data.filter('RETAILER_ITEM_ID == "{}"'.format(ITEM))

print('N = {:,}'.format(loaded_data.count()))

# COMMAND ----------

# Select columns to be included in data frame
lags_to_include = get_lag_column_name(x=range(1, 8))
lags_quesi_loess = get_lag_column_name(x=[i * 7 for i in range(1, 4)])
dynamic_intercepts = [c for c in loaded_data.columns if 'TIME_OF_YEAR' in c]
holidays_only = [c for c in loaded_data.columns if 'HOLIDAY' in c and '_LAG_' not in c and '_LEAD_' not in c]

# Columns for the model to use
predictor_cols = ['RECENT_ON_HAND_INVENTORY_QTY', 'PRICE', 'SNAPINDEX', 'NONSNAPINDEX']
predictor_cols += ['ORGANIZATION_UNIT_NUM']
predictor_cols += lags_quesi_loess
predictor_cols += dynamic_intercepts
predictor_cols += ['WEEK_SEASONALITY', 'YEAR_SEASONALITY', 'DOW']
# predictor_cols += holidays_only
predictor_cols += lags_to_include[:-1]

mandatory_fields = ['POS_ITEM_QTY', 'SALES_DT', 'RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID']
select_columns = mandatory_fields + predictor_cols

loaded_data = loaded_data.fillna({
    'ON_HAND_INVENTORY_QTY': 0,
    'RECENT_ON_HAND_INVENTORY_QTY': 0,
    'RECENT_ON_HAND_INVENTORY_DIFF': 0
})

# COMMAND ----------

data_with_final_features = loaded_data.select(*select_columns).dropna()

# Now that we have the lag variables, filter the dataset on the actual start date
data_with_final_features = data_with_final_features.where(pyf.col("SALES_DT") >= START_DATE)

# Adding log log to non binary columns
# Note: series is un-log-transformed before performing calculations in mase.py in the package
#       POS_ITEM_QTY is un-log-transformed as BASELINE_POS_ITEM_QTY in 03.1 - Inference Driver
columns_to_be_log_transformed = ['POS_ITEM_QTY', 'RECENT_ON_HAND_INVENTORY_QTY'] \
                                + ['WEEK_SEASONALITY', 'YEAR_SEASONALITY'] \
                                + lags_to_include \
                                + lags_quesi_loess

for column_name in columns_to_be_log_transformed:
    data_with_final_features = data_with_final_features.withColumn(
        column_name,
        pyf.signum(column_name) * pyf.log(pyf.abs(pyf.col(column_name)) + pyf.lit(1))
    )

# Convert long (integers) to some kind of float
for col_name, col_type in data_with_final_features.dtypes:
    if (col_type == 'bigint' or col_type == 'long') and col_name != 'ORGANIZATION_UNIT_NUM':
        data_with_final_features = data_with_final_features.withColumn(
            col_name,
            data_with_final_features[col_name].cast('float')
        )

# Repartition N_cpus x N_Workers =
data_partitioned = data_with_final_features.repartition(64 * 16, 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM')
print('N Partitions', data_partitioned.rdd.getNumPartitions())

# COMMAND ----------

# TODO Add a good comment for this cells
compress_pos_data_schema = pyt.StructType([
    pyt.StructField('RETAILER', pyt.StringType()),
    pyt.StructField('CLIENT', pyt.StringType()),
    pyt.StructField('COUNTRY_CODE', pyt.StringType()),
    pyt.StructField('RETAILER_ITEM_ID', pyt.StringType()),
    pyt.StructField('DATAFRAME_PICKLE', pyt.StringType())
])
compress_pos_data_cols = [col.name for col in compress_pos_data_schema]

@pyf.pandas_udf(compress_pos_data_schema, pyf.PandasUDFType.GROUPED_MAP)
def compress_pos_data(df):
    return pd.DataFrame(
        [[
            df['RETAILER'].unique()[0],
            df['CLIENT'].unique()[0],
            df['COUNTRY_CODE'].unique()[0],
            df['RETAILER_ITEM_ID'].unique()[0],
            universal_encoder(df, True)
        ]],
        columns=compress_pos_data_cols
    )

# Try this function out
df_compressed = data_partitioned.groupby('RETAILER_ITEM_ID').apply(compress_pos_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading Champion Models

# COMMAND ----------

champions_path = '/mnt{mod}/artifacts/country_code/champion_models/retailer={retailer}/client={client}/country_code={country_code}'.format(
    mod='' if MODEL_SOURCE == 'LOCAL' else '/prod-ro',
    retailer=RETAILER,
    client=CLIENT,
    country_code=COUNTRY_CODE
)

check_path_exists(champions_path, 'delta')

champions = spark.read.format('delta') \
    .load(champions_path) \
    .drop('RETAILER', 'CLIENT', 'COUNTRY_CODE') \
    .withColumnRenamed('ORGANIZATION_UNIT_NUM', 'ORGANIZATION_UNIT_LIST')

# COMMAND ----------

champions.cache().count(), df_compressed.cache().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Inference

# COMMAND ----------

df_distributed_input = df_compressed.join(champions, ['RETAILER_ITEM_ID'], 'inner')
print('{:,}'.format(df_distributed_input.cache().count()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Results

# COMMAND ----------

df_inferred = df_distributed_input.groupby('RETAILER_ITEM_ID').apply(distributed_inference)

# Select expression to match inference table
df_results = df_inferred.selectExpr(
    'ORGANIZATION_UNIT_NUM',
    'RETAILER_ITEM_ID',
    'CURRENT_TIMESTAMP() as LOAD_TS',
    '"Dynamic.Retail.Forecast.Engine" as RECORD_SOURCE_CD',
    'exp(PREDICTIONS)-1 as BASELINE_POS_ITEM_QTY',
    'SALES_DT',
    'MODEL_ID'
)

print('{:,}'.format(df_results.cache().count()))

# COMMAND ----------

# Looking up important hash keys
databaseName = '{}_{}_{}_dv'.format(RETAILER.lower(), CLIENT.lower(), COUNTRY_CODE.lower())
itemMasterTableName = '{}.hub_retailer_item'.format(databaseName)
storeMasterTableName = '{}.hub_organization_unit'.format(databaseName)

df_results = df_results.alias('PRDF') \
    .join(sqlContext.read.table(storeMasterTableName).alias('OUH'),
          pyf.col('OUH.ORGANIZATION_UNIT_NUM') == pyf.col('PRDF.ORGANIZATION_UNIT_NUM'), 'inner') \
    .join(sqlContext.read.table(itemMasterTableName).alias('RIH'),
          pyf.col('RIH.RETAILER_ITEM_ID') == pyf.col('PRDF.RETAILER_ITEM_ID'), 'inner') \
    .select('PRDF.*', 'OUH.HUB_ORGANIZATION_UNIT_HK', 'RIH.HUB_RETAILER_ITEM_HK')

# COMMAND ----------

# Insert into table rather than blob
# The Hive table definition will determine format, partitioning, and location of the data file...
# so we don't have to worry about those low-level details
insertDatabaseName = databaseName
if ENVIRONMENT == 'DEV':
    insertDatabaseName = 'RETAIL_FORECAST_ENGINE'
elif ENVIRONMENT == 'PROD':
    insertDatabaseName = '{}_{}_{}_retail_alert_im'.format(RETAILER.lower(), CLIENT.lower(), COUNTRY_CODE.lower())

insertTableName = '{}.DRFE_FORECAST_BASELINE_UNIT'.format(insertDatabaseName)

# COMMAND ----------

df_results \
    .select('HUB_ORGANIZATION_UNIT_HK', 'HUB_RETAILER_ITEM_HK', 'LOAD_TS', 'RECORD_SOURCE_CD', 'BASELINE_POS_ITEM_QTY',
            'MODEL_ID', 'SALES_DT') \
    .write.mode('overwrite').insertInto(insertTableName, overwrite=True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Results Summary
# MAGIC
# MAGIC (TODO modify if you want)

# COMMAND ----------

# TODO Plots?
