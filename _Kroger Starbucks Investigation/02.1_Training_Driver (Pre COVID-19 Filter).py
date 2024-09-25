# Databricks notebook source
import uuid
import numpy as np
import pandas as pd

from time import time

import seaborn as sns
import matplotlib.pyplot as graph

from pyspark.sql import functions as pyf
from pyspark.sql import types as pyt

from datetime import datetime as dtm

# Models
from catboost import CatBoostRegressor

# Acosta.Alerting package imports
from acosta.alerting.helpers import universal_encoder, universal_decoder, check_path_exists
from acosta.alerting.helpers.features import get_lag_column_name
from acosta.alerting.training import distributed_model_fit, get_partial_distributed_train_func, \
    training_schema_col_name_list, TRAINING_SCHEMA_LIST

import acosta
import pyarrow

print(acosta.__version__)
print(pyarrow.__version__)

# COMMAND ----------

dbutils.widgets.text('retailer', 'walmart', 'Retailer')
dbutils.widgets.text('client', 'clorox', 'Client')
dbutils.widgets.text('countrycode', 'us', 'Country Code')

dbutils.widgets.text('store', '', 'Organization Unit Num')
dbutils.widgets.text('item', '', 'Retailer Item ID')
dbutils.widgets.text('runid', '', 'Run ID')

RETAILER = dbutils.widgets.get('retailer').strip().lower()
CLIENT = dbutils.widgets.get('client').strip().lower()
COUNTRY_CODE  = dbutils.widgets.get('countrycode').strip().lower()

RUN_ID = dbutils.widgets.get('runid').strip()

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

if RUN_ID == '':
    RUN_ID = str(uuid.uuid4())

# PATHS (new!)
PATH_RESULTS_OUTPUT = '/mnt/artifacts/country_code/training_results/retailer={retailer}/client={client}/country_code={country_code}/'.format(
    retailer=RETAILER,
    client=CLIENT,
    country_code=COUNTRY_CODE
)

PATH_ENGINEERED_FEATURES_OUTPUT = '/mnt/processed/training/{run_id}/engineered/'.format(run_id=RUN_ID)
print(RUN_ID)

# COMMAND ----------

check_path_exists(PATH_ENGINEERED_FEATURES_OUTPUT, file_format='delta', errors='raise')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Setup

# COMMAND ----------

# Filtering of data set
loaded_data = spark.read.format('delta').load(PATH_ENGINEERED_FEATURES_OUTPUT)

if STORE:
    loaded_data = loaded_data.filter('ORGANIZATION_UNIT_NUM == "{}"'.format(STORE))
if ITEM:
    loaded_data = loaded_data.filter('RETAILER_ITEM_ID == "{}"'.format(ITEM))

# This filter requires at least 84 days of non-zero sales in the entire dataset
subset_meets_threshold = loaded_data.select('RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM',
                                            'POS_ITEM_QTY') \
    .filter('POS_ITEM_QTY > 0') \
    .groupBy('RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM') \
    .count() \
    .filter('count >= 84') \
    .drop('count')

data_over_threshold = loaded_data.join(
    pyf.broadcast(subset_meets_threshold),
    ['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'],
    'leftsemi'
)
print(data_over_threshold.rdd.getNumPartitions())

# COMMAND ----------

data_over_threshold = data_over_threshold.fillna({
    'ON_HAND_INVENTORY_QTY': 0,
    'RECENT_ON_HAND_INVENTORY_QTY': 0,
    'RECENT_ON_HAND_INVENTORY_DIFF': 0
})

# COMMAND ----------

# Select columns to be included in data frame
lags_to_include = get_lag_column_name(x=range(1, 8))
lags_quesi_loess = get_lag_column_name(x=[i * 7 for i in range(1, 4)])
dynamic_intercepts = [c for c in data_over_threshold.columns if 'TIME_OF_YEAR' in c]
holidays_only = [c for c in data_over_threshold.columns if 'HOLIDAY' in c and '_LAG_' not in c and '_LEAD_' not in c]

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

data_with_final_features = data_over_threshold.select(*select_columns).dropna()

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

# MAGIC %md
# MAGIC ## Model Training

# COMMAND ----------

# Create instances of training units
algorithm_list = [
    CatBoostRegressor(
        iterations=10000,
        learning_rate=0.3,
        loss_function='RMSE',
        cat_features=['DOW', 'ORGANIZATION_UNIT_NUM'],
        use_best_model=True,
        early_stopping_rounds=25,
        verbose=True
    ),
]

# COMMAND ----------

train_func = get_partial_distributed_train_func(
    x_names=predictor_cols,
    y_name='POS_ITEM_QTY',
    test_size=0.1,
    algorithm_list=algorithm_list
)

@pyf.pandas_udf(pyt.StructType(TRAINING_SCHEMA_LIST), pyf.PandasUDFType.GROUPED_MAP)
def train_udf(df):
    return train_func(df=df)


# COMMAND ----------

# Train models with UDF
train_results = data_partitioned.groupby('RETAILER_ITEM_ID').apply(train_udf)
print(train_results.cache().count())

# COMMAND ----------

display(train_results.select('MODEL_ID', 'METRICS_R2_TEST', 'METRICS_RMSE_TEST'))

# COMMAND ----------

# Save model results
train_results \
    .select(*training_schema_col_name_list) \
    .withColumn('DATE_MODEL_TRAINED', pyf.current_timestamp()) \
    .withColumn('TRAINING_ID', pyf.lit(RUN_ID)) \
    .write.mode('overwrite') \
    .format('delta') \
    .save(PATH_RESULTS_OUTPUT)

# COMMAND ----------

# MAGIC %md
# MAGIC # Results Summary

# COMMAND ----------

print('Trained Model Count: {:,}'.format(train_results.count()))
print('Output Model Count: {:,}'.format(train_results.filter('MODEL_ID is not null').count()))

# COMMAND ----------

df = train_results.filter('MODEL_ID is not null') \
    .select('RETAILER_ITEM_ID', 'METRICS_R2_TEST', 'METRICS_R2_TRAIN', 'METRICS_RMSE_TEST', 'METRICS_RMSE_TRAIN') \
    .toPandas()

graph.title('Training Performance: {:,} Models'.format(len(df)))
sns.distplot(df['METRICS_R2_TRAIN'], kde=False, color='seagreen')
graph.xlabel('$R^2$')
display(graph.show())
graph.close()

# COMMAND ----------

graph.title('Testing Performance: {:,} Models'.format(len(df)))
sns.distplot(df['METRICS_R2_TEST'], kde=False)
graph.xlabel('$R^2$')
display(graph.show())
graph.close()

# COMMAND ----------

graph.title('Training Performance: {:,} Models'.format(len(df)))
sns.distplot(df['METRICS_RMSE_TRAIN'], kde=False, color='seagreen')
graph.xlabel('$RMSE$ $(Units)$')
display(graph.show())
graph.close()

# COMMAND ----------

graph.title('Testing Performance: {:,} Models'.format(len(df)))
sns.distplot(df['METRICS_RMSE_TEST'], kde=False)
graph.xlabel('$RMSE$ $(Units)$')
display(graph.show())
graph.close()
