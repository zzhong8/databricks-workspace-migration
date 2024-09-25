# Databricks notebook source
import uuid
import numpy as np
import pandas as pd

import seaborn as sns
import matplotlib.pyplot as graph

from pyspark.sql import functions as pyf
from pyspark.sql import types as pyt


from datetime import datetime as dtm

# Models
from catboost import CatBoostRegressor

# Acosta.Alerting package imports
from acosta.alerting.helpers.features import get_lag_column_name
from acosta.alerting.training import distributed_model_fit, ALERT_TRAINING_SCHEMA_LIST
from acosta.alerting.helpers import path_exists

import acosta
print(acosta.__version__)

# COMMAND ----------

sqlContext.setConf('spark.executor.cores', '8')

# COMMAND ----------

dbutils.widgets.text('retailer', 'WALMART', 'Retailer')
dbutils.widgets.text('client', 'CLOROX', 'Client')
dbutils.widgets.text('store', '', 'Organization Unit Num')
dbutils.widgets.text('item', '', 'Retailer Item ID')
dbutils.widgets.text('runid', '', 'Run ID')

RETAILER = dbutils.widgets.get('retailer').strip().upper()
CLIENT = dbutils.widgets.get('client').strip().upper()
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

if RUN_ID == '':
    RUN_ID = str(uuid.uuid4())

# PATHS
PATH_RESULTS_OUTPUT = '/mnt/artifacts/training_results/retailer={retailer}/client={client}/'.format(
    retailer=RETAILER,
    client=CLIENT
)
PATH_ENGINEERED_FEATURES_OUTPUT = '/mnt/processed/training/{run_id}/engineered/'.format(run_id=RUN_ID)
print(RUN_ID)

# COMMAND ----------

# Check if the path exists otherwise raise error.
path_exists(PATH_ENGINEERED_FEATURES_OUTPUT, 'delta', 'raise')

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
subset_meets_threshold = loaded_data.select('RETAILER', 'CLIENT', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM',
                                            'POS_ITEM_QTY') \
    .filter('POS_ITEM_QTY > 0') \
    .groupBy('RETAILER', 'CLIENT', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM') \
    .count() \
    .filter('count >= 84') \
    .drop('count')

data_over_threshold = loaded_data.join(
    subset_meets_threshold,
    ['RETAILER', 'CLIENT', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'],
    'inner'
)

# COMMAND ----------

# Select columns to be included in data frame
lags_to_include = get_lag_column_name(x=range(1, 8))
lags_quesi_loess = get_lag_column_name(x=[i*7 for i in range(1, 4)])
dynamic_intercepts = [c for c in data_over_threshold.columns if 'TIME_OF_YEAR' in c]
holidays_only = [c for c in data_over_threshold.columns if 'HOLIDAY' in c and "_LAG_" not in c and "_LEAD_" not in c]

# Columns for the model to use
predictor_cols = ['RECENT_ON_HAND_INVENTORY_QTY', 'PRICE', 'SNAPINDEX', 'NONSNAPINDEX']
predictor_cols += ['ORGANIZATION_UNIT_NUM']
predictor_cols += lags_quesi_loess
predictor_cols += dynamic_intercepts
predictor_cols += ['WEEK_SEASONALITY', 'YEAR_SEASONALITY', 'DOW']
predictor_cols += holidays_only
predictor_cols += lags_to_include[:-1]

mandatory_fields = ["POS_ITEM_QTY", "SALES_DT", "RETAILER", "CLIENT", "RETAILER_ITEM_ID"]
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

data_partitioned = data_with_final_features.repartition(800, 'ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID')

print('N Partitions', data_partitioned.rdd.getNumPartitions())

# COMMAND ----------

# Create instances of training units
algorithm_list = [
    CatBoostRegressor(
        iterations=10000,
        learning_rate=0.1,
        loss_function='RMSE',
        cat_features=['DOW', 'ORGANIZATION_UNIT_NUM'],
        use_best_model=True,
        early_stopping_rounds=25,
        verbose=True
    ),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Training

# COMMAND ----------

print('Training initiated at {}'.format(dtm.now().strftime('%Y%m%d:%H:%M')))

#  A basic overview of trained_models_rdd construction:
# .map: Change RDD of rows into K,V pairs of identifiers and features as dict (same row count)
# .reduceByKey: For common Key (store,UPC): append daily features as a list of dicts (fewer rows, 1 per store, item)
# .mapValues: Passing list of dictionaries (features) to a function that will be constructed into a Pandas DF
#     ^-- This now is doing work on a single node for an individual item, store.
#
# The end result is an rdd containing the _results_history list of dictionaries containing
# all training units' results and the identifier and model object for the best training unit.

trained_models_rdd = data_partitioned.rdd \
    .map(lambda r: ((r.RETAILER_ITEM_ID), [r.asDict()])) \
    .reduceByKey(lambda accum, r: accum + r) \
    .mapValues(lambda x: distributed_model_fit(
        x_names=predictor_cols,
        y_name='POS_ITEM_QTY',
        df=x,
        test_size=0.1,
        training_unit_list=algorithm_list
    ))

# .flatMapValue is taking a 3D object - stack of training results, and flattens to a list of individual runs
# with repeating key entries
# Having an extraction function will simplify the extraction of tuple elements + dictionary values
# into the results columns.
# This would be equivalent to having a json result written and parsing it in.
model_performance = trained_models_rdd \
    .flatMapValues(lambda entry: entry) \
    .map(lambda entry: entry[1])

# Jump of out rdd's and back into Spark Dataframes
# The columns must be sorted because the distributed_train function unpacks 
# the dictionary into a row object.  When a dictionary is unpacked into a 
# Row it sorts the keys for backward compatibility with older verions of python
model_performance = spark.createDataFrame(
    model_performance,
    pyt.StructType(sorted(ALERT_TRAINING_SCHEMA_LIST, key=lambda x: x.name))
)

# Write out the dataframe to parquet for reference
model_performance \
    .select(*[c.name for c in ALERT_TRAINING_SCHEMA_LIST]) \
    .withColumn('DATE_MODEL_TRAINED', pyf.current_timestamp()) \
    .withColumn('TRAINING_ID', pyf.lit(RUN_ID)) \
    .write \
    .mode('append') \
    .format('parquet') \
    .save(PATH_RESULTS_OUTPUT)

print('Training ended at {}'.format(dtm.now().strftime('%Y%m%d:%H:%M')))

# COMMAND ----------

model_runs_meta = spark.read.parquet(PATH_RESULTS_OUTPUT) \
    .filter('TRAINING_ID == "{run_id}"'.format(run_id=RUN_ID))

# COMMAND ----------

print('Trained Model Count: {:,}'.format(model_runs_meta.count()))
print('Output Model Count: {:,}'.format(model_runs_meta.filter('MODEL_ID is not null').count()))

# COMMAND ----------

df = model_runs_meta.filter('MODEL_ID is not null') \
    .select('METRICS', 'ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID') \
    .withColumn('R2_train', pyf.col('METRICS.r2_train')) \
    .withColumn('R2_test', pyf.col('METRICS.r2_test')) \
    .toPandas()

graph.title('Training Performance: {:,} Models'.format(len(df)))
sns.distplot(df['R2_train'], color='seagreen')
graph.xlabel('$R^2$')
display(graph.show())
graph.close()

# COMMAND ----------

graph.title('Testing Performance: {:,} Models'.format(len(df)))
sns.distplot(df['R2_test'])
graph.xlabel('$R^2$')
display(graph.show())
graph.close()

# COMMAND ----------

display(
    model_runs_meta.select(
        'TRAINING_ID', 'ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID', 'MODEL_ID', 'MODEL_METADATA',
        pyf.size(pyf.col('COLUMN_NAMES')).alias('COLUMN_COUNT'),
        'TEST_NON_ZERO_DAYS', 'TEST_DAYS_COUNT', 'METRICS'
    )
)
