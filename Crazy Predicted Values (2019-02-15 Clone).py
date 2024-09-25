# Databricks notebook source
import uuid
import warnings
import pandas as pd
import numpy as np
from pyspark.sql import functions as pyf

import pyspark.sql.types as pyt

# COMMAND ----------

# MAGIC %sql
# MAGIC --outlier in prediction
# MAGIC SELECT *
# MAGIC FROM
# MAGIC (SELECT 
# MAGIC       *,
# MAGIC       ROW_NUMBER() OVER(partition BY 
# MAGIC                               HUB_ORGANIZATION_UNIT_HK,
# MAGIC                               HUB_RETAILER_ITEM_HK,
# MAGIC                               SALES_DT
# MAGIC                           ORDER BY LOAD_TS
# MAGIC                           DESC
# MAGIC                           )AS ROW_NUMBER 
# MAGIC FROM walmart_dole_retail_alert_im.DRFE_FORECAST_BASELINE_UNIT) DFBU
# MAGIC WHERE DFBU.ROW_NUMBER = 1
# MAGIC AND DFBU.BASELINE_POS_ITEM_QTY > 100
# MAGIC ORDER BY BASELINE_POS_ITEM_QTY DESC

# COMMAND ----------

# MAGIC %md
# MAGIC # Crazy prediction problem
# MAGIC
# MAGIC Some of the models are making _crazy_ predictions. This appears to be entirely caused by something on the last few days on models that have small datasets. Below is an example of pathological problems, afterwards I'll explain what's happening.

# COMMAND ----------

import pickle
import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LassoCV
from sklearn.metrics import r2_score
from sklearn.preprocessing import StandardScaler, MinMaxScaler

import pyspark.sql.functions as pyf

import acosta
from acosta.alerting.helpers.recipefile import PrioritizedFeatureSelection
from acosta.alerting.helpers.features import get_day_of_week_column_names, get_lag_column_name, get_day_of_month_column_names

import matplotlib.pyplot as graph
import seaborn as sns

print(acosta.__version__)

retailer = 'walmart'.upper()
client = 'dole'.upper()
model_id = 'b20c5390-00cb-41d0-83f3-67927a9fcbe5'

PATH_RESULTS_OUTPUT = '/mnt/artifacts/training_results/retailer={}/client={}/'.format(retailer, client)

# COMMAND ----------

meta_data = spark.read.parquet(PATH_RESULTS_OUTPUT).filter('MODEL_ID == "{}"'.format(model_id))

display(meta_data)

# COMMAND ----------

meta_df = meta_data.toPandas()

# COMMAND ----------

data_bunch = meta_df['DATASETS'].values[0]
print(data_bunch.keys())
print(len(data_bunch['pred_train']))
print(len(data_bunch['pred_test']))

print('{:.4%}'.format(r2_score(np.exp(data_bunch['train']), np.exp(data_bunch['pred_train']))))

for actual, predicted in zip(data_bunch['test'], data_bunch['pred_test']):
  print('{:,.1f} vs {:,.1f}'.format(np.exp(actual), np.exp(predicted)))

# COMMAND ----------

model = pickle.loads(meta_df['MODEL_OBJECT'].values[0])
col_names = meta_df['COLUMN_NAMES'].values[0]

print(model)

print()
print('alpha', model.alpha_)
print(col_names)
print(model.coef_)
print(model.intercept_)
print()

for name, coef in zip(col_names, model.coef_):
  print('{:.4f} {}'.format(coef, name))

# COMMAND ----------

print(model.named_steps['scaler'].mean_)
print(model.named_steps['scaler'].var_)

for name, var in zip(col_names, model.named_steps['scaler'].var_):
  print(var, name)

# COMMAND ----------

# MAGIC %md
# MAGIC What's happening is this. The training dataset does not see the whole year, therefore there are some radial basis functions that are not centered in training set. This means the range of values of these radial basis functions are some very small numbers, eg: (1e-50, 1e-24). When the model pipeline scaler `StandardScaler()` step hits these features it computers some very small variance and therefore, in the test set when/if it starts to encounter the dates where the mass of the radial basis function begins to increase (remember a number that increases expontial until it peaks) then the increase is exaggregated enormously by standardising with such a microscopic variance. 
# MAGIC
# MAGIC The solution here would be to remove all seasonality variables from standard scaling and only `MinMaxScale()` them between 0 and 1. 
# MAGIC
# MAGIC To test this I'm going to run to training process and then see if I can make a pipeline that can do this for us.

# COMMAND ----------

organization_num, retailer_item_num = meta_df[['ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID']].values[0]
print(organization_num, retailer_item_num)

# COMMAND ----------

PATH_ENGINEERED_FEATURES_OUTPUT = '/mnt/processed/training/{run_id}/engineered/'.format(run_id=meta_df['TRAINING_ID'].values[0])
print(PATH_ENGINEERED_FEATURES_OUTPUT)

# COMMAND ----------

model_data = spark.read.parquet(PATH_ENGINEERED_FEATURES_OUTPUT)\
  .filter('ORGANIZATION_UNIT_NUM == {} and RETAILER_ITEM_ID == {}'.format(organization_num, retailer_item_num))

above_threshold_data = model_data.select('RETAILER', 'CLIENT', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM', 'POS_ITEM_QTY')\
  .filter('POS_ITEM_QTY > 0')\
  .groupBy('RETAILER', 'CLIENT', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM')\
  .count()\
  .filter('count > 84')\
  .drop('count')

data_over_threshold = model_data.join(
  above_threshold_data,
  ['RETAILER', 'CLIENT', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'],
  'inner'
)

print(model_data.count())
print(above_threshold_data.count())
print(data_over_threshold.count())

# Create 'Recipe' of columns to be added in priority order
list_of_cols = [
    ['RECENT_ON_HAND_INVENTORY_QTY', 'SNAPINDEX', 'NONSNAPINDEX'],
    ['WEEK_SEASONALITY', 'YEAR_SEASONALITY'],  # Basic seasonality columns
    [c for c in data_over_threshold.columns if 'TIME_OF_YEAR' in c],  # Dynamic intercepts
    [c for c in data_over_threshold.columns if 'AMPLITUDE' in c],   # Dynamic weekly amplitude
    [c for c in data_over_threshold.columns if 'HOLIDAY' in c and "_LAG_" not in c and "_LEAD_" not in c]
    # Week and month seasonality handles these. When we start using LMMs then these will be more helpful
]

recipe = PrioritizedFeatureSelection(list_of_cols)
priority_list = recipe.subset_by_priority()

mandatory_fields = ["POS_ITEM_QTY", "SALES_DT", "RETAILER", "CLIENT", "ORGANIZATION_UNIT_NUM", "RETAILER_ITEM_ID"]
lags_to_include = get_lag_column_name(x=range(1, 8))

_select_columns = mandatory_fields + priority_list + lags_to_include

data_with_final_features = data_over_threshold.select(*_select_columns).dropna()

# Adding log log to non binary columns
# Note: series is un-log-transformed before performing calculations in mase.py in the package
#       POS_ITEM_QTY is un-log-transformed as BASELINE_POS_ITEM_QTY in 03.1 - Inference Driver
columns_to_be_log_transformed = ["POS_ITEM_QTY", "RECENT_ON_HAND_INVENTORY_QTY"] \
    + ['WEEK_SEASONALITY', 'YEAR_SEASONALITY'] \
    + [c for c in data_over_threshold.columns if 'TIME_OF_YEAR' in c] \
    + [c for c in data_over_threshold.columns if 'AMPLITUDE' in c] \
    + lags_to_include

for column_name in columns_to_be_log_transformed:
    data_with_final_features = data_with_final_features.withColumn(
        column_name,
        pyf.signum(column_name) * pyf.log(pyf.abs(pyf.col(column_name)) + pyf.lit(1))
    )

data = data_with_final_features#.repartition(800, 'ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID')

# COMMAND ----------

display(data)

# COMMAND ----------

df = data.toPandas().sort_values(by='SALES_DT')
print(df.head())

# COMMAND ----------

print(df.describe())

# COMMAND ----------

# Data preprocessing
def timeseries_train_test_split(d):
  n = len(df)
  test_size = 0.2

  train, test = d.head(round(n * (1-test_size))), d.tail(round(n * test_size))

  print(n, len(train), len(test))
  assert len(train) + len(test) == n, 'Bad train test split'

  y_train, y_test = train['POS_ITEM_QTY'].values, test['POS_ITEM_QTY'].values
  x_train, x_test = train[col_names], test[col_names]
  return x_train, x_test, y_train, y_test

x_train, x_test, y_train, y_test = timeseries_train_test_split(df)
print(x_train.shape, x_test.shape, y_train.shape, y_test.shape)

# COMMAND ----------

# Simple Model
simple_model = model
simple_model.fit(x_train, y_train)

# AWESOME we have reproduced the error!
print(simple_model.score(x_train, y_train))
print(simple_model.score(x_test, y_test))

for true, pred in zip(y_test, simple_model.predict(x_test)):
  print('{:.1f} v {:.1f}'.format(np.exp(true), np.exp(pred)))

# COMMAND ----------

print(col_names[1:])

# COMMAND ----------

# Seasonality is not standard scaled
x_smart_train, x_smart_test = x_train.copy(), x_test.copy()
seasonality_col_names = col_names[1:]

smart_model = LassoCV(eps=0.001, cv=10)
smart_model.fit(x_smart_train, y_train)

print(smart_model.score(x_smart_train, y_train))
print(smart_model.score(x_smart_test, y_test))

for true, pred in zip(y_test, smart_model.predict(x_smart_test)):
  print('{:.1f} v {:.1f}'.format(np.exp(true), np.exp(pred)))

# COMMAND ----------


