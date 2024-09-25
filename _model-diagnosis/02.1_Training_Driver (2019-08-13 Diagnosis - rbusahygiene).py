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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Training

# COMMAND ----------

train_results = spark.read.format('delta').load(PATH_RESULTS_OUTPUT)

# COMMAND ----------

display(train_results.select('MODEL_ID', 'METRICS_R2_TEST', 'METRICS_RMSE_TEST'))

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

# COMMAND ----------


