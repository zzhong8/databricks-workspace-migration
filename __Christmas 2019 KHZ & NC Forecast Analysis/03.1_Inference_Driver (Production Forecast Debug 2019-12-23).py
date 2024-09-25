# Databricks notebook source
# MAGIC %md
# MAGIC # Inference Driver

# COMMAND ----------

import pyspark.sql.types as pyt
import pyspark.sql.functions as pyf
from pyspark.sql.types import *
import numpy as np
import pandas as pd
import datetime

import pickle

# Possible Models
from sklearn.linear_model import LinearRegression, LassoCV, RidgeCV, BayesianRidge, Ridge, ElasticNet, Lasso
from catboost import CatBoostRegressor
from xgboost import XGBRegressor

# Use grid search for each of the models and use either 5 or 10 fold CV
# (if grid search is taking too long then use fewer params and/or fewer param values)
from sklearn.model_selection import GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

# Acosta.Alerting package imports
from acosta.alerting.preprocessing import pos_to_training_data, read_pos_data
from acosta.alerting.forecast import distributed_predict, prediction_schema
from acosta.alerting.helpers.features import get_lag_column_name
from acosta.alerting.helpers import check_path_exists, universal_encoder, universal_decoder

import acosta

print(acosta.__version__)

# COMMAND ----------

# Interface
dbutils.widgets.text('RETAILER_PARAM', 'walmart', 'Retailer')
dbutils.widgets.text('CLIENT_PARAM', 'clorox', 'Client')
dbutils.widgets.text('COUNTRY_CODE_PARAM', 'us', 'Country Code')

dbutils.widgets.text('LAST_DAY_TO_PREDICT_PARAM', '', 'Last Day to Predict (YYYYMMDD)')
dbutils.widgets.text('NUM_DAYS_TO_PREDICT_PARAM', '7', 'Number of Days to Predict')
dbutils.widgets.text('MAX_LAG', '30', 'Maximum Days to Lag')

dbutils.widgets.dropdown('ENVIRONMENT', 'dev', ['dev', 'prod'], 'Environment')
dbutils.widgets.dropdown('MODEL_SOURCE', 'local', ['local', 'prod'], 'Model Source')
dbutils.widgets.dropdown('INCLUDE_DISCOUNT_FEATURES', 'no', ['yes', 'no'], 'Include Discount Features')

# Process input parameters
RETAILER = dbutils.widgets.get('RETAILER_PARAM').strip().lower()
CLIENT = dbutils.widgets.get('CLIENT_PARAM').strip().lower()
COUNTRY_CODE = dbutils.widgets.get('COUNTRY_CODE_PARAM').strip().lower()

if len(RETAILER) == 0 or len(CLIENT) == 0 or len(COUNTRY_CODE) == 0:
    raise ValueError('Client, Retailer and Country Code must be filled in.')

LAST_PREDICTED_DATE = datetime.datetime.strptime(dbutils.widgets.get('LAST_DAY_TO_PREDICT_PARAM'), '%Y%m%d')
DAYS_TO_PREDICT = int(dbutils.widgets.get('NUM_DAYS_TO_PREDICT_PARAM'))
MINIMUM_DAYS_TO_LAG = int(dbutils.widgets.get('MAX_LAG'))
ENVIRONMENT = dbutils.widgets.get('ENVIRONMENT').upper()
MODEL_SOURCE = dbutils.widgets.get('MODEL_SOURCE').upper()

ENVIRONMENT = 'PROD' if ENVIRONMENT.startswith('PROD') else 'DEV'
MODEL_SOURCE = 'LOCAL' if MODEL_SOURCE.startswith('LOCAL') else 'PROD'

input_discount_string = dbutils.widgets.get('INCLUDE_DISCOUNT_FEATURES').strip().lower()
INCLUDE_DISCOUNT_FEATURES = 'y' in input_discount_string or 't' in input_discount_string

# Constants TODO make this useless
DATE_FIELD = 'SALES_DT'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and preprocess data

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Check to see if the cross-join table exists

# COMMAND ----------

df_predictions = df_distributed_input.groupby('RETAILER_ITEM_ID').apply(distributed_predict)
print('{:,}'.format(df_predictions.cache().count()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process and export results

# COMMAND ----------

# rename columns and add additional metadata to the predictions
prediction_results_dataframe = df_predictions.selectExpr(
    'ORGANIZATION_UNIT_NUM',
    'RETAILER_ITEM_ID',
    'CURRENT_TIMESTAMP() as LOAD_TS',
    '"Dynamic.Retail.Forecast.Engine" as RECORD_SOURCE_CD',
    'exp(PREDICTIONS)-1 as BASELINE_POS_ITEM_QTY',
    'SALES_DT',
    'MODEL_ID'
)

# LOOK UP HASH KEYS
databaseName = '{}_{}_{}_dv'.format(RETAILER.lower(), CLIENT.lower(), COUNTRY_CODE.lower())
itemMasterTableName = '{}.hub_retailer_item'.format(databaseName)
storeMasterTableName = '{}.hub_organization_unit'.format(databaseName)

prediction_results_dataframe = prediction_results_dataframe.alias('PRDF') \
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

prediction_results_dataframe \
    .select('HUB_ORGANIZATION_UNIT_HK', 'HUB_RETAILER_ITEM_HK', 'LOAD_TS', 'RECORD_SOURCE_CD', 'BASELINE_POS_ITEM_QTY',
            'MODEL_ID', 'SALES_DT') \
    .write.mode('overwrite').insertInto(insertTableName, overwrite=True)
