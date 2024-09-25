# Databricks notebook source
import uuid
import numpy as np
import pandas as pd

from time import time

# import seaborn as sns
# import matplotlib.pyplot as graph

from pyspark.sql import functions as pyf
from pyspark.sql import types as pyt

from datetime import datetime as dtm

# Acosta.Alerting package imports
from acosta.alerting.helpers import check_path_exists
from acosta.alerting.helpers.features import get_lag_column_name
# from acosta.alerting.training import distributed_model_fit, get_partial_distributed_train_func, \
#     training_schema_col_name_list, TRAINING_SCHEMA_LIST

import acosta
from acosta.alerting.helpers import check_path_exists
# import pyarrow

print(acosta.__version__)
# print(pyarrow.__version__)

# COMMAND ----------

dbutils.widgets.text('retailer', 'walmart', 'Retailer')
dbutils.widgets.text('client', 'clorox', 'Client')
dbutils.widgets.text('countrycode', 'us', 'Country Code')

dbutils.widgets.text('item', '', 'Retailer Item ID')
dbutils.widgets.text('runid', '', 'Run ID')

RETAILER = dbutils.widgets.get('retailer').strip().lower()
CLIENT = dbutils.widgets.get('client').strip().lower()
COUNTRY_CODE  = dbutils.widgets.get('countrycode').strip().lower()

RUN_ID = dbutils.widgets.get('runid').strip()

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
PATH_RESULTS_OUTPUT = '/mnt/processed/measurement_csv/retailer={retailer}/client={client}/country_code={country_code}/run_id={run_id}'.format(
    retailer=RETAILER,
    client=CLIENT,
    country_code=COUNTRY_CODE,
    run_id=RUN_ID
)

PATH_ENGINEERED_FEATURES_OUTPUT = '/mnt/processed/training/{run_id}/engineered/'.format(run_id=RUN_ID)
print('{}/retailer_item_id={}'.format(PATH_RESULTS_OUTPUT, ITEM))

# COMMAND ----------

check_path_exists(PATH_ENGINEERED_FEATURES_OUTPUT, file_format='delta', errors='raise')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Setup

# COMMAND ----------

# Filtering of data set
loaded_data = spark.read.format('delta').load(PATH_ENGINEERED_FEATURES_OUTPUT)

# COMMAND ----------

# product_number_list_path = '/mnt/artifacts/hugh/kraftheinz-intervention-data/ranked_item_by_iv_top_80%_asda_and_tesco.csv'
# product_number_list_path = '/mnt/artifacts/hugh/kraftheinz-intervention-data/ranked_item_by_iv_top_80%_morrisons.csv'
product_number_list_path = '/mnt/artifacts/hugh/kraftheinz-intervention-data/ranked_item_by_iv_top_80%_sainsburys.csv'
# product_number_list_path = '/mnt/artifacts/hugh/kraftheinz-intervention-data/ranked_item_by_iv_80%+_tesco.csv'

check_path_exists(product_number_list_path, 'csv', 'raise')

# COMMAND ----------

product_number_list_df_info = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load(product_number_list_path)

# COMMAND ----------

loaded_data = loaded_data.withColumn('RETAILER_ITEM_ID', loaded_data['RETAILER_ITEM_ID'].cast(pyt.IntegerType()))

# COMMAND ----------

product_number_list_df_info = product_number_list_df_info.filter('Retailer == "{}"'.format(RETAILER))

product_number_list = product_number_list_df_info.select('ProductNumber')

loaded_data_filtered = loaded_data.join(
    product_number_list,
    loaded_data.RETAILER_ITEM_ID == product_number_list.ProductNumber,
    'inner'
)

# COMMAND ----------

# loaded_data_filtered = loaded_data

# COMMAND ----------

# This filter requires at least 84 days of non-zero sales in the entire dataset
subset_meets_threshold = loaded_data_filtered.select('RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM',
                                            'POS_ITEM_QTY') \
    .filter('POS_ITEM_QTY > 0') \
    .groupBy('RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM') \
    .count() \
    .filter('count >= 84') \
    .drop('count')

data_over_threshold = loaded_data_filtered.join(
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

data_over_threshold.count()

# COMMAND ----------

# display(data_over_threshold.filter('RETAILER_ITEM_ID <= "0999999"'))

# COMMAND ----------

data_over_threshold.dtypes

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

# # Adding log log to non binary columns
# # Note: series is un-log-transformed before performing calculations in mase.py in the package
# #       POS_ITEM_QTY is un-log-transformed as BASELINE_POS_ITEM_QTY in 03.1 - Inference Driver
# columns_to_be_log_transformed = ['POS_ITEM_QTY', 'RECENT_ON_HAND_INVENTORY_QTY'] \
#                                 + ['WEEK_SEASONALITY', 'YEAR_SEASONALITY'] \
#                                 + lags_to_include \
#                                 + lags_quesi_loess

# for column_name in columns_to_be_log_transformed:
#     data_with_final_features = data_with_final_features.withColumn(
#         column_name,
#         pyf.signum(column_name) * pyf.log(pyf.abs(pyf.col(column_name)) + pyf.lit(1))
#     )

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

print('{:,}'.format(data_partitioned.count()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save To CSV

# COMMAND ----------

# product_numbers = data_over_threshold.select('RETAILER_ITEM_ID').orderBy(data_over_threshold.RETAILER_ITEM_ID).dropDuplicates().rdd.flatMap(lambda x: x).collect()
product_numbers = product_number_list.select('ProductNumber').rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

# for product_number in product_numbers:
#     print(product_number)

# COMMAND ----------

data_partitioned.cache()

# COMMAND ----------

if RETAILER == 'tesco':
    num_files_to_generate = 4   
else:
    num_files_to_generate = 1

for product_number in product_numbers:
    print(product_number)
    
    data_partitioned_filtered = data_partitioned.filter('RETAILER_ITEM_ID == "{}"'.format(product_number))

    data_partitioned_filtered.coalesce(num_files_to_generate)\
        .write.format('com.databricks.spark.csv')\
        .option('header', 'true')\
        .mode('overwrite')\
        .save('{}/retailer_item_id={}'.format(PATH_RESULTS_OUTPUT, product_number))

# COMMAND ----------


