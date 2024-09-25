# Databricks notebook source
import copy
import uuid
import numpy as np
import pandas as pd
from scipy import stats

import matplotlib.pyplot as graph
import seaborn as sns

import sklearn.linear_model as lm
from sklearn.metrics import r2_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelBinarizer
# from sklearn_pandas import DataFrameMapper

from catboost import CatBoostRegressor, CatBoostClassifier

from pygam.utils import b_spline_basis

from datetime import timedelta
from time import time

from pyspark.sql import functions as pyf
from pyspark.sql import types as pyt

from datetime import datetime as dtm

# Acosta.Alerting package imports
import acosta
from acosta.alerting.helpers import check_path_exists
from acosta.alerting.helpers.features import get_lag_column_name

print(acosta.__version__)
# print(pyarrow.__version__)

# from causalgraphicalmodels import CausalGraphicalModel

graph.style.use('fivethirtyeight')

def print_shape(a):
    print(' : '.join([f'{x:,}' for x in a.shape]))

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

# COMMAND ----------

product_number_list_path = '/mnt/artifacts/hugh/kraftheinz-intervention-data/ranked_item_by_iv_top_80%_tesco.csv'
# product_number_list_path = '/mnt/artifacts/hugh/kraftheinz-intervention-data/ranked_item_by_iv_18%+_tesco.csv'

check_path_exists(product_number_list_path, 'csv', 'raise')

product_number_list_df_info = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load(product_number_list_path)

product_number_list = product_number_list_df_info.select('ProductNumber')

product_numbers = product_number_list.rdd.flatMap(lambda x: x).collect()

for product_number in product_numbers:
#     if int(product_number) != 6300633:
#         continue

    print(product_number)
    
    # PATHS (new!)
    PATH_RESULTS_INPUT = '/mnt/processed/measurement_csv/retailer={retailer}/client={client}/country_code={country_code}/run_id={run_id}/retailer_item_id={retailer_item_id}'.format(
        retailer=RETAILER,
        client=CLIENT,
        country_code=COUNTRY_CODE,
        run_id=RUN_ID,
        retailer_item_id=product_number
    )

#     print(PATH_RESULTS_INPUT)
    
    # Filtering of data set
    df_features = spark.read.format('csv').option('header', 'true').load(PATH_RESULTS_INPUT)
    
    if df_features.count() > 0:

        # PATHS (new!)
        PATH_RESULTS_OUTPUT = '/mnt/processed/measurement_csv_coalesced/retailer={retailer}/client={client}/country_code={country_code}/run_id={run_id}/retailer_item_id={retailer_item_id}'.format(
            retailer=RETAILER,
            client=CLIENT,
            country_code=COUNTRY_CODE,
            run_id=RUN_ID,
            retailer_item_id=product_number
        )

        df_features.coalesce(1)\
            .write.format('com.databricks.spark.csv')\
            .option('header', 'true')\
            .mode('overwrite')\
            .save(PATH_RESULTS_OUTPUT)

# COMMAND ----------


