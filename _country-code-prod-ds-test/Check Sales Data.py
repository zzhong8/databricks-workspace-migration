# Databricks notebook source
import uuid
import warnings
import numpy as np
import pandas as pd
from datetime import datetime, date
from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark.sql import types as pyt
from pyspark.sql import functions as pyf
import acosta
from acosta.alerting.preprocessing import read_pos_data
from acosta.alerting.helpers import check_path_exists

print(acosta.__version__)
auto_model_prefix = 'model'
current_timestamp = datetime.now().strftime('%Y-%m-%d')

# COMMAND ----------

sqlContext.setConf("spark.sql.shuffle.partitions", "800")

# COMMAND ----------

dbutils.widgets.text('retailer', 'walmart', 'Retailer')
dbutils.widgets.text('client', 'clorox', 'Client')
dbutils.widgets.text('country_code', 'us', 'Country Code')

RETAILER = dbutils.widgets.get('retailer').strip().lower()
CLIENT = dbutils.widgets.get('client').strip().lower()
COUNTRY_CODE = dbutils.widgets.get('country_code').strip().lower()

if RETAILER == '':
    raise ValueError('\'retailer\' is a required parameter.  Please provide a value.')

if CLIENT == '':
    raise ValueError('\'client\' is a required parameter.  Please provide a value.')

for param in [RETAILER, CLIENT, COUNTRY_CODE]:
    print(param)

# COMMAND ----------

# Read POS data
data_vault_data = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')

# COMMAND ----------

sales_dates = data_vault_data.select('SALES_DT').distinct().orderBy('SALES_DT', ascending=False)

# COMMAND ----------

sales_dates.count()

# COMMAND ----------

display(sales_dates)

# COMMAND ----------

data_vault_data.count()

# COMMAND ----------

display(data_vault_data)

# COMMAND ----------

# This filter requires at least 84 days of non-zero sales in the entire datset
subset_meets_threshold = data_vault_data\
    .select('RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM', 'POS_ITEM_QTY') \
    .filter('POS_ITEM_QTY > 0') \
    .groupBy('RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM') \
    .count() \
    .filter('count >= 84') \
    .drop('count')

data_to_be_trained = data_vault_data.join(
    subset_meets_threshold,
    ['RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'],
    'inner'
)

# COMMAND ----------

retail_item_ids_to_be_trained = data_to_be_trained.select('RETAILER_ITEM_ID').distinct()

retail_item_ids_to_be_trained.count()

# COMMAND ----------

organization_unit_nums_to_be_trained = data_to_be_trained.select('ORGANIZATION_UNIT_NUM').distinct()

organization_unit_nums_to_be_trained.count()

# COMMAND ----------


