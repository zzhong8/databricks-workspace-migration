# Databricks notebook source
import uuid
import warnings
import numpy as np
import pandas as pd
from datetime import datetime
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

dbutils.widgets.text('retailer', 'walmart', 'Retailer')
dbutils.widgets.text('client', 'clorox', 'Client')
dbutils.widgets.text('countrycode', 'us', 'Country Code')

RETAILER = dbutils.widgets.get('retailer').strip().lower()
CLIENT = dbutils.widgets.get('client').strip().lower()
COUNTRY_CODE = dbutils.widgets.get('countrycode').strip().lower()

if RETAILER == '':
    raise ValueError('\'retailer\' is a required parameter.  Please provide a value.')

if CLIENT == '':
    raise ValueError('\'client\' is a required parameter.  Please provide a value.')

if COUNTRY_CODE  == '':
    raise ValueError('\'countrycode\' is a required parameter.  Please provide a value.')

for param in [RETAILER, CLIENT, COUNTRY_CODE]:
    print(param)

# COMMAND ----------

# Read POS data
data_vault_data = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')

# COMMAND ----------

data_vault_data1 = data_vault_data.where((col("SALES_DT") < lit("2019-07-01")) & (col("SALES_DT") >= lit("2018-07-01")))

# COMMAND ----------

data_vault_data1.select('POS_ITEM_QTY').groupBy().sum().collect()

# COMMAND ----------


