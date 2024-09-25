# Databricks notebook source
import uuid
import warnings
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
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

data_vault_data_1 = data_vault_data.where(col("SALES_DT") >= lit("2018-01-01"))

# COMMAND ----------

min_date, max_date = data_vault_data_1.select(min('SALES_DT'), max('SALES_DT')).first()

# COMMAND ----------

all_dates_with_a_record = data_vault_data_1.select('SALES_DT').distinct()

# COMMAND ----------

list_of_all_dates_with_a_record = all_dates_with_a_record.select('SALES_DT').collect()

list_of_all_dates_with_a_record

# COMMAND ----------

mvv = all_dates_with_a_record.select('SALES_DT').rdd.flatMap(lambda x: x).collect()

mvv_list = list(
   all_dates_with_a_record.select('SALES_DT').toPandas()['mvv']
)

# COMMAND ----------

mvv

# COMMAND ----------

mvv_list

# COMMAND ----------

all_dates_with_a_record.count()

# COMMAND ----------

min_date, max_date = data_vault_data1.select(min('SALES_DT'), max('SALES_DT')).first()

diff = max_date - min_date

total_number_of_possible_days = diff.days + 1

# COMMAND ----------

total_number_of_possible_days

# COMMAND ----------

dateList = [min_date + timedelta(i) for i in range(total_number_of_possible_days)]

# COMMAND ----------

dateList

# COMMAND ----------

for date in datelist:
    if date not in 
