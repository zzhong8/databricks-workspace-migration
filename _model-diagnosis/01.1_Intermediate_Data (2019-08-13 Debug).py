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
dbutils.widgets.text('store', '', 'Organization Unit Num')
dbutils.widgets.text('item', '', 'Retailer Item ID')
dbutils.widgets.text('runid', '', 'Run ID')
dbutils.widgets.text('timestamp', current_timestamp, 'Timestamp')

RETAILER = dbutils.widgets.get('retailer').strip().lower()
CLIENT = dbutils.widgets.get('client').strip().lower()
COUNTRY_CODE = dbutils.widgets.get('countrycode').strip().lower()

RUN_ID = dbutils.widgets.get('runid').strip()
TIMESTAMP = dbutils.widgets.get('timestamp').strip()

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

if COUNTRY_CODE  == '':
    raise ValueError('\'countrycode\' is a required parameter.  Please provide a value.')

if RUN_ID == '':
    RUN_ID = str(uuid.uuid4())
elif RUN_ID.lower() == 'auto':
    RUN_ID = '-'.join([auto_model_prefix, CLIENT, TIMESTAMP])

# PATHS
PATH_DATA_VAULT_TRANSFORM_OUTPUT = '/mnt/processed/training/{run_id}/data_vault/'.format(run_id=RUN_ID)

for param in [RETAILER, CLIENT, COUNTRY_CODE, STORE, ITEM, TIMESTAMP, RUN_ID]:
    print(param)

if CLIENT.lower() not in RUN_ID.lower():
    warnings.warn('Potentially uninformative RUN_ID, {} is not in the RUN_ID'.format(CLIENT))

# COMMAND ----------

# Check if the input files exist and if not raise a value error
store_states_names = '/mnt/prod-ro/artifacts/country_code/reference/orgunitnum_to_state.csv'
pay_cycles_path = '/mnt/prod-ro/artifacts/country_code/reference/pay_cycle_profiles_by_state.csv'

check_path_exists(store_states_names, 'csv', 'raise')
check_path_exists(pay_cycles_path, 'csv', 'raise')

snap_pay_cycle_path = '/mnt/artifacts/country_code/reference/snap_paycycle/retailer={retailer}/client={client}/country_code={country_code}/'\
    .format(retailer=RETAILER, client=CLIENT, country_code=COUNTRY_CODE)

# COMMAND ----------

# Read POS data
data_vault_data = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')

# COMMAND ----------

data_vault_data_2 = data_vault_data.filter('SALES_DT >= "2019-06-01"')

# COMMAND ----------

display(data_vault_data)

# COMMAND ----------

df_agg = data_vault_data.filter('RETAILER_ITEM_ID == 6385908').groupBy('SALES_DT').agg(pyf.avg("POS_ITEM_QTY").alias('AVG_POS_ITEM_QTY')).orderBy(["SALES_DT"], ascending=True)

# COMMAND ----------

display(df_agg)

# COMMAND ----------

data_vault_data_2 = data_vault_data.filter('SALES_DT >= "2019-06-01"')

# COMMAND ----------

display(data_vault_data_2)

# COMMAND ----------

# Replace negative POS_ITEM_QTY and POS_AMT values with 0
data_vault_data = data_vault_data.withColumn('POS_ITEM_QTY', pyf.when(pyf.col('POS_ITEM_QTY') >= 0, pyf.col('POS_ITEM_QTY')).otherwise(0))
data_vault_data = data_vault_data.withColumn('POS_AMT', pyf.when(pyf.col('POS_AMT') >= 0, pyf.col('POS_AMT')).otherwise(0))

# COMMAND ----------

# Import approved product list for this client
approved_product_list_reference_path = '/mnt/artifacts/country_code/reference/approved_product_list/retailer={retailer}/client={client}/country_code={country_code}/'\
    .format(retailer=RETAILER, client=CLIENT, country_code=COUNTRY_CODE)

# If the approved product list exists for this client
if check_path_exists(approved_product_list_reference_path, 'delta', 'ignore'):
    approved_product_list = spark.read.format('delta').load(approved_product_list_reference_path)\
        .select(['RetailProductCode'])
    
    data_vault_data = data_vault_data.join(
        approved_product_list,
        data_vault_data.RETAILER_ITEM_ID == approved_product_list.RetailProductCode)
    
else:
    print('Approved product list not found for client')

# COMMAND ----------

# Save the data set
data_vault_data \
    .write.mode('overwrite').format('delta') \
    .save(PATH_DATA_VAULT_TRANSFORM_OUTPUT)
