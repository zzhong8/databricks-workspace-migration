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

print(acosta.__version__)
auto_model_prefix = 'model'
current_timestamp = datetime.now().strftime('%Y-%m-%d')

# COMMAND ----------

dbutils.widgets.text('retailer', 'walmart', 'Retailer')
dbutils.widgets.text('client', 'clorox', 'Client')
dbutils.widgets.text('store', '', 'Organization Unit Num')
dbutils.widgets.text('item', '', 'Retailer Item ID')
dbutils.widgets.text('runid', '', 'Run ID')
dbutils.widgets.text('timestamp', current_timestamp, 'Timestamp')

RETAILER = dbutils.widgets.get('retailer').strip().lower()
CLIENT = dbutils.widgets.get('client').strip().lower()
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

if RUN_ID == '':
    RUN_ID = str(uuid.uuid4())
elif RUN_ID.lower() == 'auto':
    RUN_ID = '-'.join([auto_model_prefix, CLIENT, TIMESTAMP])

# PATHS
PATH_DATA_VAULT_TRANSFORM_OUTPUT = '/mnt/processed/training/{run_id}/data_vault/'.format(run_id=RUN_ID)

for param in [RETAILER, CLIENT, STORE, ITEM, TIMESTAMP, RUN_ID]:
    print(param)

if CLIENT.lower() not in RUN_ID.lower():
    warnings.warn('Potentially uninformative RUN_ID, {} is not in the RUN_ID'.format(CLIENT))

# COMMAND ----------

# Generate and save snap index for the data set
database = '{retailer}_{client}_dv'.format(retailer=RETAILER, client=CLIENT)
print('Accessing', database)

store_names_query = '''
    SELECT SOU.ORGANIZATION_UNIT_NM, HOU.ORGANIZATION_UNIT_NUM
       FROM {database}.sat_organization_unit SOU 
       INNER JOIN {database}.hub_organization_unit HOU 
       ON SOU.HUB_ORGANIZATION_UNIT_HK = HOU.HUB_ORGANIZATION_UNIT_HK
    '''

store_names_query = store_names_query.format(database=database)
store_names = spark.sql(store_names_query)

print(store_names.count())

mapped_stores_states = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load('/mnt/prod-ro/artifacts/reference/orgunitnum_to_state.csv')

stores_with_state = store_names\
    .join(mapped_stores_states, store_names.ORGANIZATION_UNIT_NUM == mapped_stores_states.ORGANIZATION_UNIT_NUM, 'left_outer')\
    .select(store_names['ORGANIZATION_UNIT_NUM'], mapped_stores_states['STATE'])

# Puerto Rico currently does not have a SNAP pay-cycle profile - use DEFAULT profile
stores_with_state = stores_with_state.withColumn("STATE", when(col("STATE") == "PR", "DEFAULT").otherwise(col("STATE")))

# Replace non-matches with DEFAULT profile
stores_with_state = stores_with_state.withColumn("STATE", when(col("STATE").isNull(), "DEFAULT").otherwise(col("STATE")))

# Remove any duplicates and take the first state alphabetically
windowSpec = Window.partitionBy('ORGANIZATION_UNIT_NUM').orderBy('STATE')
unique_store_names = stores_with_state\
    .withColumn('column_rank', row_number().over(windowSpec)) \
    .filter('column_rank == 1')\
    .drop('column_rank')

# TODO: Instead of reading from CSV, calculate these values
pay_cycles = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load('/mnt/prod-ro/artifacts/reference/pay_cycle_profiles_by_state.csv')

stores_with_pc = unique_store_names\
    .join(pay_cycles, unique_store_names.STATE == pay_cycles.STATE)\
    .select(unique_store_names['ORGANIZATION_UNIT_NUM'], pay_cycles['*'])

print(stores_with_pc.count() / 31, "stores")

# COMMAND ----------

snap_pay_cycle_path = '/mnt/artifacts/reference/snap_paycycle/retailer={retailer}/client={client}/'
stores_with_pc.write.mode('overwrite').parquet(snap_pay_cycle_path.format(retailer=RETAILER, client=CLIENT))

# COMMAND ----------

# Read POS data and filter
data_vault_data = read_pos_data(RETAILER, CLIENT, sqlContext).filter('POS_ITEM_QTY >= 0').repartition('SALES_DT')

# COMMAND ----------

data_vault_data.columns

# COMMAND ----------

data_vault_data.count()

# COMMAND ----------

from py4j.protocol import Py4JJavaError

def path_exist(path):
    try:
        rdd = sc.textFile(path)
        rdd.take(1)
        return True
      
    except Py4JJavaError as e:
        return False

# COMMAND ----------

# Import approved product list for this client
approved_product_list_reference_path = '/mnt/artifacts/reference/approved_product_list/retailer={retailer}/client={client}/'

if path_exist(approved_product_list_reference_path.format(retailer=RETAILER, client=CLIENT)):
    approved_product_list = spark.read.format("delta").load(approved_product_list_reference_path.format(retailer=RETAILER, client=CLIENT)).select(["RetailProductCode"])
    
    data_vault_data = data_vault_data.join(approved_product_list, data_vault_data.RETAILER_ITEM_ID == approved_product_list.RetailProductCode)
    
else:
    print("Approved product list not found for client")

# COMMAND ----------

print("Got here")

# COMMAND ----------

data_vault_data.count()

# COMMAND ----------

# # Save the data set
# data_vault_data \
#     .write.mode('overwrite').format('parquet') \
#     .save(PATH_DATA_VAULT_TRANSFORM_OUTPUT)
