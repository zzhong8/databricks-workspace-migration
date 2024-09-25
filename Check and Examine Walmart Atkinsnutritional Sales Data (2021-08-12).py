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

sqlContext.setConf("spark.sql.shuffle.partitions", "800")

# COMMAND ----------

dbutils.widgets.text('retailer', 'walmart', 'Retailer')
dbutils.widgets.text('client', 'clorox', 'Client')
dbutils.widgets.text('country_code', 'us', 'Country Code')

dbutils.widgets.text('store', '', 'Organization Unit Num')
dbutils.widgets.text('item', '', 'Retailer Item ID')
dbutils.widgets.text('runid', '', 'Run ID')
dbutils.widgets.text('timestamp', current_timestamp, 'Timestamp')

dbutils.widgets.text('source_system', 'retaillink', 'Source System')

RETAILER = dbutils.widgets.get('retailer').strip().lower()
CLIENT = dbutils.widgets.get('client').strip().lower()
COUNTRY_CODE = dbutils.widgets.get('country_code').strip().lower()

RUN_ID = dbutils.widgets.get('runid').strip()
TIMESTAMP = dbutils.widgets.get('timestamp').strip()

SOURCE_SYSTEM = dbutils.widgets.get('source_system').strip()

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

for param in [SOURCE_SYSTEM, RETAILER, CLIENT, COUNTRY_CODE, STORE, ITEM, TIMESTAMP, RUN_ID]:
    print(param)

# COMMAND ----------

# %sql
# select min(sales_dt), max(sales_dt)
# from retaillink_walmart_barillaamericainc_us_dv.vw_latest_sat_epos_summary

# COMMAND ----------

# Read POS data
data_vault_data = read_pos_data(SOURCE_SYSTEM, RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')

# COMMAND ----------

# Let's only look as April 2021 onwards
data = data_vault_data.where(((col("SALES_DT") < lit("2022-02-01")) & (col("SALES_DT") >= lit("2020-11-01"))) & (col("POS_ITEM_QTY") >= 0))

# COMMAND ----------

if STORE:
    data = data.filter(f'ORGANIZATION_UNIT_NUM == "{STORE}"')

if ITEM:
    data = data.filter(f'RETAILER_ITEM_ID == "{ITEM}"')

# COMMAND ----------

data = data.orderBy('SALES_DT', ascending=False)

# COMMAND ----------

sales_dates = data.select('SALES_DT').distinct().orderBy('SALES_DT', ascending=False)

# COMMAND ----------

display(sales_dates)

# COMMAND ----------

display(data)

# COMMAND ----------

# Try a particular store and item:

STORE = '2677'
ITEM = '576475476'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use retaillink_walmart_gpconsumerproductsoperation_us_dv;
# MAGIC
# MAGIC select retailer_item_id, retailer_item_desc from sat_retailer_item
# MAGIC where (retailer_item_id = 576475475 or retailer_item_id = 576475476 or retailer_item_id = 576928659)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use retaillink_walmart_gpconsumerproductsoperation_us_dv;
# MAGIC
# MAGIC select organization_unit_num, organization_unit_nm from sat_organization_unit
# MAGIC where (organization_unit_num = 2677 or organization_unit_num = 5926 or organization_unit_num = 7728 or organization_unit_num = 9316)

# COMMAND ----------

# Let's only look as April 2021 onwards
data = data_vault_data.where((col("SALES_DT") < lit("2021-12-31")) & (col("SALES_DT") >= lit("2021-04-01")))

# COMMAND ----------

if STORE:
    data = data.filter(f'ORGANIZATION_UNIT_NUM == "{STORE}"')

if ITEM:
    data = data.filter(f'RETAILER_ITEM_ID == "{ITEM}"')

# COMMAND ----------

data = data.orderBy('SALES_DT', ascending=False)

# COMMAND ----------

display(data)

# COMMAND ----------

# # Replace negative POS_ITEM_QTY and POS_AMT values with 0
# data_vault_data = data_vault_data.withColumn("POS_ITEM_QTY", pyf.when(pyf.col("POS_ITEM_QTY") >= 0, pyf.col("POS_ITEM_QTY")).otherwise(0))
# data_vault_data = data_vault_data.withColumn("POS_AMT", pyf.when(pyf.col("POS_AMT") >= 0, pyf.col("POS_AMT")).otherwise(0))

# COMMAND ----------

# Let's only look as April 2021 onwards
data_debug = data_vault_data.where((col("SALES_DT") < lit("2021-05-21")) & (col("SALES_DT") >= lit("2021-05-01")))

# COMMAND ----------

ITEM = '9222706'

data_debug = data_debug.filter(f'RETAILER_ITEM_ID == "{ITEM}"')

# COMMAND ----------

data_debug = data_debug.orderBy('SALES_DT', ascending=False)

# COMMAND ----------

display(data_debug)

# COMMAND ----------


