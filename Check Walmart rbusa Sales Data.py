# Databricks notebook source
from pprint import pprint
import warnings
from datetime import datetime
from pyspark.sql import Window
import pyspark.sql.functions as pyf
import acosta
import pyarrow
from acosta.alerting.preprocessing import read_pos_data, pos_to_training_data
from acosta.alerting.helpers import check_path_exists

print(acosta.__version__)
print(pyarrow.__version__)

current_timestamp = datetime.now().strftime('%Y-%m-%d')

# COMMAND ----------

dbutils.widgets.text('source_system', 'retaillink', 'Source System')

dbutils.widgets.text('retailer', 'walmart', 'Retailer')
dbutils.widgets.text('client', 'clorox', 'Client')
dbutils.widgets.text('country_code', 'us', 'Country Code')

dbutils.widgets.text('store', '', 'Organization Unit Num')
dbutils.widgets.text('item', '', 'Retailer Item ID')

dbutils.widgets.text('run_id', '', 'Run ID')
dbutils.widgets.text('timestamp', current_timestamp, 'Timestamp')

dbutils.widgets.dropdown('MODEL_SOURCE', 'local', ['local', 'prod'], 'Model Source')
dbutils.widgets.dropdown('INCLUDE_DISCOUNT_FEATURES', 'No', ['Yes', 'No'], 'Include Discount Features')

SOURCE_SYSTEM = dbutils.widgets.get('source_system').strip().lower()

RETAILER = dbutils.widgets.get('retailer').strip().lower()
CLIENT = dbutils.widgets.get('client').strip().lower()
COUNTRY_CODE = dbutils.widgets.get('country_code').strip().lower()

RUN_ID = dbutils.widgets.get('run_id').strip()
TIMESTAMP = dbutils.widgets.get('timestamp').strip()

MODEL_SOURCE = dbutils.widgets.get('MODEL_SOURCE').upper()
MODEL_SOURCE = 'LOCAL' if MODEL_SOURCE.startswith('LOCAL') else 'PROD'
INCLUDE_DISCOUNT_FEATURES = dbutils.widgets.get('INCLUDE_DISCOUNT_FEATURES').strip().lower() == 'yes'

try:
    STORE = int(dbutils.widgets.get('store').strip())
except ValueError:
    STORE = None
try:
    ITEM = str(dbutils.widgets.get('item').strip())
except ValueError:
    ITEM = None

if SOURCE_SYSTEM == '':
    raise ValueError('\'source_system\' is a required parameter.  Please provide a value.')

if RETAILER == '':
    raise ValueError('\'retailer\' is a required parameter.  Please provide a value.')

if CLIENT == '':
    raise ValueError('\'client\' is a required parameter.  Please provide a value.')

if COUNTRY_CODE == '':
    raise ValueError('\'country_code\' is a required parameter. Please provide a value.')

if RUN_ID == '':
    RUN_ID = '-'.join([auto_model_prefix, RETAILER, CLIENT, COUNTRY_CODE, TIMESTAMP])

elif RUN_ID.lower() == 'auto':
    RUN_ID = '-'.join([auto_model_prefix, RETAILER, CLIENT, COUNTRY_CODE, TIMESTAMP])

# PATHS
PATH_ENGINEERED_FEATURES_OUTPUT = '/mnt/processed/training/{run_id}/engineered/'.format(run_id=RUN_ID)

for param in [SOURCE_SYSTEM, RETAILER, CLIENT, COUNTRY_CODE, STORE, ITEM, RUN_ID]:
    print(param)

if CLIENT.lower() not in RUN_ID.lower():
    warnings.warn('Potentially uninformative RUN_ID, {} is not in the RUN_ID'.format(CLIENT))

# COMMAND ----------

# Read POS data
data_vault_data = read_pos_data(SOURCE_SYSTEM, RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')

pprint(dict(data_vault_data.dtypes))

# COMMAND ----------

data_vault_data1 = data_vault_data.where(pyf.col("SALES_DT") >= pyf.lit("2021-01-01"))

# COMMAND ----------

display(data_vault_data1)

# COMMAND ----------

organization_unit_nums = data_vault_data1.select('ORGANIZATION_UNIT_NUM').distinct().orderBy('ORGANIZATION_UNIT_NUM', ascending=False)

# COMMAND ----------

organization_unit_nums.count()

# COMMAND ----------

retailer_item_ids = data_vault_data1.select('RETAILER_ITEM_ID').distinct().orderBy('RETAILER_ITEM_ID', ascending=False)

# COMMAND ----------

retailer_item_ids.count()

# COMMAND ----------

sales_dates = data_vault_data.select('SALES_DT').distinct().orderBy('SALES_DT', ascending=False)

# COMMAND ----------

display(sales_dates)

# COMMAND ----------

# # Replace negative POS_ITEM_QTY and POS_AMT values with 0
# data_vault_data = data_vault_data.withColumn("POS_ITEM_QTY", pyf.when(pyf.col("POS_ITEM_QTY") >= 0, pyf.col("POS_ITEM_QTY")).otherwise(0))
# data_vault_data = data_vault_data.withColumn("POS_AMT", pyf.when(pyf.col("POS_AMT") >= 0, pyf.col("POS_AMT")).otherwise(0))
