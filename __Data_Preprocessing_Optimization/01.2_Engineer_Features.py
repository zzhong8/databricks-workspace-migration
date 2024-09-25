# Databricks notebook source
import uuid
import warnings

import acosta
from acosta.alerting.preprocessing import pos_to_training_data

print(acosta.__version__)

# COMMAND ----------

dbutils.widgets.text('retailer', 'walmart', 'Retailer')
dbutils.widgets.text('client', 'clorox', 'Client')
dbutils.widgets.text('countrycode', 'us', 'Country Code')

dbutils.widgets.text('store', '', 'Organization Unit Num')
dbutils.widgets.text('item', '', 'Retailer Item ID')
dbutils.widgets.text('runid', '', 'Run ID')

dbutils.widgets.dropdown('MODEL_SOURCE', 'local', ['local', 'prod'], 'Model Source')
dbutils.widgets.dropdown('INCLUDE_DISCOUNT_FEATURES', 'No', ['Yes', 'No'], 'Include Discount Features')

RETAILER = dbutils.widgets.get('retailer').strip().lower()
CLIENT = dbutils.widgets.get('client').strip().lower()
COUNTRY_CODE = dbutils.widgets.get('countrycode').strip().lower()

RUN_ID = dbutils.widgets.get('runid').strip()
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

if RETAILER == '':
    raise ValueError('\'retailer\' is a required parameter.  Please provide a value.')

if CLIENT == '':
    raise ValueError('\'client\' is a required parameter.  Please provide a value.')

if COUNTRY_CODE == '':
    raise ValueError('\'countrycode\' is a required parameter. Please provide a value.')

if RUN_ID == '':
    RUN_ID = str(uuid.uuid4())

# PATHS
PATH_DATA_VAULT_TRANSFORM_OUTPUT = '/mnt/processed/training/{run_id}/data_vault/'.format(run_id=RUN_ID)
PATH_ENGINEERED_FEATURES_OUTPUT = '/mnt/processed/training/{run_id}/engineered/'.format(run_id=RUN_ID)
PATH_TEMP_DIR_OUTPUT = '/mnt/processed/training/{run_id}/temp/'.format(run_id=RUN_ID)

for param in [RETAILER, CLIENT, COUNTRY_CODE, STORE, ITEM, RUN_ID]:
    print(param)

if CLIENT.lower() not in RUN_ID.lower():
    warnings.warn('Potentially uninformative RUN_ID, {} is not in the RUN_ID'.format(CLIENT))

# COMMAND ----------

data = spark.read.format('delta').load(PATH_DATA_VAULT_TRANSFORM_OUTPUT)
print(data.dtypes)

# COMMAND ----------

if STORE:
    data = data.filter('ORGANIZATION_UNIT_NUM == "{}"'.format(STORE))

if ITEM:
    data = data.filter('RETAILER_ITEM_ID == "{}"'.format(ITEM))

# This filter requires at least 84 days of non-zero sales in the entire dataset
subset_meets_threshold = data\
    .select('RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM', 'POS_ITEM_QTY') \
    .filter('POS_ITEM_QTY > 0') \
    .groupBy('RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM') \
    .count() \
    .filter('count >= 84') \
    .drop('count')

data = data.join(
    subset_meets_threshold,
    ['RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'],
    'inner'
)

# COMMAND ----------

output_data = pos_to_training_data(
    df=data,
    retailer=RETAILER,
    client=CLIENT,
    country_code=COUNTRY_CODE,
    model_source=MODEL_SOURCE,
    spark=spark,
    spark_context=sc,
    include_discount_features=INCLUDE_DISCOUNT_FEATURES,
    item_store_cols=['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM']
)

data = None

# COMMAND ----------

_ = [print(name, dtype) for name, dtype in output_data.dtypes]

# COMMAND ----------

output_data \
    .write.format('delta') \
    .mode('overwrite') \
    .save(PATH_ENGINEERED_FEATURES_OUTPUT)
