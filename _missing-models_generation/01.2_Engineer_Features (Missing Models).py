# Databricks notebook source
import uuid
import warnings

import acosta
# Acosta.Alerting package imports
from acosta.alerting.preprocessing import pos_to_training_data

print(acosta.__version__)

# COMMAND ----------

dbutils.widgets.text('retailer', 'walmart', 'Retailer')
dbutils.widgets.text('client', 'clorox', 'Client')
dbutils.widgets.text('store', '', 'Organization Unit Num')
dbutils.widgets.text('item', '', 'Retailer Item ID')
dbutils.widgets.text('runid', '', 'Run ID')
dbutils.widgets.dropdown("INCLUDE_DISCOUNT_FEATURES", "No", ["Yes", "No"], "Include Discount Features")

RETAILER = dbutils.widgets.get('retailer').strip().lower()
CLIENT = dbutils.widgets.get('client').strip().lower()
RUN_ID = dbutils.widgets.get('runid').strip()
INCLUDE_DISCOUNT_FEATURES = dbutils.widgets.get("INCLUDE_DISCOUNT_FEATURES").strip().lower() == "yes"

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

# PATHS
PATH_DATA_VAULT_TRANSFORM_OUTPUT = '/mnt/processed/training/{run_id}/data_vault/'.format(run_id=RUN_ID)
PATH_ENGINEERED_FEATURES_OUTPUT = '/mnt/processed/training/{run_id}/engineered/'.format(run_id=RUN_ID)
PATH_TEMP_DIR_OUTPUT = '/mnt/processed/training/{run_id}/temp/'.format(run_id=RUN_ID)

for param in [RETAILER, CLIENT, STORE, ITEM, RUN_ID]:
    print(param)

if CLIENT.lower() not in RUN_ID.lower():
    warnings.warn('Potentially uninformative RUN_ID, {} is not in the RUN_ID'.format(CLIENT))

# COMMAND ----------

# sqlContext.setConf("spark.sql.shuffle.partitions", "800")

# COMMAND ----------

data = spark.read.parquet(PATH_DATA_VAULT_TRANSFORM_OUTPUT)
print(data.dtypes)

# COMMAND ----------

# missing_stores = spark.read.format('csv')\
#     .options(header='false', inferSchema='true')\
#     .load('/mnt/prod-ro/artifacts/reference/missing_store_list.csv')

# COMMAND ----------

# display(missing_stores)

# COMMAND ----------

# data = data\
#     .join(missing_stores, data.ORGANIZATION_UNIT_NUM == missing_stores._c0)\
#     .select(data['*'])

# COMMAND ----------

missing_items = spark.read.format('csv')\
    .options(header='false', inferSchema='true')\
    .load('/mnt/prod-ro/artifacts/reference/tysonhillshire_missing_item_list.csv')

# COMMAND ----------

display(missing_items)

# COMMAND ----------

data = data\
    .join(missing_items, data.RETAILER_ITEM_ID == missing_items._c0)\
    .select(data['*'])

# COMMAND ----------

display(data)

# COMMAND ----------

display(
    data
        .select("RETAILER_ITEM_ID") # Every record converted to a single column - the year captured
        .distinct()                         # Reduce all years to the list of distinct years
        .orderBy("RETAILER_ITEM_ID")
)

# COMMAND ----------

if STORE:
    data = data.filter('ORGANIZATION_UNIT_NUM == "{}"'.format(STORE))

if ITEM:
    data = data.filter('RETAILER_ITEM_ID == "{}"'.format(ITEM))

# This filter requires at least 84 days of non-zero sales in the entire datset
subset_meets_threshold = data\
    .select('RETAILER', 'CLIENT', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM', 'POS_ITEM_QTY') \
    .filter('POS_ITEM_QTY > 0') \
    .groupBy('RETAILER', 'CLIENT', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM') \
    .count() \
    .filter('count >= 84') \
    .drop('count')

data = data.join(
    subset_meets_threshold,
    ['RETAILER', 'CLIENT', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'],
    'inner'
)

# COMMAND ----------

output_data = pos_to_training_data(
    df=data,
    retailer=RETAILER,
    client=CLIENT,
    spark=spark,
    spark_context=sc,
    include_discount_features= INCLUDE_DISCOUNT_FEATURES,
    item_store_cols=['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM']
)

data = None

# COMMAND ----------

_ = [print(name, dtype) for name, dtype in output_data.dtypes]

# COMMAND ----------

output_data \
    .write.format('parquet') \
    .mode('overwrite') \
    .save(PATH_ENGINEERED_FEATURES_OUTPUT)

# COMMAND ----------


