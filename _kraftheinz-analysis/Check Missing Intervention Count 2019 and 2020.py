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

agg_data = data\
    .select('RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM', 'POS_ITEM_QTY') \
    .groupBy('RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM') \
    .count()

# COMMAND ----------

display(agg_data)

# COMMAND ----------

print(agg_data.count())

# COMMAND ----------

agg_data_non_zero = data\
    .select('RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM', 'POS_ITEM_QTY') \
    .filter('POS_ITEM_QTY > 0') \
    .groupBy('RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM') \
    .count()

# COMMAND ----------

display(agg_data_non_zero)

# COMMAND ----------

print(agg_data_non_zero.count())

# COMMAND ----------

agg_data_zero = agg_data.drop('count').join(
    agg_data_non_zero.drop('count'),
    ['RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'],
    'left_anti'
)

# COMMAND ----------

print(agg_data_zero.count())

# COMMAND ----------

if STORE:
    data = data.filter('ORGANIZATION_UNIT_NUM == "{}"'.format(STORE))

if ITEM:
    data = data.filter('RETAILER_ITEM_ID == "{}"'.format(ITEM))

# COMMAND ----------

# This filter requires at least 84 days of non-zero sales in the entire dataset
subset_meets_threshold = data\
    .select('RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM', 'POS_ITEM_QTY') \
    .filter('POS_ITEM_QTY > 0') \
    .groupBy('RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM') \
    .count() \
    .filter('count >= 84') \
    .drop('count')

data_meets_threshold = data.join(
    subset_meets_threshold,
    ['RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'],
    'inner'
)

# COMMAND ----------

print(subset_meets_threshold.count())

# COMMAND ----------

_ = [print(name, dtype) for name, dtype in data_meets_threshold.dtypes]

# COMMAND ----------

# This filter requires at least 84 days of non-zero sales in the entire dataset
subset_does_not_meet_threshold = data\
    .select('RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM', 'POS_ITEM_QTY') \
    .filter('POS_ITEM_QTY > 0') \
    .groupBy('RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM') \
    .count() \
    .filter('count < 84') \
    .drop('count')

data_does_not_meet_threshold = data.join(
    subset_does_not_meet_threshold,
    ['RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'],
    'inner'
)

# COMMAND ----------

# This filter requires at least 30 days of non-zero sales in the entire dataset
subset_meets_30_day_threshold = data\
    .select('RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM', 'POS_ITEM_QTY') \
    .filter('POS_ITEM_QTY > 0') \
    .groupBy('RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM') \
    .count() \
    .filter('count >= 30') \
    .drop('count')

data_meets_30_day_threshold = data.join(
    subset_meets_30_day_threshold,
    ['RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'],
    'inner'
)

# COMMAND ----------

# This filter requires at least 30 days of non-zero sales in the entire dataset
subset_does_not_meet_30_day_threshold = data\
    .select('RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM', 'POS_ITEM_QTY') \
    .filter('POS_ITEM_QTY > 0') \
    .groupBy('RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM') \
    .count() \
    .filter('count < 30') \
    .drop('count')

data_does_not_meet_30_day_threshold = data.join(
    subset_does_not_meet_30_day_threshold,
    ['RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'],
    'inner'
)

# COMMAND ----------

print(subset_does_not_meet_threshold.count())

# COMMAND ----------

df_iv = spark.read.format('delta').load('/mnt/processed/temp/intervention_data-kraftheinz-tesco')

df_iv.count()

# COMMAND ----------

_ = [print(name, dtype) for name, dtype in df_iv.dtypes]

# COMMAND ----------

df_iv = df_iv.filter('InterventionDate >= "2019-01-01"')
df_iv.count()

# COMMAND ----------

display(df_iv)

# COMMAND ----------

df_iv = df_iv.withColumnRenamed('RefExternal', 'RETAILER_ITEM_ID')
df_iv = df_iv.withColumnRenamed('ChainRefExternal', 'ORGANIZATION_UNIT_NUM')

# COMMAND ----------

df_iv_missing_data = df_iv.join(
    agg_data,
    ['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'],
    'left_anti'
)

df_iv_missing_data.count()

# df  = df1.join(df2, on=['key'], how='left_anti')

# COMMAND ----------

display(df_iv_missing_data)

# COMMAND ----------

df_iv_agg_data = df_iv.join(
    agg_data,
    ['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'],
    'inner'
)

df_iv_agg_data.count()

# COMMAND ----------

df_iv_agg_data_zero = df_iv.join(
    agg_data_zero,
    ['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'],
    'inner'
)

df_iv_agg_data_zero.count()

# COMMAND ----------

display(df_iv_agg_data_zero)

# COMMAND ----------

df_iv_agg_data_non_zero = df_iv.join(
    agg_data_non_zero,
    ['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'],
    'inner'
)

df_iv_agg_data_non_zero.count()

# COMMAND ----------

df_iv_meets_threshold = df_iv.join(
    subset_meets_threshold,
    ['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'],
    'inner'
)

df_iv_meets_threshold.count()

# COMMAND ----------

display(df_iv_meets_threshold)

# COMMAND ----------

df_iv_does_not_meet_threshold = df_iv.join(
    subset_does_not_meet_threshold,
    ['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'],
    'inner'
)

df_iv_does_not_meet_threshold.count()

# COMMAND ----------

df_iv_does_not_meet_30_day_threshold = df_iv.join(
    subset_does_not_meet_30_day_threshold,
    ['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'],
    'inner'
)

df_iv_does_not_meet_30_day_threshold.count()

# COMMAND ----------

df_ivs_with_epos = spark.read.format('delta').load('/mnt/processed/temp/kraftheinz_tesco_inv_epos')

df_ivs_with_epos.count()

# COMMAND ----------

df_ivs_with_epos = df_ivs_with_epos.withColumnRenamed('RefExternal', 'RETAILER_ITEM_ID')
df_ivs_with_epos = df_ivs_with_epos.withColumnRenamed('ChainRefExternal', 'ORGANIZATION_UNIT_NUM')

# COMMAND ----------

df_ivs_with_epos_meets_threshold = df_ivs_with_epos.join(
    subset_meets_threshold,
    ['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'],
    'inner'
)

df_ivs_with_epos_meets_threshold.count()

# COMMAND ----------

df_ivs_with_epos_does_not_meet_threshold = df_ivs_with_epos.join(
    subset_does_not_meet_threshold,
    ['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'],
    'inner'
)

df_ivs_with_epos_does_not_meet_threshold.count()

# COMMAND ----------


