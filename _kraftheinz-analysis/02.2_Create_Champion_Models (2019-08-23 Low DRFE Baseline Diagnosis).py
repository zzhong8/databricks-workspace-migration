# Databricks notebook source
from pyspark.sql import functions as pyf
from pyspark.sql.types import *
from pyspark.sql.window import Window
import datetime as dtm
import uuid
from acosta.alerting.helpers import check_path_exists

# CONSTANTS
current_datetime = dtm.datetime.now()

# COMMAND ----------

# Loading all variables and constants
dbutils.widgets.text('RETAILER_PARAM', 'walmart', 'Retailer(s)')
dbutils.widgets.text('CLIENT_PARAM', '', 'Client(s)')
dbutils.widgets.text('COUNTRY_CODE_PARAM', 'us', 'Country Code')

# COMMAND ----------

CLIENT_PARAM = dbutils.widgets.get('CLIENT_PARAM').strip()
RETAILER_PARAM = dbutils.widgets.get('RETAILER_PARAM').strip()
COUNTRY_CODE_PARAM = dbutils.widgets.get('COUNTRY_CODE_PARAM').strip()

# COMMAND ----------

print(CLIENT_PARAM, RETAILER_PARAM, COUNTRY_CODE_PARAM)

# COMMAND ----------

MODEL_SOURCE = ''
champions_path = '/mnt{mod}/artifacts/country_code/champion_models/retailer={retailer}/client={client}/country_code={country_code}'.format(
    mod='' if MODEL_SOURCE == 'LOCAL' else '/prod-ro',
    retailer=RETAILER_PARAM,
    client=CLIENT_PARAM,
    country_code=COUNTRY_CODE_PARAM
)
print(champions_path)
check_path_exists(champions_path, 'delta')

champions = spark.read.format('delta') \
    .load(champions_path) \
    .drop('RETAILER', 'CLIENT', 'COUNTRY_CODE') \
    .withColumnRenamed('ORGANIZATION_UNIT_NUM', 'ORGANIZATION_UNIT_LIST')

# COMMAND ----------

# Import approved product list for this client
champion_models_write_reference_path = '/mnt/artifacts/hugh/champion_models/client={client}/retailer={retailer}/'\
    .format(client=CLIENT_PARAM, retailer=RETAILER_PARAM)

# Save the data set
champions.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save(champion_models_write_reference_path)

# COMMAND ----------


