# Databricks notebook source
# PSEUDOCODE TO CREATE REFERENCE ARTIFACT

# To create process, thinking to create tables for 'all known products' 'all known stores' that represent data loaded into the date
# reference table. Then do a join against them to identify new elements in each, and generate all dates going forward.

import math
import datetime
from pyspark.sql import Row
from pyspark.sql.types import *
import pyspark.sql.functions as pyf

from acosta.alerting.helpers import check_path_exists

# COMMAND ----------

dbutils.widgets.text('retailer', 'walmart', 'Retailer')
dbutils.widgets.text('client', 'campbellssnack', 'Client')
dbutils.widgets.text('start', '20190101', 'Starting Date')
dbutils.widgets.text('end', '20191231', '_Ending Date')

RETAILER = dbutils.widgets.get('retailer').strip().upper()
CLIENT = dbutils.widgets.get('client').strip().upper()

START = datetime.datetime.strptime(dbutils.widgets.get('start'), '%Y%m%d').date()
END = datetime.datetime.strptime(dbutils.widgets.get('end'), '%Y%m%d').date()

if RETAILER == '':
    raise ValueError('"retailer" is a required parameter. Please provide a value.')

if CLIENT == '':
    raise ValueError('"client" is a required parameter. Please provide a value.')

for param in [RETAILER, CLIENT, START, END]:
    print(param)

# COMMAND ----------

# TODO add path exists check after the format has changed to delta.
# Get the store/item combinations that we have champion models for
stores_items = spark.read.parquet('/mnt/artifacts/champion_models_v2/')\
.filter("RETAILER = '{}' and CLIENT = '{}'".format(RETAILER.upper(), CLIENT.upper()))\
.select(['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM']).distinct()

print('Number of items: {:,}'.format(stores_items.select('RETAILER_ITEM_ID').distinct().count()))

# COMMAND ----------

display(stores_items)
