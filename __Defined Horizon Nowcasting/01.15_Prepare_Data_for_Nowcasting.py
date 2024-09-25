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

from pyspark.sql.types import IntegerType


import acosta
from acosta.alerting.preprocessing import read_pos_data, pos_to_training_data
from acosta.alerting.helpers import check_path_exists

print(acosta.__version__)
auto_model_prefix = 'data'
current_timestamp = datetime.now().strftime('%Y-%m-%d')

# COMMAND ----------

PATH_ENGINEERED_FEATURES_OUTPUT = '/mnt/processed/training/{run_id}/engineered/'.format(run_id=RUN_ID)

for param in [RETAILER, CLIENT, COUNTRY_CODE, STORE, ITEM, RUN_ID]:
    print(param)

# COMMAND ----------

# Read POS data
data = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')

# COMMAND ----------

if STORE:
    data = data.filter('ORGANIZATION_UNIT_NUM == "{}"'.format(STORE))

if ITEM:
    data = data.filter('RETAILER_ITEM_ID == "{}"'.format(ITEM))

# COMMAND ----------

# Get 30 days of sales data before the first actual day that we wish to nowcast
data = data.where((col("SALES_DT") <= END_DATE) & (date_add(col("SALES_DT"), 30) >= START_DATE))

# COMMAND ----------

# Replace negative POS_ITEM_QTY and POS_AMT values with 0
data = data.withColumn('POS_ITEM_QTY', pyf.when(pyf.col('POS_ITEM_QTY') >= 0, pyf.col('POS_ITEM_QTY')).otherwise(0))
data = data.withColumn('POS_AMT', pyf.when(pyf.col('POS_AMT') >= 0, pyf.col('POS_AMT')).otherwise(0))

# COMMAND ----------

# Import approved product list for this client
approved_product_list_reference_path = '/mnt/artifacts/country_code/reference/approved_product_list/retailer={retailer}/client={client}/country_code={country_code}/'\
    .format(retailer=RETAILER, client=CLIENT, country_code=COUNTRY_CODE)

# If the approved product list exists for this client
if check_path_exists(approved_product_list_reference_path, 'delta', 'ignore'):
    approved_product_list = spark.read.format('delta').load(approved_product_list_reference_path)\
        .select(['RetailProductCode'])
    
    data = data.join(
        approved_product_list,
        data.RETAILER_ITEM_ID == approved_product_list.RetailProductCode)
    
else:
    print('Approved product list not found for client')

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
