# Databricks notebook source
import uuid
import warnings
import numpy as np
import pandas as pd
import datetime

from pyspark.sql import Window

# from pyspark.sql.functions import *
from pyspark.sql import types as pyt
from pyspark.sql import functions as pyf
import acosta

from acosta.alerting.preprocessing import read_pos_data
from acosta.alerting.helpers import check_path_exists

print(acosta.__version__)

# COMMAND ----------

RETAILER1 = 'kroger'
CLIENT1 = 'starbucks'
COUNTRY_CODE1 = 'us'

# COMMAND ----------

# Read POS data
data_vault_data = read_pos_data(RETAILER1, CLIENT1, COUNTRY_CODE1, sqlContext).repartition('SALES_DT')

# COMMAND ----------

data_vault_data.dtypes

# COMMAND ----------

data_vault_data = data_vault_data.withColumn('RETAILER_ITEM_ID', pyf.regexp_replace('RETAILER_ITEM_ID', r'^[0]*', ''))

# COMMAND ----------

kroger_authorization_list_items_path = '/mnt/artifacts/hugh/kroger-starbucks/Kroger_Starbucks_Business_Selected_Items.csv'
check_path_exists(kroger_authorization_list_items_path, 'csv', 'raise')

# COMMAND ----------

# Load table with business selected items and join it to the main table
df = spark.read.format('csv') \
    .options(header='true', inferSchema='false') \
    .load(kroger_authorization_list_items_path)

df = df.withColumn('Product ID', df['Product ID'].cast('string'))
df = df.withColumn('Product ID', pyf.regexp_replace('Product ID', r'^[0]*', ''))

# COMMAND ----------

df.count()

# COMMAND ----------

# join
data_vault_data = data_vault_data.join(df,
                         data_vault_data.RETAILER_ITEM_ID == df['Product ID'],
                         how='inner') \
    .fillna(0, subset=df.columns[:-1]).drop('Product ID')

# COMMAND ----------

data_vault_data.select('RETAILER_ITEM_ID').distinct().count()

# COMMAND ----------

retailer_item_ids_df = data_vault_data.select('RETAILER_ITEM_ID').distinct().orderBy('RETAILER_ITEM_ID')

# COMMAND ----------

display(retailer_item_ids_df)

# COMMAND ----------

store_item_ids_df = data_vault_data.select('RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM').distinct().orderBy('RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM')

# COMMAND ----------

data_vault_data_selected_columns = data_vault_data.select('RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM', 'SALES_DT', 'POS_ITEM_QTY')

# COMMAND ----------

data_vault_data_pre_reset = data_vault_data_selected_columns.where((pyf.col("SALES_DT") < pyf.lit("2020-03-01")) & (pyf.col("SALES_DT") >= pyf.lit("2020-02-02")))
data_vault_data_post_reset = data_vault_data_selected_columns.where((pyf.col("SALES_DT") < pyf.lit("2020-07-19")) & (pyf.col("SALES_DT") >= pyf.lit("2020-06-21")))

# COMMAND ----------

data_vault_data_pre_reset = data_vault_data_pre_reset\
    .withColumn('POS_ITEM_QTY', (pyf.col('POS_ITEM_QTY') / 4))

data_vault_data_post_reset = data_vault_data_post_reset\
    .withColumn('POS_ITEM_QTY', (pyf.col('POS_ITEM_QTY') / 4))

# COMMAND ----------

average_weekly_sales_by_item_store_pre_reset = data_vault_data_pre_reset.select('RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM', 'POS_ITEM_QTY').orderBy('RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM').groupBy('RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM').sum().withColumnRenamed('sum(POS_ITEM_QTY)', 'AVG_WEEKLY_SALES_PRE_RESET')

average_weekly_sales_by_item_store_post_reset = data_vault_data_post_reset.select('RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM', 'POS_ITEM_QTY').orderBy('RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM').groupBy('RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM').sum().withColumnRenamed('sum(POS_ITEM_QTY)', 'AVG_WEEKLY_SALES_POST_RESET')

# COMMAND ----------

result = store_item_ids_df\
    .join(average_weekly_sales_by_item_store_pre_reset, ['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'], 'left')\
    .join(average_weekly_sales_by_item_store_post_reset, ['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'], 'left')\

# COMMAND ----------

display(result)

# COMMAND ----------


