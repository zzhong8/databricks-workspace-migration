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

SOURCE_SYSTEM = 'retaillink'
RETAILER = 'walmart'
CLIENT = 'harrysinc'
COUNTRY_CODE = 'us'

# COMMAND ----------

# Read POS data
data_vault_data2022 = read_pos_data(SOURCE_SYSTEM, RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data2022 = data_vault_data2022.where((pyf.col("SALES_DT") < pyf.lit("2022-08-01")) & (pyf.col("SALES_DT") >= pyf.lit("2022-01-01")))

total_sales_by_date2022 = data_vault_data2022.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_sales_by_date2022)

# COMMAND ----------

# Read POS data
data_vault_data2024 = read_pos_data(SOURCE_SYSTEM, RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data2024 = data_vault_data2024.where((pyf.col("SALES_DT") < pyf.lit("2024-08-01")) & (pyf.col("SALES_DT") >= pyf.lit("2024-01-01")))

total_sales_by_date2024 = data_vault_data2024.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_sales_by_date2024)

# COMMAND ----------

count_sales_by_date2022 = data_vault_data2022.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").count()

display(count_sales_by_date2022)

# COMMAND ----------

count_sales_by_date2024 = data_vault_data2024.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").count()

display(count_sales_by_date2024)

# COMMAND ----------

display(data_vault_data2022)

# COMMAND ----------

count_sales_by_store_item_2022 = data_vault_data2022.select("ORGANIZATION_UNIT_NUM", "RETAILER_ITEM_ID").orderBy("ORGANIZATION_UNIT_NUM", "RETAILER_ITEM_ID").groupBy("ORGANIZATION_UNIT_NUM", "RETAILER_ITEM_ID").count()

display(count_sales_by_store_item_2022)

# COMMAND ----------

pivot_df_2022 = count_sales_by_store_item_2022.groupBy("ORGANIZATION_UNIT_NUM").pivot("RETAILER_ITEM_ID").sum("count")

display(pivot_df_2022)

# COMMAND ----------

# Read POS data
data_vault_data1 = read_pos_data(SOURCE_SYSTEM, RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data1 = data_vault_data1.where((pyf.col("SALES_DT") < pyf.lit("2022-08-01")) & (pyf.col("SALES_DT") >= pyf.lit("2020-01-01")))

total_sales_by_date1 = data_vault_data1.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_sales_by_date1)

# COMMAND ----------

total_inventory_by_date1 = data_vault_data1.select("SALES_DT", "ON_HAND_INVENTORY_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_inventory_by_date1)

# COMMAND ----------

data_vault_data_585758642 = data_vault_data1.where(pyf.col("retailer_item_id") == pyf.lit("585758642"))

# COMMAND ----------

total_sales_by_date_585758642 = data_vault_data_585758642.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_sales_by_date_585758642)

# COMMAND ----------

total_inventory_by_date_585758642 = data_vault_data_585758642.select("SALES_DT", "ON_HAND_INVENTORY_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_inventory_by_date_585758642)

# COMMAND ----------

data_vault_data_585758525 = data_vault_data1.where(pyf.col("retailer_item_id") == pyf.lit("585758525"))

# COMMAND ----------

total_sales_by_date_585758525 = data_vault_data_585758525.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_sales_by_date_585758525)

# COMMAND ----------

total_inventory_by_date_585758525 = data_vault_data_585758525.select("SALES_DT", "ON_HAND_INVENTORY_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_inventory_by_date_585758525)

# COMMAND ----------

data_vault_data_596856454 = data_vault_data1.where(pyf.col("retailer_item_id") == pyf.lit("596856454"))

# COMMAND ----------

total_sales_by_date_596856454 = data_vault_data_596856454.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_sales_by_date_596856454)

# COMMAND ----------

total_inventory_by_date_596856454 = data_vault_data_596856454.select("SALES_DT", "ON_HAND_INVENTORY_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_inventory_by_date_596856454)

# COMMAND ----------

data_vault_data_596856455 = data_vault_data1.where(pyf.col("retailer_item_id") == pyf.lit("596856455"))

# COMMAND ----------

total_sales_by_date_596856455 = data_vault_data_596856455.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_sales_by_date_596856455)

# COMMAND ----------

total_inventory_by_date_596856455 = data_vault_data_596856455.select("SALES_DT", "ON_HAND_INVENTORY_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_inventory_by_date_596856455)

# COMMAND ----------

data_vault_data_567451625 = data_vault_data1.where(pyf.col("retailer_item_id") == pyf.lit("567451625"))

# COMMAND ----------

total_sales_by_date_567451625 = data_vault_data_567451625.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_sales_by_date_567451625)

# COMMAND ----------

total_inventory_by_date_567451625 = data_vault_data_567451625.select("SALES_DT", "ON_HAND_INVENTORY_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_inventory_by_date_567451625)

# COMMAND ----------

data_vault_data_567451584 = data_vault_data1.where(pyf.col("retailer_item_id") == pyf.lit("567451584"))

# COMMAND ----------

total_sales_by_date_567451584 = data_vault_data_567451584.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_sales_by_date_567451584)

# COMMAND ----------

total_inventory_by_date_567451584 = data_vault_data_567451584.select("SALES_DT", "ON_HAND_INVENTORY_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_inventory_by_date_567451584)

# COMMAND ----------

data_vault_data_585758644 = data_vault_data1.where(pyf.col("retailer_item_id") == pyf.lit("585758644"))

# COMMAND ----------

total_sales_by_date_585758644 = data_vault_data_585758644.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_sales_by_date_585758644)

# COMMAND ----------

total_inventory_by_date_585758644 = data_vault_data_585758644.select("SALES_DT", "ON_HAND_INVENTORY_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_inventory_by_date_585758644)

# COMMAND ----------


