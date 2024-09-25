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

# Interface
dbutils.widgets.text('RETAILER_PARAM', 'walmart', 'Retailer')
dbutils.widgets.text('CLIENT_PARAM', 'clorox', 'Client')
dbutils.widgets.text('COUNTRY_CODE_PARAM', 'us', 'Country Code')

# Process input parameters
RETAILER = dbutils.widgets.get('RETAILER_PARAM').strip().casefold()
CLIENT = dbutils.widgets.get('CLIENT_PARAM').strip().casefold()
COUNTRY_CODE = dbutils.widgets.get('COUNTRY_CODE_PARAM').strip().casefold()

# COMMAND ----------

for param in [RETAILER, CLIENT, COUNTRY_CODE]:
    print(param)

# COMMAND ----------

# Read POS data
data_vault_data = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')

# COMMAND ----------

data_vault_data_2019 = data_vault_data.where((pyf.col("SALES_DT") < pyf.lit("2020-01-01")) & (pyf.col("SALES_DT") >= pyf.lit("2019-01-01")))

total_sales_by_date_2019 = data_vault_data_2019.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_sales_by_date_2019)

# COMMAND ----------

total_inventory_by_date_2019 = data_vault_data_2019.select("SALES_DT", "ON_HAND_INVENTORY_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_inventory_by_date_2019)

# COMMAND ----------

data_vault_data_2020 = data_vault_data.where((pyf.col("SALES_DT") < pyf.lit("2021-01-01")) & (pyf.col("SALES_DT") >= pyf.lit("2020-01-01")))

total_sales_by_date_2020 = data_vault_data_2020.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_sales_by_date_2020)

# COMMAND ----------

total_inventory_by_date_2020 = data_vault_data_2020.select("SALES_DT", "ON_HAND_INVENTORY_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_inventory_by_date_2020)

# COMMAND ----------


