# Databricks notebook source
import uuid
import warnings
import numpy as np
import pandas as pd
from datetime import datetime
from pyspark.sql import Window
from pyspark.sql import types as pyt
from pyspark.sql import functions as pyf
from pyspark.sql import SQLContext

import acosta
from acosta.alerting.preprocessing import read_pos_data

print(acosta.__version__)

# COMMAND ----------

RETAILER = 'asda'
CLIENT = 'nestlecore'
COUNTRY_CODE = 'uk'
SOURCE_SYSTEM = 'retaillink'

# COMMAND ----------

# Read POS data
data_vault_data1 = read_pos_data(SOURCE_SYSTEM, RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data1 = data_vault_data1.where((pyf.col("SALES_DT") < pyf.lit("2023-12-31")) & (pyf.col("SALES_DT") >= pyf.lit("2023-01-01")))

total_sales_by_date1 = data_vault_data1.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_sales_by_date1)

# COMMAND ----------

total_inventory_by_date1 = data_vault_data1.select("SALES_DT", "ON_HAND_INVENTORY_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_inventory_by_date1)

# COMMAND ----------

RETAILER = 'morrisons'
SOURCE_SYSTEM = 'msd'

# COMMAND ----------

# Read POS data
data_vault_data2 = read_pos_data(SOURCE_SYSTEM, RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data2 = data_vault_data2.where((pyf.col("SALES_DT") < pyf.lit("2023-12-31")) & (pyf.col("SALES_DT") >= pyf.lit("2023-01-01")))

total_sales_by_date2 = data_vault_data2.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()
display(total_sales_by_date2)

# COMMAND ----------

total_inventory_by_date2 = data_vault_data2.select("SALES_DT", "ON_HAND_INVENTORY_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_inventory_by_date2)

# COMMAND ----------

RETAILER = 'sainsburys'
SOURCE_SYSTEM = 'horizon'

# COMMAND ----------

# Read POS data
data_vault_data3 = read_pos_data(SOURCE_SYSTEM, RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data3 = data_vault_data3.where((pyf.col("SALES_DT") < pyf.lit("2023-12-31")) & (pyf.col("SALES_DT") >= pyf.lit("2023-01-01")))

total_sales_by_date3 = data_vault_data3.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()
display(total_sales_by_date3)

# COMMAND ----------

total_inventory_by_date3 = data_vault_data3.select("SALES_DT", "ON_HAND_INVENTORY_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_inventory_by_date3)

# COMMAND ----------

RETAILER = 'tesco'
SOURCE_SYSTEM = 'tescopartnertoolkit'

# COMMAND ----------

# Read POS data
data_vault_data4 = read_pos_data(SOURCE_SYSTEM, RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data4 = data_vault_data4.where((pyf.col("SALES_DT") < pyf.lit("2023-12-31")) & (pyf.col("SALES_DT") >= pyf.lit("2023-01-01")))

total_sales_by_date4 = data_vault_data4.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()
display(total_sales_by_date4)

# COMMAND ----------

total_inventory_by_date4 = data_vault_data4.select("SALES_DT", "ON_HAND_INVENTORY_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_inventory_by_date4)
