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

RETAILER = 'sainsburys'
CLIENT = 'perfetti'
COUNTRY_CODE = 'uk'
SOURCE_SYSTEM = 'horizon'

# COMMAND ----------

# Read POS data
data_vault_data5 = read_pos_data(SOURCE_SYSTEM, RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data5 = data_vault_data5.where((pyf.col("SALES_DT") < pyf.lit("2023-12-31")) & (pyf.col("SALES_DT") >= pyf.lit("2023-01-01")))

total_sales_by_date5 = data_vault_data5.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_sales_by_date5)

# COMMAND ----------


