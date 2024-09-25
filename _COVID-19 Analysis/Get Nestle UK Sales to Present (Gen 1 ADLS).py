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

sqlContext = SQLContext(sc)

# COMMAND ----------

RETAILER = 'asda'
CLIENT = 'nestlecore'
COUNTRY_CODE = 'uk'

# COMMAND ----------

def read_pos_data(retailer, client, country_code, sql_context):
    """
    Reads in POS data from the Data Vault and returns a DataFrame

    The output DataFrame is suitable for use in the pos_to_training_data function.

    Note that when reading from the data vault, we use a pre-defined view that will
    sort out the restatement records for us.  This means we don't have to write our
    own logic for handling duplicate rows within the source dataset.

    :param string retailer: the name of the retailer to pull
    :param string client: the name of the client to pull
    :param string country_code: the two character name of the country code to pull
    :param pyspark.sql.context.HiveContext sql_context: PySparkSQL context that can be used to connect to the Data Vault

    :return DataFrame:
    """

    retailer = retailer.strip().upper()
    client = client.strip().upper()
    country_code = country_code.strip().upper()

    database = '{retailer}_{client}_{country_code}_dv'.format(
        retailer=retailer.strip().lower(),
        client=client.strip().lower(),
        country_code=country_code.strip().lower()
    )

    sql_statement = """
        SELECT
              \'{retailer}\' AS RETAILER,
              \'{client}\' AS CLIENT,
              \'{country_code}\' AS COUNTRY_CODE,
              HRI.RETAILER_ITEM_ID,
              HOU.ORGANIZATION_UNIT_NUM,
              VSLES.SALES_DT,
              VSLES.POS_ITEM_QTY,
              VSLES.POS_AMT,
              VSLES.ON_HAND_INVENTORY_QTY,
              VSLES.POS_AMT / VSLES.POS_ITEM_QTY AS UNIT_PRICE,
              'DATA VAULT' AS ROW_ORIGIN,
              'COMPUTED' AS PRICE_ORIGIN,
              'IGNORE' AS TRAINING_ROLE
           FROM {database}.vw_sat_link_epos_summary VSLES
           LEFT OUTER JOIN {database}.hub_retailer_item HRI
                ON HRI.HUB_RETAILER_ITEM_HK = VSLES.HUB_RETAILER_ITEM_HK
           LEFT OUTER JOIN {database}.hub_organization_unit HOU
                ON HOU.HUB_ORGANIZATION_UNIT_HK = VSLES.HUB_ORGANIZATION_UNIT_HK
    """

    sql_statement = sql_statement.format(database=database, retailer=retailer, client=client, country_code=country_code)

    return sql_context.sql(sql_statement)

# COMMAND ----------

# Read POS data
data_vault_data1 = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data1 = data_vault_data1.where((pyf.col("SALES_DT") < pyf.lit("2023-12-31")) & (pyf.col("SALES_DT") >= pyf.lit("2023-01-01")))

total_sales_by_date1 = data_vault_data1.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_sales_by_date1)

# COMMAND ----------

# total_inventory_by_date1 = data_vault_data1.select("SALES_DT", "ON_HAND_INVENTORY_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

# display(total_inventory_by_date1)

# COMMAND ----------

RETAILER = 'morrisons'

# COMMAND ----------

# Read POS data
data_vault_data2 = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data2 = data_vault_data2.where((pyf.col("SALES_DT") < pyf.lit("2023-12-31")) & (pyf.col("SALES_DT") >= pyf.lit("2023-01-01")))

total_sales_by_date2 = data_vault_data2.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()
display(total_sales_by_date2)

# COMMAND ----------

# total_inventory_by_date2 = data_vault_data2.select("SALES_DT", "ON_HAND_INVENTORY_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

# display(total_inventory_by_date2)

# COMMAND ----------

RETAILER = 'sainsburys'

# COMMAND ----------

# Read POS data
data_vault_data3 = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data3 = data_vault_data3.where((pyf.col("SALES_DT") < pyf.lit("2023-12-31")) & (pyf.col("SALES_DT") >= pyf.lit("2023-01-01")))

total_sales_by_date3 = data_vault_data3.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()
display(total_sales_by_date3)

# COMMAND ----------

# total_inventory_by_date3 = data_vault_data3.select("SALES_DT", "ON_HAND_INVENTORY_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

# display(total_inventory_by_date3)

# COMMAND ----------

RETAILER = 'tesco'

# COMMAND ----------

# Read POS data
data_vault_data4 = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data4 = data_vault_data4.where((pyf.col("SALES_DT") < pyf.lit("2023-12-31")) & (pyf.col("SALES_DT") >= pyf.lit("2023-01-01")))

total_sales_by_date4 = data_vault_data4.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()
display(total_sales_by_date4)

# COMMAND ----------

# total_inventory_by_date4 = data_vault_data4.select("SALES_DT", "ON_HAND_INVENTORY_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

# display(total_inventory_by_date4)

# COMMAND ----------

# Look for weird sales values

data_vault_data1_high_sales = data_vault_data1.where(pyf.col("POS_ITEM_QTY") >= 500)

display(data_vault_data1_high_sales)

# COMMAND ----------

# Look for weird sales values

data_vault_data2_high_sales = data_vault_data2.where(pyf.col("POS_ITEM_QTY") >= 500)

display(data_vault_data2_high_sales)

# COMMAND ----------

# Look for weird sales values

data_vault_data3_high_sales = data_vault_data3.where(pyf.col("POS_ITEM_QTY") >= 500)

display(data_vault_data3_high_sales)

# COMMAND ----------

# Look for weird sales values

data_vault_data4_high_sales = data_vault_data4.where(pyf.col("POS_ITEM_QTY") >= 500)

display(data_vault_data4_high_sales)

# COMMAND ----------

SOURCE_SYSTEM = 'msd'
RETAILER = 'morrisons'
CLIENT = 'droetker'
COUNTRY_CODE = 'uk'

# COMMAND ----------

# Read POS data
data_vault_data1 = read_pos_data(SOURCE_SYSTEM, RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data1 = data_vault_data1.where((pyf.col("SALES_DT") < pyf.lit("2023-08-01")) & (pyf.col("SALES_DT") >= pyf.lit("2022-01-01")))

total_sales_by_date1 = data_vault_data1.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_sales_by_date1)

# COMMAND ----------

SOURCE_SYSTEM = 'msd'
RETAILER = 'morrisons'
CLIENT = 'premier'
COUNTRY_CODE = 'uk'

# COMMAND ----------

# Read POS data
data_vault_data1 = read_pos_data(SOURCE_SYSTEM, RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data1 = data_vault_data1.where((pyf.col("SALES_DT") < pyf.lit("2023-08-01")) & (pyf.col("SALES_DT") >= pyf.lit("2022-01-01")))

total_sales_by_date1 = data_vault_data1.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_sales_by_date1)

# COMMAND ----------


