# Databricks notebook source
# Import libraries

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

# Define params

SOURCE_SYSTEM = 'market6'
RETAILER = 'kroger'
CLIENT = 'bushbros'
COUNTRY_CODE = 'us'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc market6_kroger_bushbros_us_dv.vw_latest_sat_epos_summary

# COMMAND ----------

# TODO: modify in-line sql to join to sat_organization_unit and sat_retailer_item_upc tables

def read_pos_data(source_system, retailer, client, country_code, sql_context):
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

    source_system = source_system.strip().upper()
    retailer = retailer.strip().upper()
    client = client.strip().upper()
    country_code = country_code.strip().upper()

    database = '{source_system}_{retailer}_{client}_{country_code}_dv'.format(
        source_system=source_system.strip().lower(),
        retailer=retailer.strip().lower(),
        client=client.strip().lower(),
        country_code=country_code.strip().lower()
    )

    sql_statement = """
        SELECT
              \'{retailer}\' AS RETAILER,
              \'{client}\' AS CLIENT,
              \'{country_code}\' AS COUNTRY_CODE,
              VSLES.RETAILER_ITEM_ID,
              VSLES.ORGANIZATION_UNIT_NUM,
              VSLES.SALES_DT,
              VSLES.POS_ITEM_QTY,
              VSLES.POS_AMT,
              VSLES.ON_HAND_INVENTORY_QTY,
              VSLES.POS_AMT / VSLES.POS_ITEM_QTY AS UNIT_PRICE,
              'DATA VAULT' AS ROW_ORIGIN,
              'COMPUTED' AS PRICE_ORIGIN,
              'IGNORE' AS TRAINING_ROLE
           FROM {database}.vw_latest_sat_epos_summary VSLES
    """

    sql_statement = sql_statement.format(database=database, retailer=retailer, client=client, country_code=country_code)

    return sql_context.sql(sql_statement)

# COMMAND ----------

# Important - Read POS data
data_vault_data1 = read_pos_data(SOURCE_SYSTEM, RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data1 = data_vault_data1.where((pyf.col("SALES_DT") < pyf.lit("2024-01-31")) & (pyf.col("SALES_DT") >= pyf.lit("2022-01-01")))

# COMMAND ----------

display(data_vault_data1)

# COMMAND ----------

# Join to items of interest

# COMMAND ----------

# TODO: Aggregate by week

# COMMAND ----------

total_sales_by_date1 = data_vault_data1.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_sales_by_date1)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from market6_kroger_bushbros_us_dv.sat_organization_unit

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from market6_kroger_bushbros_us_dv.sat_retailer_item

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from market6_kroger_bushbros_us_dv.sat_retailer_item_upc

# COMMAND ----------

count_sales_by_store_item = data_vault_data1.select("ORGANIZATION_UNIT_NUM", "RETAILER_ITEM_ID").orderBy("ORGANIZATION_UNIT_NUM", "RETAILER_ITEM_ID").groupBy("ORGANIZATION_UNIT_NUM", "RETAILER_ITEM_ID").count()

display(count_sales_by_store_item)

# COMMAND ----------

pivot_df = count_sales_by_store_item.groupBy("ORGANIZATION_UNIT_NUM").pivot("RETAILER_ITEM_ID").sum("count")

display(pivot_df)

# COMMAND ----------

count_sales_by_date1 = data_vault_data1.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").count()

display(count_sales_by_date1)

# COMMAND ----------

avg_sales_by_date1 = data_vault_data1.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").mean()

display(avg_sales_by_date1)

# COMMAND ----------

total_inventory_by_date1 = data_vault_data1.select("SALES_DT", "ON_HAND_INVENTORY_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_inventory_by_date1)

# COMMAND ----------


