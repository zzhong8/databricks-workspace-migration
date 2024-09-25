# Databricks notebook source
from pprint import pprint

import warnings

from datetime import datetime
from pyspark.sql import Window
from pyspark.sql import functions as pyf
from pyspark.sql.types import *

import acosta
import pyarrow
from acosta.alerting.preprocessing import read_pos_data, pos_to_training_data
from acosta.alerting.helpers import check_path_exists

print(acosta.__version__)
print(pyarrow.__version__)

current_timestamp = datetime.now().strftime('%Y-%m-%d')

# COMMAND ----------

def read_pos_data_gen1(retailer, client, country_code, sql_context):
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

# SOURCE_SYSTEM = 'tescolink'
RETAILER = 'tesco'
CLIENT = 'nestlecore'
COUNTRY_CODE = 'uk'

# COMMAND ----------

# Read POS data
data = read_pos_data_gen1(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')

pprint(dict(data.dtypes))

# COMMAND ----------

num_store_items = data.select("ORGANIZATION_UNIT_NUM", "RETAILER_ITEM_ID").distinct().count()

print(num_store_items)

# COMMAND ----------

num_stores = data.select("ORGANIZATION_UNIT_NUM").distinct().count()

print(num_stores)

# COMMAND ----------

num_items = data.select("RETAILER_ITEM_ID").distinct().count()

print(num_items)

# COMMAND ----------

SOURCE_SYSTEM = 'market6'
RETAILER = 'kroger'
CLIENT = 'danonewave'
COUNTRY_CODE = 'us'

# COMMAND ----------

# Read POS data
data = read_pos_data(SOURCE_SYSTEM, RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')

pprint(dict(data.dtypes))

# COMMAND ----------

num_store_items= data.select("ORGANIZATION_UNIT_NUM","RETAILER_ITEM_ID").distinct().count()

print(num_store_items)

# COMMAND ----------

SOURCE_SYSTEM = 'market6'
RETAILER = 'kroger'
CLIENT = 'barilla'
COUNTRY_CODE = 'us'

# COMMAND ----------

# Read POS data
data = read_pos_data(SOURCE_SYSTEM, RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')

pprint(dict(data.dtypes))

# COMMAND ----------

num_store_items= data.select("ORGANIZATION_UNIT_NUM","RETAILER_ITEM_ID").distinct().count()

print(num_store_items)

# COMMAND ----------

SOURCE_SYSTEM = 'retaillink'
RETAILER = 'walmart'
CLIENT = 'nestlewaters'
COUNTRY_CODE = 'us'

TODAY = datetime.strptime('20210316', '%Y%m%d')
YESTERDAY = datetime.strptime('20210315', '%Y%m%d')
TWO_DAYS_AGO = datetime.strptime('20210314', '%Y%m%d')
THREE_DAYS_AGO = datetime.strptime('20210313', '%Y%m%d')

# COMMAND ----------

# Read POS data
data = read_pos_data(SOURCE_SYSTEM, RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')

pprint(dict(data.dtypes))

# COMMAND ----------

data_today = data.where(pyf.col("SALES_DT") == TODAY)

num_store_items_today = data_today.select("ORGANIZATION_UNIT_NUM","RETAILER_ITEM_ID").distinct().count()

print(num_store_items_today)

# COMMAND ----------

data_yesterday = data.where(pyf.col("SALES_DT") == YESTERDAY)

num_data_yesterday = data_yesterday.count()

print(num_data_yesterday)

# COMMAND ----------

num_stores_yesterday = data_yesterday.select("ORGANIZATION_UNIT_NUM").distinct().count()

print(num_stores_yesterday)

# COMMAND ----------

num_items_yesterday = data_yesterday.select("RETAILER_ITEM_ID").distinct().count()

print(num_items_yesterday)

# COMMAND ----------

num_store_items_yesterday = data_yesterday.select("ORGANIZATION_UNIT_NUM","RETAILER_ITEM_ID").distinct().count()

print(num_store_items_yesterday)

# COMMAND ----------

data_two_days_ago = data.where(pyf.col("SALES_DT") == TWO_DAYS_AGO)

num_data_two_days_ago = data_two_days_ago.count()

print(num_data_two_days_ago)

# COMMAND ----------

num_items_two_days_ago = data_two_days_ago.select("RETAILER_ITEM_ID").distinct().count()

print(num_items_two_days_ago)

# COMMAND ----------

data_three_days_ago = data.where(pyf.col("SALES_DT") == THREE_DAYS_AGO)

num_data_three_days_ago = data_three_days_ago.count()

print(num_data_three_days_ago)

# COMMAND ----------

data_yesterday_non_zero_sales = data.where(pyf.col("SALES_DT") == YESTERDAY).filter('POS_ITEM_QTY > 0')

num_data_yesterday_non_zero_sales = data_yesterday_non_zero_sales.count()

print(num_data_yesterday_non_zero_sales)

# COMMAND ----------

data_two_days_ago_non_zero_sales = data.where(pyf.col("SALES_DT") == TWO_DAYS_AGO).filter('POS_ITEM_QTY > 0')

num_data_two_days_ago_non_zero_sales = data_two_days_ago_non_zero_sales.count()

print(num_data_two_days_ago_non_zero_sales)

# COMMAND ----------

data_three_days_ago_non_zero_sales = data.where(pyf.col("SALES_DT") == THREE_DAYS_AGO).filter('POS_ITEM_QTY > 0')

num_data_three_days_ago_non_zero_sales = data_three_days_ago_non_zero_sales.count()

print(num_data_three_days_ago_non_zero_sales)

# COMMAND ----------


