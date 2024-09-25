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
import acosta
from acosta.alerting.preprocessing import read_pos_data
from acosta.alerting.helpers import check_path_exists


print(acosta.__version__)
auto_model_prefix = 'model'
current_timestamp = datetime.now().strftime('%Y-%m-%d')

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

    database = '{retailer}_{client}_{country_code}_dv'.format(retailer=retailer.strip().lower(),
                                                              client=client.strip().lower(),
                                                              country_code=country_code.strip().lower())

    sql_statement = """
        SELECT
              \'{retailer}\' AS RETAILER,
              \'{client}\' AS CLIENT,
              \'{country_code}\' AS COUNTRY_CODE,
              HRI.RETAILER_ITEM_ID,
              SRI.RETAILER_ITEM_DESC, 
              HOU.ORGANIZATION_UNIT_NUM,
              SOU.ORGANIZATION_UNIT_NM,
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
           LEFT OUTER JOIN {database}.sat_retailer_item SRI   
                ON SRI.HUB_RETAILER_ITEM_HK = VSLES.HUB_RETAILER_ITEM_HK
           LEFT OUTER JOIN {database}.hub_organization_unit HOU 
                ON HOU.HUB_ORGANIZATION_UNIT_HK = VSLES.HUB_ORGANIZATION_UNIT_HK
           LEFT OUTER JOIN {database}.sat_organization_unit SOU 
                ON SOU.HUB_ORGANIZATION_UNIT_HK = VSLES.HUB_ORGANIZATION_UNIT_HK
        """

    sql_statement = sql_statement.format(database=database, retailer=retailer, client=client, country_code=country_code)

    return sql_context.sql(sql_statement)


# COMMAND ----------

dbutils.widgets.text('retailer', 'walmart', 'Retailer')
dbutils.widgets.text('client', 'clorox', 'Client')
dbutils.widgets.text('countrycode', 'us', 'Country Code')
dbutils.widgets.text('store', '', 'Organization Unit Num')
dbutils.widgets.text('item', '', 'Retailer Item ID')
dbutils.widgets.text('runid', '', 'Run ID')
dbutils.widgets.text('timestamp', current_timestamp, 'Timestamp')

RETAILER = dbutils.widgets.get('retailer').strip().lower()
CLIENT = dbutils.widgets.get('client').strip().lower()
COUNTRY_CODE = dbutils.widgets.get('countrycode').strip().lower()

RUN_ID = dbutils.widgets.get('runid').strip()
TIMESTAMP = dbutils.widgets.get('timestamp').strip()

try:
    STORE = int(dbutils.widgets.get('store').strip())
except ValueError:
    STORE = None
try:
    ITEM = str(dbutils.widgets.get('item').strip())
except ValueError:
    ITEM = None

if RETAILER == '':
    raise ValueError('\'retailer\' is a required parameter.  Please provide a value.')

if CLIENT == '':
    raise ValueError('\'client\' is a required parameter.  Please provide a value.')

if COUNTRY_CODE  == '':
    raise ValueError('\'countrycode\' is a required parameter.  Please provide a value.')

if RUN_ID == '':
    RUN_ID = str(uuid.uuid4())
elif RUN_ID.lower() == 'auto':
    RUN_ID = '-'.join([auto_model_prefix, CLIENT, TIMESTAMP])

# PATHS
PATH_DATA_VAULT_TRANSFORM_OUTPUT = '/mnt/processed/training/{run_id}/data_vault/'.format(run_id=RUN_ID)

for param in [RETAILER, CLIENT, COUNTRY_CODE, STORE, ITEM, TIMESTAMP, RUN_ID]:
    print(param)

if CLIENT.lower() not in RUN_ID.lower():
    warnings.warn('Potentially uninformative RUN_ID, {} is not in the RUN_ID'.format(CLIENT))

# COMMAND ----------

retailer = RETAILER
client = CLIENT
country_code = COUNTRY_CODE

database = '{retailer}_{client}_{country_code}_dv'.format(retailer=retailer.strip().lower(),
                                                          client=client.strip().lower(),
                                                          country_code=country_code.strip().lower())
    
sql_get_retailer_item_ids = """
    SELECT DISTINCT
          \'{retailer}\' AS RETAILER,
          \'{client}\' AS CLIENT,
          \'{country_code}\' AS COUNTRY_CODE,
          HRI.RETAILER_ITEM_ID,
          SRI.RETAILER_ITEM_DESC
       FROM
       {database}.hub_retailer_item HRI 
       INNER JOIN {database}.sat_retailer_item SRI   
            ON HRI.HUB_RETAILER_ITEM_HK = SRI.HUB_RETAILER_ITEM_HK
       WHERE SRI.RETAILER_ITEM_DESC != ""
    """

sql_get_retailer_item_ids = sql_get_retailer_item_ids.format(database=database, retailer=retailer, client=client, country_code=country_code)

df_retailer_item_ids = sqlContext.sql(sql_get_retailer_item_ids)

display(df_retailer_item_ids)

# COMMAND ----------

# Read POS data
data_vault_data = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')

# COMMAND ----------

data_vault_data.count()

# COMMAND ----------

data_vault_data.columns

# COMMAND ----------

data_vault_data1 = data_vault_data.where((col("SALES_DT") <= lit("2019-12-31")) & (col("SALES_DT") >= lit("2019-01-01")))
# data_vault_data2 = data_vault_data1.where((col("RETAILER_ITEM_ID") == ITEM))

# COMMAND ----------

data_vault_data1.select('POS_ITEM_QTY').groupBy().sum().collect()

# COMMAND ----------

display(data_vault_data1)

# COMMAND ----------

data_vault_data2 = data_vault_data1.where((col("RETAILER_ITEM_ID") == ITEM))
data_vault_data2.select('POS_ITEM_QTY').groupBy().sum().collect()

# COMMAND ----------

# intervention_date_windows_path = '/mnt/artifacts/hugh/kraftheinz-intervention-data/kraft_heinz_intervention_date_windows.csv'
# intervention_path = '/mnt/artifacts/hugh/kraftheinz-intervention-data/KHZ-All_Interventions_1819.csv'

# check_path_exists(intervention_date_windows_path, 'csv', 'raise')

# COMMAND ----------

spark_df_info = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load(intervention_date_windows_path)

cols = spark_df_info.columns

cols

# COMMAND ----------

spark_df_intervention_info = spark.read.format('csv')\
    .options(header='false', inferSchema='true')\
    .load(intervention_path)

spark_df_intervention_info = spark_df_intervention_info.toDF(*cols)

df_intervention_products = spark_df_intervention_info.select('ProductNumber').distinct()

# COMMAND ----------

data_vault_data3 = data_vault_data1.join(df_intervention_products, data_vault_data1.RETAILER_ITEM_ID == df_intervention_products.ProductNumber)
data_vault_data3.select('POS_ITEM_QTY').groupBy().sum().collect()

# COMMAND ----------

analyzed_product_list_path = '/mnt/artifacts/hugh/kraftheinz-intervention-data/ranked_item_by_iv_top_76%_tesco_list.csv'

check_path_exists(analyzed_product_list_path, 'csv', 'raise')

# COMMAND ----------

df_analyzed_products = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load(analyzed_product_list_path)

# COMMAND ----------

data_vault_data4 = data_vault_data1.join(df_analyzed_products, data_vault_data1.RETAILER_ITEM_ID == df_analyzed_products.ProductNumber)
data_vault_data4.select('POS_ITEM_QTY').groupBy().sum().collect()
