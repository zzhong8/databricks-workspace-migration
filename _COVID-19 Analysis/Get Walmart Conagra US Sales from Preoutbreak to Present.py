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

RETAILER1 = 'walmart'
CLIENT1 = 'conagra'
COUNTRY_CODE1 = 'us'

# COMMAND ----------

# Read POS data
data_vault_data1 = read_pos_data(RETAILER1, CLIENT1, COUNTRY_CODE1, sqlContext).repartition('SALES_DT')
data_vault_data1 = data_vault_data1.where((pyf.col("SALES_DT") < pyf.lit("2020-11-01")) & (pyf.col("SALES_DT") >= pyf.lit("2020-02-23")))

total_sales_by_date1 = data_vault_data1.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_sales_by_date1)

# COMMAND ----------

total_inventory_by_date1 = data_vault_data1.select("SALES_DT", "ON_HAND_INVENTORY_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_inventory_by_date1)

# COMMAND ----------

dbutils.widgets.text('START_DATE', '', 'Start Date (YYYYMMDD)')
dbutils.widgets.dropdown('MODEL_SOURCE', 'prod', ['local', 'prod'], 'Model Source')

if len(dbutils.widgets.get('START_DATE')) > 0:
    start_date = datetime.datetime.strptime(dbutils.widgets.get('START_DATE'), '%Y%m%d')
    
else:
    start_date = None

MODEL_SOURCE = dbutils.widgets.get('MODEL_SOURCE').upper()
MODEL_SOURCE = 'LOCAL' if MODEL_SOURCE.startswith('LOCAL') else 'PROD'

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled", "true")

# COMMAND ----------

champions_path = '/mnt{mod}/artifacts/country_code/champion_models/'.format(
    mod='' if MODEL_SOURCE == 'LOCAL' else '/prod-ro'
)

# COMMAND ----------

items_assigned_with_business_rules_path = '/mnt/artifacts/country_code/reference/Business_Selected_Items.csv'
check_path_exists(items_assigned_with_business_rules_path, 'csv', 'raise')

# COMMAND ----------

# Get retailer, client and country_code tuples from the champion driver
input_data_list = []
for retailer_level_path in dbutils.fs.ls(champions_path):
    retailer_i = retailer_level_path.path.split('retailer=')[1].split('/')[0]
    
    for client_level_path in dbutils.fs.ls(retailer_level_path.path):
        client_i = client_level_path.path.split('client=')[1].split('/')[0]
        
        for country_level_path in dbutils.fs.ls(client_level_path.path):
            country_i = country_level_path.path.split('country_code=')[1].split('/')[0]
            
            input_data_list.append((
                retailer_i,
                client_i,
                country_i
            ))  
input_data_list =  [('walmart', 'truco', 'ca')]
 
print(input_data_list)

# COMMAND ----------

# function to prepare a table for single retailer and client

def build_pos_table(retailer, client, country_code, start_date, end_date):
    """
    Read in predicted POS from Database for both forecast engine and loess
    Join with POS data, retail_item_qty and retail_organization_num
    from Data Vault.

    :param string retailer: the name of the retailer to pull
    :param string client: the name of the client to pull
    :param string start_date: first day to read data
    :param string end_date: last day to read data
    :return DataFrame
    """

    retailer = retailer.strip().upper()
    client = client.strip().upper()
    country_code = country_code.strip().upper()

    if start_date == None:
        start_date = '2019-01-19'

    if end_date == None:
        end_date = (datetime.datetime.now() - datetime.timedelta(1)) \
            .strftime('%Y-%m-%d')

    database_im = '{retailer}_{client}_{country_code}_retail_alert_im' \
        .format(retailer=retailer.strip().lower(),
                client=client.strip().lower(),
                country_code=country_code.strip().lower()
                )
    database_dv = '{retailer}_{client}_{country_code}_dv' \
        .format(retailer=retailer.strip().lower(),
                client=client.strip().lower(),
                country_code=country_code.strip().lower()
                )

    print('Build table for {}_{}_{} from {} to {}'.format(retailer, client, country_code, start_date, end_date))

    sql_statement = """
        SELECT
            HOU.ORGANIZATION_UNIT_NUM,
            HRI.RETAILER_ITEM_ID,
            \'{retailer}\' AS RETAILER,
            \'{client}\' AS CLIENT,    
            \'{country_code}\' AS COUNTRY_CODE,      
            DFBU.SALES_DT,
            VSLES.POS_ITEM_QTY,
            NVL(VSLES.POS_AMT, 0) POS_AMT,
            VSLES.ON_HAND_INVENTORY_QTY,
            DFBU.BASELINE_POS_ITEM_QTY AS DRFE_POS_ITEM_QTY,
            (DFBU.BASELINE_POS_ITEM_QTY - VSLES.POS_ITEM_QTY) AS DRFE_ERROR,
            DFBU.LOAD_TS AS DRFE_LOAD_TS
        FROM
            (SELECT HUB_ORGANIZATION_UNIT_HK,
                    HUB_RETAILER_ITEM_HK,
                    SALES_DT,
                    BASELINE_POS_ITEM_QTY,
                    LOAD_TS,
                    ROW_NUMBER() OVER(partition BY 
                                            HUB_ORGANIZATION_UNIT_HK,
                                            HUB_RETAILER_ITEM_HK,
                                            SALES_DT
                                        ORDER BY LOAD_TS
                                        DESC
                                        ) AS ROW_NUMBER 
             FROM {database_im}.DRFE_FORECAST_BASELINE_UNIT
             WHERE SALES_DT > \'{start_date}\'
             AND SALES_DT <= \'{end_date}\'
            ) DFBU 
        INNER JOIN (SELECT 
                              (CASE WHEN POS_ITEM_QTY < 0 THEN 0 
                              ELSE POS_ITEM_QTY
                              END) POS_ITEM_QTY,
                              (CASE WHEN POS_AMT < 0 THEN 0
                              ELSE POS_AMT
                              END) POS_AMT,
                              ON_HAND_INVENTORY_QTY,
                              HUB_ORGANIZATION_UNIT_HK,
                              HUB_RETAILER_ITEM_HK,
                              SALES_DT
                        FROM
                        {database_dv}.VW_SAT_LINK_EPOS_SUMMARY
                        WHERE SALES_DT > \'{start_date}\'
                        AND SALES_DT <= \'{end_date}\'
                        ) VSLES 
            ON(
                DFBU.HUB_RETAILER_ITEM_HK = VSLES.HUB_RETAILER_ITEM_HK
                AND DFBU.HUB_ORGANIZATION_UNIT_HK = VSLES.HUB_ORGANIZATION_UNIT_HK
                AND DFBU.SALES_DT = VSLES.SALES_DT                
            )
        INNER JOIN {database_dv}.HUB_RETAILER_ITEM HRI
            ON DFBU.HUB_RETAILER_ITEM_HK = HRI.HUB_RETAILER_ITEM_HK
        INNER JOIN {database_dv}.HUB_ORGANIZATION_UNIT HOU 
            ON DFBU.HUB_ORGANIZATION_UNIT_HK = HOU.HUB_ORGANIZATION_UNIT_HK


        WHERE DFBU.ROW_NUMBER = 1
        """
    sql_statement = sql_statement.format(retailer=retailer,
                                         client=client,
                                         country_code=country_code,
                                         database_im=database_im,
                                         database_dv=database_dv,
                                         start_date=start_date,
                                         end_date=end_date
                                         )
    sql_statement = spark.sql(sql_statement)

    return sql_statement


# COMMAND ----------

# Add model type from champion
def add_model_type(champions_path, retailer, client, country_code):
    """Read the champion model, extract the model type and test mean squared error

    :param string champion_path: path to production champion model parquett table
    :param string retailer: the name of the retailer to pull
    :param string client: the name of the client to pull
    :return DataFrame
    """
    retailer = retailer.strip().lower()
    client = client.strip().lower()
    country_code = country_code.strip().lower()

    client_champion_path = champions_path + 'retailer={retailer}/client={client}/country_code={country_code}' \
        .format(retailer=retailer, client=client, country_code=country_code)
    champions = spark.read.format('delta') \
        .load(client_champion_path)

    champion_models = champions.select('RETAILER_ITEM_ID',
                                       pyf.split(pyf.split('MODEL_METADATA',
                                                           '.core.').getItem(
                                           1), ' ').getItem(0) \
                                       .alias('MODEL_TYPE'),
                                       'METRICS_MSE_TEST') \
        .withColumnRenamed('METRICS_MSE_TEST', 'TEST_SET_MSE_PERFORMANCE')

    return champion_models


# COMMAND ----------

# Make an empty table to append
schema = pyt.StructType([
    pyt.StructField('ORGANIZATION_UNIT_NUM', pyt.IntegerType(), True),
    pyt.StructField('RETAILER_ITEM_ID', pyt.StringType(), True),
    pyt.StructField('RETAILER', pyt.StringType(), True),
    pyt.StructField('CLIENT', pyt.StringType(), True),
    pyt.StructField('COUNTRY_CODE', pyt.StringType(), True),
    pyt.StructField('SALES_DT', pyt.StringType(), True),
    pyt.StructField('POS_ITEM_QTY', pyt.DecimalType(), True),
    pyt.StructField('POS_AMT', pyt.DecimalType(), True),
    pyt.StructField('ON_HAND_INVENTORY_QTY', pyt.DecimalType(), True),
    pyt.StructField('DRFE_POS_ITEM_QTY', pyt.DecimalType(), True),
    pyt.StructField('DRFE_ERROR', pyt.DecimalType(), True),
    pyt.StructField('DRFE_LOAD_TS', pyt.TimestampType(), True),
    pyt.StructField('MODEL_TYPE', pyt.StringType(), True),
    pyt.StructField('TEST_SET_MSE_PERFORMANCE', pyt.DecimalType(), True)
])
unioned_df = sqlContext.createDataFrame([], schema)

# find first dates
df = spark.sql("""SELECT * FROM RETAIL_FORECAST_ENGINE.PERFORMANCE""")
startDate_df = df.groupby('RETAILER', 'CLIENT', 'COUNTRY_CODE') \
    .agg(pyf.max('SALES_DT').alias('MAX_DATE')).toPandas()

# Loop over all retailers, client and country_code tuples
for retailer, client, country_code in input_data_list:
    if start_date == None:
        start_date = startDate_df[(startDate_df.RETAILER == retailer.upper()) & (startDate_df.CLIENT == client.upper()) \
            & (startDate_df.COUNTRY_CODE == country_code.upper())]['MAX_DATE'].values.astype('str')[0]    
    retailer_client_df = build_pos_table(
        retailer, client, country_code, start_date, end_date=None
    )
    columns_order = unioned_df.columns
    champions_model = add_model_type(champions_path, retailer, client, country_code)
    retailer_client_df = retailer_client_df.join(
        champions_model,
        ['RETAILER_ITEM_ID'],
        'inner').select(columns_order)
    unioned_df = unioned_df.unionAll(retailer_client_df)

# COMMAND ----------

# Load tables with business rules and join it to the main table
df = spark.read.format('csv') \
    .options(header='true', inferSchema='true') \
    .load(items_assigned_with_business_rules_path)

df = df.withColumn('Product ID', df['Product ID'].cast('string'))

# join
result = unioned_df.join(df,
                         unioned_df.RETAILER_ITEM_ID == df['Product ID'],
                         how='left') \
    .fillna(0, subset=df.columns[:-1]).drop('Product ID')

# COMMAND ----------

display(result)

# COMMAND ----------

total_predicted_sales_by_date = result.select("SALES_DT", "DRFE_POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_predicted_sales_by_date)

# COMMAND ----------

total_actual_sales_by_date = result.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_actual_sales_by_date)

# COMMAND ----------

total_dfre_error_by_date = result.select("SALES_DT", "DRFE_ERROR").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_dfre_error_by_date)

# COMMAND ----------

result.count()
