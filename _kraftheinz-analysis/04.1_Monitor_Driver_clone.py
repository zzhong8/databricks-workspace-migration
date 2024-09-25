# Databricks notebook source
import uuid
import warnings
import pandas as pd
import numpy as np
import datetime

from pyspark.sql import functions as pyf
import pyspark.sql.types as pyt
import acosta
from acosta.alerting.helpers import check_path_exists

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
input_data_list = [('asda', 'kraftheinz', 'uk'), ('asda', 'nestlecereals', 'uk'), ('walmart', 'rbusahealth', 'us'), ('walmart', 'rbusahygiene', 'us')]
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
            SRI.RETAILER_ITEM_DESC,
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
        INNER JOIN 
            (SELECT HUB_RETAILER_ITEM_HK,
                    RETAILER_ITEM_DESC,
                    ROW_NUMBER() OVER(partition BY 
                                        HUB_RETAILER_ITEM_HK
                                        ORDER BY LOAD_TS
                                        DESC
                                        ) AS ROW_NUMBER 
            FROM {database_dv}.SAT_RETAILER_ITEM
            ) SRI
            ON DFBU.HUB_RETAILER_ITEM_HK = SRI.HUB_RETAILER_ITEM_HK

        WHERE DFBU.ROW_NUMBER = 1
        AND SRI.ROW_NUMBER = 1
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
    pyt.StructField('RETAILER_ITEM_DESC', pyt.StringType(), True),
    pyt.StructField('RETAILER', pyt.StringType(), True),
    pyt.StructField('CLIENT', pyt.StringType(), True),
    pyt.StructField('COUNTRY_CODE', pyt.StringType(), True),
    pyt.StructField('SALES_DT', pyt.TimestampType(), True),
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

kh = unioned_df.filter('CLIENT == "KRAFTHEINZ"')

# COMMAND ----------

display(kh)

# COMMAND ----------

nestle = unioned_df.filter('CLIENT == "NESTLECEREALS"')

# COMMAND ----------

display(nestle)

# COMMAND ----------

rbusahealth = unioned_df.filter('CLIENT == "RBUSAHEALTH"')

# COMMAND ----------

display(rbusahealth)

# COMMAND ----------

rbusahygiene = unioned_df.filter('CLIENT == "RBUSAHYGIENE"')

# COMMAND ----------

display(rbusahygiene) 

# COMMAND ----------

# # Load tables with business rules and join it to the main table
# df = spark.read.format('csv') \
#     .options(header='true', inferSchema='true') \
#     .load(items_assigned_with_business_rules_path)

# df = df.withColumn('Product ID', df['Product ID'].cast('string'))

# # join
# result = unioned_df.join(df,
#                          unioned_df.RETAILER_ITEM_ID == df['Product ID'],
#                          how='left') \
#     .fillna(0, subset=df.columns[:-1]).drop('Product ID')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Updating the table

# COMMAND ----------

# result.createOrReplaceTempView('CURRENT_RESULTS')

# COMMAND ----------

# %sql
# MERGE INTO RETAIL_FORECAST_ENGINE.PERFORMANCE MON
# USING CURRENT_RESULTS RES
# ON (MON.SALES_DT = RES.SALES_DT
#    AND MON.RETAILER_ITEM_ID = RES.RETAILER_ITEM_ID
#    AND MON.CLIENT = RES.CLIENT
#    AND MON.RETAILER = RES.RETAILER
#    AND MON.COUNTRY_CODE = RES.COUNTRY_CODE
#    AND MON.ORGANIZATION_UNIT_NUM = RES.ORGANIZATION_UNIT_NUM
#    )
# WHEN MATCHED THEN
#   UPDATE SET
#   MON.RETAILER_ITEM_DESC = RES.RETAILER_ITEM_DESC,
#   MON.POS_ITEM_QTY = RES.POS_ITEM_QTY,
#   MON.POS_AMT = RES.POS_AMT,
#   MON.DRFE_POS_ITEM_QTY = RES.DRFE_POS_ITEM_QTY,
#   MON.ON_HAND_INVENTORY_QTY = RES.ON_HAND_INVENTORY_QTY,
#   MON.DRFE_ERROR = RES.DRFE_ERROR,
#   MON.DRFE_LOAD_TS = RES.DRFE_LOAD_TS,
#   MON.MODEL_TYPE = RES.MODEL_TYPE,
#   MON.TEST_SET_MSE_PERFORMANCE = RES.TEST_SET_MSE_PERFORMANCE,
#   MON.High_Dollar_Sales = RES.High_Dollar_Sales,
#   MON.High_Frequency = RES.High_Frequency,
#   MON.Low_Frequency = RES.Low_Frequency
# WHEN NOT MATCHED
#   THEN INSERT (RETAILER_ITEM_ID, RETAILER_ITEM_DESC, RETAILER, CLIENT,
#                 COUNTRY_CODE, ORGANIZATION_UNIT_NUM, SALES_DT, POS_ITEM_QTY,
#                 POS_AMT, ON_HAND_INVENTORY_QTY, DRFE_POS_ITEM_QTY,
#                 DRFE_ERROR, DRFE_LOAD_TS, MODEL_TYPE,
#                 TEST_SET_MSE_PERFORMANCE,
#                 High_Dollar_Sales, High_Frequency, Low_Frequency
#               )
#   VALUES (RES.RETAILER_ITEM_ID, RES.RETAILER_ITEM_DESC, RES.RETAILER, RES.CLIENT,
#           RES.COUNTRY_CODE, RES.ORGANIZATION_UNIT_NUM, RES.SALES_DT, RES.POS_ITEM_QTY,
#           RES.POS_AMT, RES.ON_HAND_INVENTORY_QTY, RES.DRFE_POS_ITEM_QTY,
#           RES.DRFE_ERROR, RES.DRFE_LOAD_TS, RES.MODEL_TYPE,
#           RES.TEST_SET_MSE_PERFORMANCE, RES.High_Dollar_Sales,
#           RES.High_Frequency, RES.Low_Frequency
#           )

# COMMAND ----------

# %sql
# OPTIMIZE RETAIL_FORECAST_ENGINE.PERFORMANCE
# ZORDER BY RETAILER_ITEM_ID, SALES_DT

# COMMAND ----------


