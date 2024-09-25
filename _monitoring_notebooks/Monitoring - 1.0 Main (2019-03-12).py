# Databricks notebook source
import uuid
import warnings
import pandas as pd
import numpy as np
import datetime

from pyspark.sql import functions as pyf
import pyspark.sql.types as pyt

# COMMAND ----------

champions_path = '/mnt/prod-ro/artifacts/champion_models/'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get retailers and clients name from the blob storage file names

# COMMAND ----------

list_dir = dbutils.fs.ls(champions_path)
file_names = [f for f in list_dir if f[1].startswith('retailer')]

retailer_list = []

for file in file_names:
    retailer_list.append(file[1].split('=')[1].split('/')[0])
  
print(retailer_list)

for retailer in retailer_list:
    list_dir = dbutils.fs.ls('{}retailer={}'.format(champions_path, retailer))

file_names = [f for f in list_dir if f[1].startswith('client')]

client_list = []
for file in file_names:
    client_list.append(file[1].split('=')[1].split('/')[0])
  
print(client_list)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare table

# COMMAND ----------

# function to prepare a table for single retailer and client

def build_pos_table(retailer, client, start_date, end_date):
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
    
    if start_date == None:
        start_date = '2019-01-19'
    
    if end_date == None:
        end_date = (datetime.datetime.now() - datetime.timedelta(1)).strftime('%Y-%m-%d')
        
    database_im = '{retailer}_{client}_retail_alert_im' \
        .format(retailer=retailer.strip().lower(),
                client=client.strip().lower(),
                )
    
    database_dv = '{retailer}_{client}_dv' \
        .format(retailer=retailer.strip().lower(),
                client=client.strip().lower(),
                )
    
    sql_statement = """
        SELECT
        
            HRI.RETAILER_ITEM_ID,
            SRI.RETAILER_ITEM_DESC,
            \'{retailer}\' AS RETAILER,
            \'{client}\' AS CLIENT,
            HOU.ORGANIZATION_UNIT_NUM,     
            DFBU.SALES_DT,
            VSLES.POS_ITEM_QTY,
            VSLES.ON_HAND_INVENTORY_QTY,
            DFBU.BASELINE_POS_ITEM_QTY AS DRFE_POS_ITEM_QTY,
            LFBU.BASELINE_POS_ITEM_QTY AS LEGACY_POS_ITEM_QTY,
            (DFBU.BASELINE_POS_ITEM_QTY - VSLES.POS_ITEM_QTY) AS DRFE_ERROR,
            (LFBU.BASELINE_POS_ITEM_QTY - VSLES.POS_ITEM_QTY) AS LEGACY_ERROR,
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
        LEFT OUTER JOIN 
            (SELECT HUB_ORGANIZATION_UNIT_HK,
                    HUB_RETAILER_ITEM_HK,
                    SALES_DT,
                    BASELINE_POS_ITEM_QTY,
                    ROW_NUMBER() OVER(partition BY 
                                            HUB_ORGANIZATION_UNIT_HK,
                                            HUB_RETAILER_ITEM_HK,
                                            SALES_DT
                                        ORDER BY LOAD_TS
                                        DESC
                                        ) AS ROW_NUMBER 
             FROM {database_im}.LOESS_FORECAST_BASELINE_UNIT
             WHERE SALES_DT > \'{start_date}\'
             AND SALES_DT <= \'{end_date}\'
            ) LFBU
            ON(
                DFBU.HUB_RETAILER_ITEM_HK = LFBU.HUB_RETAILER_ITEM_HK
                AND DFBU.HUB_ORGANIZATION_UNIT_HK = LFBU.HUB_ORGANIZATION_UNIT_HK
                AND DFBU.SALES_DT = LFBU.SALES_DT
            ) 
        INNER JOIN (SELECT 
                              (CASE WHEN POS_ITEM_QTY < 0 THEN 0 
                              ELSE POS_ITEM_QTY
                              END) POS_ITEM_QTY,
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
        --AND LFBU.ROW_NUMBER = 1
        AND SRI.ROW_NUMBER = 1
        """
    sql_statement = sql_statement.format(retailer=retailer,
                                         client=client,
                                         database_im=database_im,
                                         database_dv=database_dv,
                                         start_date=start_date,
                                         end_date =end_date
                                         )
    sql_statement = spark.sql(sql_statement)
    
    return sql_statement

# COMMAND ----------

# Add model type from champion
def add_model_type(champions_path, retailer, client):
    """
    Read the champion model, extract the model type and test mean squared error
    
    :param string champion_path: path to production champion model parquet table
    :param string retailer: the name of the retailer to pull
    :param string client: the name of the client to pull
    :return DataFrame
    """
    retailer = retailer.strip().upper()
    client = client.strip().upper()
    
    champions = spark.read.parquet(champions_path+'retailer={retailer}'.format(retailer=retailer)) \
        .filter("RETAILER == '{retailer}' and CLIENT == '{client}'".format(retailer=retailer, client=client))\
        .drop("RETAILER", "CLIENT")
    champion_models = champions.select("ORGANIZATION_UNIT_NUM", "RETAILER_ITEM_ID", 
                                       pyf.split(pyf.col("MODEL_METADATA"), "\(")\
                                       .getItem(0).alias("MODEL_TYPE"), 
                                       "TEST_SET_MSE_PERFORMANCE"
                                      )
    
    return champion_models

# COMMAND ----------

# Make an empty table to append
schema = pyt.StructType([
    pyt.StructField("ORGANIZATION_UNIT_NUM", pyt.IntegerType(), True),
    pyt.StructField("RETAILER_ITEM_ID", pyt.StringType(), True),
    pyt.StructField("RETAILER_ITEM_DESC", pyt.StringType(), True),
    pyt.StructField("RETAILER", pyt.StringType(), True),
    pyt.StructField("CLIENT", pyt.StringType(), True),
    pyt.StructField("SALES_DT", pyt.StringType(), True),
    pyt.StructField("POS_ITEM_QTY", pyt.DecimalType() , True),
    pyt.StructField("ON_HAND_INVENTORY_QTY", pyt.DecimalType() , True),
    pyt.StructField("DRFE_POS_ITEM_QTY", pyt.DecimalType() , True),
    pyt.StructField("LEGACY_POS_ITEM_QTY", pyt.DecimalType() , True),
    pyt.StructField("DRFE_ERROR", pyt.DecimalType() , True),
    pyt.StructField("LEGACY_ERROR", pyt.DecimalType() , True),
    pyt.StructField("DRFE_LOAD_TS", pyt.TimestampType(), True),
    pyt.StructField("MODEL_TYPE", pyt.StringType(), True),
    pyt.StructField("TEST_SET_MSE_PERFORMANCE", pyt.DecimalType(), True)
])
unioned_df = sqlContext.createDataFrame([], schema)


# Find the first date
df = spark.sql("""SELECT * FROM RETAIL_FORECAST_ENGINE.RESULTS_SUMMARY""")
startDate_df = df.groupby("RETAILER", "CLIENT").agg(pyf.max("SALES_DT").alias("MAX_DATE")).toPandas()

# Loop over all retailers and clients
for retailer in retailer_list:
    for client in client_list:
        start_date = startDate_df[(startDate_df.RETAILER == retailer) & (startDate_df.CLIENT == client)]['MAX_DATE'].values.astype('str')[0]
        print('Build table for {}_{} from {}'.format(retailer, client, start_date))
        retailer_client_df =  build_pos_table(retailer, client, start_date, end_date=None)
        champions_model = add_model_type(champions_path, retailer, client)
        retailer_client_df = retailer_client_df.join(champions_model, ["ORGANIZATION_UNIT_NUM", "RETAILER_ITEM_ID"], "inner")
        unioned_df = unioned_df.unionAll(retailer_client_df)

# COMMAND ----------

#Load tables with business rules and join it to the main table
df = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load('/mnt/artifacts/reference/Business_Selected_Items.csv')

df = df.withColumn('Product ID', df['Product ID'].cast('string'))

# join
result = unioned_df.join(df, unioned_df.RETAILER_ITEM_ID == df['Product ID'], how='left').fillna(0, subset=df.columns[:-1]).drop('Product ID')

# COMMAND ----------

display(result)

# COMMAND ----------

result.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###DDL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS RETAIL_FORECAST_ENGINE.RESULTS_SUMMARY;
# MAGIC -- CREATE TABLE RETAIL_FORECAST_ENGINE.RESULTS_SUMMARY
# MAGIC --     (ORGANIZATION_UNIT_NUM STRING,
# MAGIC --     RETAILER_ITEM_ID STRING,
# MAGIC --     RETAILER_ITEM_DESC STRING,
# MAGIC --     RETAILER STRING,
# MAGIC --     CLIENT STRING,
# MAGIC --     SALES_DT DATE,
# MAGIC --     POS_ITEM_QTY FLOAT,
# MAGIC --     ON_HAND_INVENTORY_QTY FLOAT,
# MAGIC --     DRFE_POS_ITEM_QTY FLOAT,
# MAGIC --     LEGACY_POS_ITEM_QTY FLOAT,
# MAGIC --     DRFE_ERROR FLOAT,
# MAGIC --     LEGACY_ERROR FLOAT,
# MAGIC --     DRFE_LOAD_TS TIMESTAMP,
# MAGIC --     MODEL_TYPE STRING,
# MAGIC --     TEST_SET_MSE_PERFORMANCE FLOAT,
# MAGIC --     HIGH_DOLLAR_SALES STRING,
# MAGIC --     HIGH_FREQUENCY STRING,
# MAGIC --     LOW_FREQUENCY STRING
# MAGIC --     )
# MAGIC --     USING DELTA
# MAGIC --     PARTITIONED BY (RETAILER, CLIENT)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### New Clients
# MAGIC For new clients insert into the table. It is faster

# COMMAND ----------

## New Clients
##For new clients insert to the table. It is faster
##enable it only if we have a new client or we want to add a huge table with no repeatition to the table
#result.write.mode("append").insertInto("RETAIL_FORECAST_ENGINE.RESULTS_SUMMARY")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Updating the table

# COMMAND ----------

result.createOrReplaceTempView('CURRENT_RESULTS')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO RETAIL_FORECAST_ENGINE.RESULTS_SUMMARY MON
# MAGIC USING CURRENT_RESULTS RES
# MAGIC ON (MON.SALES_DT = RES.SALES_DT
# MAGIC    AND MON.RETAILER_ITEM_ID = RES.RETAILER_ITEM_ID
# MAGIC    AND MON.CLIENT = RES.CLIENT
# MAGIC    AND MON.RETAILER = RES.RETAILER
# MAGIC    AND MON.ORGANIZATION_UNIT_NUM = RES.ORGANIZATION_UNIT_NUM
# MAGIC    )
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC   MON.RETAILER_ITEM_DESC = RES.RETAILER_ITEM_DESC,
# MAGIC   MON.POS_ITEM_QTY = RES.POS_ITEM_QTY,
# MAGIC   MON.DRFE_POS_ITEM_QTY = RES.DRFE_POS_ITEM_QTY,
# MAGIC   MON.ON_HAND_INVENTORY_QTY = RES.ON_HAND_INVENTORY_QTY,
# MAGIC   MON.LEGACY_POS_ITEM_QTY = RES.LEGACY_POS_ITEM_QTY,
# MAGIC   MON.DRFE_ERROR = RES.DRFE_ERROR,
# MAGIC   MON.LEGACY_ERROR = RES.LEGACY_ERROR,
# MAGIC   MON.DRFE_LOAD_TS = RES.DRFE_LOAD_TS,
# MAGIC   MON.MODEL_TYPE = RES.MODEL_TYPE,
# MAGIC   MON.TEST_SET_MSE_PERFORMANCE = RES.TEST_SET_MSE_PERFORMANCE,
# MAGIC   MON.High_Dollar_Sales = RES.High_Dollar_Sales,
# MAGIC   MON.High_Frequency = RES.High_Frequency,
# MAGIC   MON.Low_Frequency = RES.Low_Frequency
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (RETAILER_ITEM_ID, RETAILER_ITEM_DESC, RETAILER, CLIENT, ORGANIZATION_UNIT_NUM, SALES_DT, POS_ITEM_QTY, 
# MAGIC               ON_HAND_INVENTORY_QTY, DRFE_POS_ITEM_QTY, LEGACY_POS_ITEM_QTY, DRFE_ERROR, LEGACY_ERROR, DRFE_LOAD_TS, MODEL_TYPE, 
# MAGIC               TEST_SET_MSE_PERFORMANCE, High_Dollar_Sales, High_Frequency, Low_Frequency
# MAGIC               )
# MAGIC   VALUES (RETAILER_ITEM_ID, RETAILER_ITEM_DESC, RETAILER, CLIENT, ORGANIZATION_UNIT_NUM, SALES_DT, POS_ITEM_QTY, 
# MAGIC               ON_HAND_INVENTORY_QTY, DRFE_POS_ITEM_QTY, LEGACY_POS_ITEM_QTY, DRFE_ERROR, LEGACY_ERROR, DRFE_LOAD_TS, MODEL_TYPE,
# MAGIC               TEST_SET_MSE_PERFORMANCE, High_Dollar_Sales, High_Frequency, Low_Frequency
# MAGIC               )
# MAGIC    

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE RETAIL_FORECAST_ENGINE.RESULTS_SUMMARY
