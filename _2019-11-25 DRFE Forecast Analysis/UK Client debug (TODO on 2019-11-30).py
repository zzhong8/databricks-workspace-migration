# Databricks notebook source
import uuid
import warnings
import pandas as pd
import numpy as np
import datetime

from pyspark.sql import functions as pyf
from pyspark.sql.functions import col

# COMMAND ----------

# function to prepare a table for single retailer and client

def build_pos_table(retailer, client, country_code, start_date, end_date):

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
        INNER JOIN (SELECT POS_ITEM_QTY,
                          POS_AMT,
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

df = build_pos_table(retailer='SAINSBURYS', client='KRAFTHEINZ', country_code='Uk', start_date='2019-06-26', end_date=None)

# COMMAND ----------

df.cache().count()

# COMMAND ----------

display(df.select('SALES_DT').distinct().orderBy('SALES_DT', ascending=False))

# COMMAND ----------

display(df)

# COMMAND ----------

item_list = df.select('RETAILER_ITEM_ID').distinct().toPandas()

item_list_sample = item_list.sample(frac=0.1)
item_list_df = spark.createDataFrame(item_list_sample)

sampled_item_clorox = df.join(item_list_df,
                               df['RETAILER_ITEM_ID']==item_list_df['RETAILER_ITEM_ID'],
                               'left_semi')

# COMMAND ----------

df.coalesce(1)\
  .write.format('com.databricks.spark.csv')\
  .option('header', 'true')\
  .mode('overwrite')\
  .save(f'/mnt/artifacts/tara/random/uk_diagnosis/retailer=sainsbury/client=kraftheinz/full/')

# COMMAND ----------


