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

SOURCE_SYSTEM = 'bigred'
RETAILER = 'target'
CLIENT = 'wildcat'
COUNTRY_CODE = 'us'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use retail_alert_target_wildcat_us_im;
# MAGIC
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc retail_alert_target_wildcat_us_im.drfe_forecast_baseline_unit

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from retail_alert_target_wildcat_us_im.lost_sales_value

# COMMAND ----------

df_sql_query_retail_alert_target_wildcat_us_im = """
  
SELECT
  'Target' as retailer,
  'Harrys' as client,
  substr(sales_dt, 1, 7) AS call_month,
  SUM(LOST_SALES_AMT) as total_lsv

FROM 
  retail_alert_target_wildcat_us_im.lost_sales_value

GROUP BY
  client,
  retailer,
  call_month
  ORDER BY
  call_month
"""

# COMMAND ----------

df_retail_alert_target_wildcat_us_im = spark.sql(df_sql_query_retail_alert_target_wildcat_us_im)
display(df_retail_alert_target_wildcat_us_im)

# COMMAND ----------

# MAGIC %sql    
# MAGIC   
# MAGIC   SELECT
# MAGIC       vw_sat_lnk.HUB_Organization_Unit_HK,
# MAGIC       vw_sat_lnk.HUB_Retailer_Item_HK,
# MAGIC       CURRENT_TIMESTAMP AS LOAD_TS,
# MAGIC       CASE 
# MAGIC         WHEN vw_sat_unt_prc.UNIT_PRICE_AMT IS NULL THEN 0
# MAGIC         WHEN fcst_unt.BASELINE_POS_ITEM_QTY IS NULL THEN 0
# MAGIC         WHEN vw_sat_lnk.POS_ITEM_QTY IS NULL THEN 0
# MAGIC         WHEN vw_sat_lnk.POS_ITEM_QTY >= fcst_unt.BASELINE_POS_ITEM_QTY THEN 0
# MAGIC         WHEN vw_sat_lnk.POS_ITEM_QTY < fcst_unt.BASELINE_POS_ITEM_QTY THEN (fcst_unt.BASELINE_POS_ITEM_QTY - vw_sat_lnk.POS_ITEM_QTY) * vw_sat_unt_prc.UNIT_PRICE_AMT
# MAGIC       ELSE 0 END AS LOST_SALES_AMT,
# MAGIC       vw_sat_lnk.sales_dt AS SALES_DT
# MAGIC     
# MAGIC     FROM bigred_target_wildcat_us_dv.vw_latest_sat_epos_summary vw_sat_lnk 
# MAGIC     INNER JOIN bigred_target_wildcat_us_dv.sat_retailer_item_unit_price vw_sat_unt_prc  
# MAGIC       ON vw_sat_lnk.HUB_ORGANIZATION_UNIT_HK = vw_sat_unt_prc.HUB_ORGANIZATION_UNIT_HK and 
# MAGIC       vw_sat_lnk.HUB_RETAILER_ITEM_HK = vw_sat_unt_prc.HUB_RETAILER_ITEM_HK and 
# MAGIC       vw_sat_lnk.sales_dt = vw_sat_unt_prc.sales_dt 
# MAGIC     
# MAGIC     INNER JOIN retail_alert_target_wildcat_us_im.drfe_FORECAST_BASELINE_UNIT fcst_unt 
# MAGIC       ON fcst_unt.sales_dt = vw_sat_lnk.sales_dt and 
# MAGIC       fcst_unt.HUB_Organization_Unit_HK = vw_sat_lnk.HUB_Organization_Unit_HK and 
# MAGIC       fcst_unt.HUB_Retailer_Item_HK = vw_sat_lnk.HUB_Retailer_Item_HK

# COMMAND ----------

# MAGIC %sql    
# MAGIC   
# MAGIC   SELECT
# MAGIC       vw_sat_lnk.HUB_Organization_Unit_HK,
# MAGIC       vw_sat_lnk.HUB_Retailer_Item_HK,
# MAGIC       CURRENT_TIMESTAMP AS LOAD_TS,
# MAGIC       CASE 
# MAGIC         WHEN vw_sat_unt_prc.UNIT_PRICE_AMT IS NULL THEN 0
# MAGIC         WHEN fcst_unt.BASELINE_POS_ITEM_QTY IS NULL THEN 0
# MAGIC         WHEN vw_sat_lnk.POS_ITEM_QTY IS NULL THEN 0
# MAGIC       ELSE (fcst_unt.BASELINE_POS_ITEM_QTY - vw_sat_lnk.POS_ITEM_QTY) * vw_sat_unt_prc.UNIT_PRICE_AMT
# MAGIC       END AS LOST_SALES_AMT,
# MAGIC       vw_sat_lnk.sales_dt AS SALES_DT
# MAGIC     
# MAGIC     FROM bigred_target_wildcat_us_dv.vw_latest_sat_epos_summary vw_sat_lnk 
# MAGIC     INNER JOIN bigred_target_wildcat_us_dv.sat_retailer_item_unit_price vw_sat_unt_prc  
# MAGIC       ON vw_sat_lnk.HUB_ORGANIZATION_UNIT_HK = vw_sat_unt_prc.HUB_ORGANIZATION_UNIT_HK and 
# MAGIC       vw_sat_lnk.HUB_RETAILER_ITEM_HK = vw_sat_unt_prc.HUB_RETAILER_ITEM_HK and 
# MAGIC       vw_sat_lnk.sales_dt = vw_sat_unt_prc.sales_dt 
# MAGIC     
# MAGIC     INNER JOIN retail_alert_target_wildcat_us_im.drfe_FORECAST_BASELINE_UNIT fcst_unt 
# MAGIC       ON fcst_unt.sales_dt = vw_sat_lnk.sales_dt and 
# MAGIC       fcst_unt.HUB_Organization_Unit_HK = vw_sat_lnk.HUB_Organization_Unit_HK and 
# MAGIC       fcst_unt.HUB_Retailer_Item_HK = vw_sat_lnk.HUB_Retailer_Item_HK
# MAGIC     
# MAGIC     WHERE vw_sat_lnk.sales_dt = '2022-08-01'

# COMMAND ----------

df_sql_query_retail_alert_target_wildcat_us_im2 = """
  
SELECT
  'Target' as retailer,
  'Harrys' as client,
  substr(sales_dt, 1, 7) AS call_month,
  SUM(LOST_SALES_AMT) as total_lsv

FROM (
    SELECT
      vw_sat_lnk.HUB_Organization_Unit_HK,
      vw_sat_lnk.HUB_Retailer_Item_HK,
      CURRENT_TIMESTAMP AS LOAD_TS,
      CASE 
        WHEN vw_sat_unt_prc.UNIT_PRICE_AMT IS NULL THEN 0
        WHEN fcst_unt.BASELINE_POS_ITEM_QTY IS NULL THEN 0
        WHEN vw_sat_lnk.POS_ITEM_QTY IS NULL THEN 0
      ELSE (fcst_unt.BASELINE_POS_ITEM_QTY - vw_sat_lnk.POS_ITEM_QTY) * vw_sat_unt_prc.UNIT_PRICE_AMT
      END AS LOST_SALES_AMT,
      vw_sat_lnk.sales_dt AS SALES_DT
    
    FROM bigred_target_wildcat_us_dv.vw_latest_sat_epos_summary vw_sat_lnk 
    INNER JOIN bigred_target_wildcat_us_dv.sat_retailer_item_unit_price vw_sat_unt_prc  
      ON vw_sat_lnk.HUB_ORGANIZATION_UNIT_HK = vw_sat_unt_prc.HUB_ORGANIZATION_UNIT_HK and 
      vw_sat_lnk.HUB_RETAILER_ITEM_HK = vw_sat_unt_prc.HUB_RETAILER_ITEM_HK and 
      vw_sat_lnk.sales_dt = vw_sat_unt_prc.sales_dt 
    
    INNER JOIN retail_alert_target_wildcat_us_im.drfe_FORECAST_BASELINE_UNIT fcst_unt 
      ON fcst_unt.sales_dt = vw_sat_lnk.sales_dt and 
      fcst_unt.HUB_Organization_Unit_HK = vw_sat_lnk.HUB_Organization_Unit_HK and 
      fcst_unt.HUB_Retailer_Item_HK = vw_sat_lnk.HUB_Retailer_Item_HK
    )
GROUP BY
  client,
  retailer,
  call_month
  ORDER BY
  call_month
"""

# COMMAND ----------

df_retail_alert_target_wildcat_us_im2 = spark.sql(df_sql_query_retail_alert_target_wildcat_us_im2)
display(df_retail_alert_target_wildcat_us_im2)

# COMMAND ----------


