# Databricks notebook source
retailer_client = "walmart_barillaamericainc_us"

# COMMAND ----------

# MAGIC %md
# MAGIC # New LSV derivation

# COMMAND ----------

viewDdl = """
WITH CALC_LSV AS 
   (
    
    SELECT
      vw_sat_lnk.HUB_Organization_Unit_HK,
      vw_sat_lnk.HUB_Retailer_Item_HK,
      CURRENT_TIMESTAMP AS LOAD_TS,
      cast(CASE 
        WHEN vw_sat_unt_prc.UNIT_PRICE_AMT IS NULL THEN 0
        WHEN fcst_unt.BASELINE_POS_ITEM_QTY IS NULL THEN 0
        WHEN vw_sat_lnk.POS_ITEM_QTY IS NULL THEN 0
        WHEN vw_sat_lnk.POS_ITEM_QTY >= fcst_unt.BASELINE_POS_ITEM_QTY THEN (fcst_unt.BASELINE_POS_ITEM_QTY - vw_sat_lnk.POS_ITEM_QTY) * vw_sat_unt_prc.UNIT_PRICE_AMT -- Replaced this with 0
        WHEN vw_sat_lnk.POS_ITEM_QTY < fcst_unt.BASELINE_POS_ITEM_QTY THEN (fcst_unt.BASELINE_POS_ITEM_QTY - vw_sat_lnk.POS_ITEM_QTY) * vw_sat_unt_prc.UNIT_PRICE_AMT
      ELSE 0 END as decimal(15,2)) AS LOST_SALES_AMT,
      vw_sat_lnk.sales_dt AS SALES_DT

    
    FROM {0}_dv.vw_sat_link_epos_summary vw_sat_lnk 
    INNER JOIN {1}_dv.vw_sat_retailer_item_unit_price vw_sat_unt_prc
      ON vw_sat_lnk.HUB_ORGANIZATION_UNIT_HK = vw_sat_unt_prc.HUB_ORGANIZATION_UNIT_HK and 
      vw_sat_lnk.HUB_RETAILER_ITEM_HK = vw_sat_unt_prc.HUB_RETAILER_ITEM_HK and 
      vw_sat_lnk.sales_dt = vw_sat_unt_prc.sales_dt 
    
    INNER JOIN {2}_retail_alert_im.DRFE_FORECAST_BASELINE_UNIT fcst_unt 
      ON fcst_unt.sales_dt = vw_sat_lnk.sales_dt and 
      fcst_unt.HUB_Organization_Unit_HK = vw_sat_lnk.HUB_Organization_Unit_HK and 
      fcst_unt.HUB_Retailer_Item_HK = vw_sat_lnk.HUB_Retailer_Item_HK
    
    )
    
select * from CALC_LSV
""".format(retailer_client, retailer_client, retailer_client)

# COMMAND ----------

spark.sql(viewDdl).createOrReplaceTempView("tmp_vw_lost_sales_value_new")

# COMMAND ----------

# MAGIC %md
# MAGIC # Get OSA Slow Sales data with the new LSV Derivation

# COMMAND ----------

sqlOsaSlowSales = """
SELECT
  llsv.HUB_ORGANIZATION_UNIT_HK
  , llsv.HUB_RETAILER_ITEM_HK
  , "Slow Sales" AS ALERT_MESSAGE_DESC
  , llsv.SALES_DT
  , sum(adsd.LOST_SALES_AMT) as LOST_SALES_AMT
  
FROM {0}_dv.vw_sat_link_epos_summary sles

INNER JOIN (select * from tmp_vw_lost_sales_value_new) llsv -- Pulling data from the new LSV derivation TMP View
  ON sles.HUB_ORGANIZATION_UNIT_HK = llsv.HUB_ORGANIZATION_UNIT_HK
  AND sles.HUB_RETAILER_ITEM_HK = llsv.HUB_RETAILER_ITEM_HK
  AND sles.SALES_DT = llsv.SALES_DT
  
INNER JOIN (select * from tmp_vw_lost_sales_value_new) adsd -- Pulling data from the new LSV derivation TMP View
  ON adsd.HUB_ORGANIZATION_UNIT_HK = llsv.HUB_ORGANIZATION_UNIT_HK
  AND adsd.HUB_RETAILER_ITEM_HK = llsv.HUB_RETAILER_ITEM_HK
  AND adsd.SALES_DT between llsv.SALES_DT - interval 1 day and llsv.SALES_DT

GROUP BY
  llsv.HUB_ORGANIZATION_UNIT_HK
  , llsv.HUB_RETAILER_ITEM_HK
  , llsv.SALES_DT

HAVING
  sum(adsd.LOST_SALES_AMT) > 0

ORDER BY
  llsv.HUB_ORGANIZATION_UNIT_HK
  , llsv.HUB_RETAILER_ITEM_HK
  , llsv.SALES_DT
  
""".format(retailer_client)

# COMMAND ----------

spark.sql(sqlOsaSlowSales).createOrReplaceTempView("tmp_vw_alert_on_shelf_availability_new")

# COMMAND ----------

sqlCountOsaAlerts = """
select

(select
  count(*)
from {0}_retail_alert_im.alert_on_shelf_availability
where
  SALES_DT between date('2020-06-01') and date('2020-09-05') and
  ALERT_MESSAGE_DESC = 'Slow Sales' and
  LOST_SALES_AMT >= 7) num_slow_sales_alerts_using_old_lsv,

(select
  count(*)
from {1}_retail_alert_im.alert_on_shelf_availability
where
  SALES_DT between date('2020-06-01') and date('2020-09-05') and
  ALERT_MESSAGE_DESC <> 'Slow Sales') num_non_slow_sales_alerts_using_old_lsv,

(select
  count(*)
from tmp_vw_alert_on_shelf_availability_new
where
  SALES_DT between date('2020-06-01') and date('2020-09-05') and
  ALERT_MESSAGE_DESC = 'Slow Sales' and
  LOST_SALES_AMT >= 7) num_osa_alerts_using_new_lsv

""".format(retailer_client, retailer_client)

results = spark.sql(sqlCountOsaAlerts)

# COMMAND ----------

# display(results)

# COMMAND ----------

results.coalesce(1).write.format('csv').save("/mnt/processed/hugh/lsv-analysis/{0}".format(retailer_client), header='true')

# COMMAND ----------



# COMMAND ----------

min_and_max_alert_dates = """
select min(sales_dt), max(sales_dt)
from {}_retail_alert_im.alert_on_shelf_availability

""".format(retailer_client)

# COMMAND ----------

display(spark.sql(min_and_max_alert_dates))

# COMMAND ----------


