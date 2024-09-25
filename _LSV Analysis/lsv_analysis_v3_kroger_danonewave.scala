// Databricks notebook source
spark.sql("select current_timestamp").rdd.collect

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from kroger_danonewave_us_retail_alert_im.alert_inventory_cleanup limit 10

// COMMAND ----------

// MAGIC %md
// MAGIC # Current LSV derivation

// COMMAND ----------

// MAGIC %sql
// MAGIC WITH CALC_LSV AS 
// MAGIC    (
// MAGIC     SELECT
// MAGIC       vw_sat_lnk.HUB_Organization_Unit_HK,
// MAGIC       vw_sat_lnk.HUB_Retailer_Item_HK,
// MAGIC       CURRENT_TIMESTAMP AS LOAD_TS,
// MAGIC       CASE 
// MAGIC         WHEN vw_sat_unt_prc.UNIT_PRICE_AMT IS NULL THEN 0
// MAGIC         WHEN fcst_unt.BASELINE_POS_ITEM_QTY IS NULL THEN 0
// MAGIC         WHEN vw_sat_lnk.POS_ITEM_QTY IS NULL THEN 0
// MAGIC         WHEN vw_sat_lnk.POS_ITEM_QTY >= fcst_unt.BASELINE_POS_ITEM_QTY THEN 0
// MAGIC         WHEN vw_sat_lnk.POS_ITEM_QTY < fcst_unt.BASELINE_POS_ITEM_QTY THEN (fcst_unt.BASELINE_POS_ITEM_QTY - vw_sat_lnk.POS_ITEM_QTY) * vw_sat_unt_prc.UNIT_PRICE_AMT
// MAGIC       ELSE 0 END AS LOST_SALES_AMT,
// MAGIC       vw_sat_lnk.sales_dt AS SALES_DT
// MAGIC
// MAGIC     FROM kroger_danonewave_us_dv.vw_sat_link_epos_summary vw_sat_lnk 
// MAGIC     INNER JOIN kroger_danonewave_us_dv.VW_SAT_RETAILER_ITEM_UNIT_PRICE vw_sat_unt_prc
// MAGIC       ON vw_sat_lnk.HUB_ORGANIZATION_UNIT_HK = vw_sat_unt_prc.HUB_ORGANIZATION_UNIT_HK and 
// MAGIC       vw_sat_lnk.HUB_RETAILER_ITEM_HK = vw_sat_unt_prc.HUB_RETAILER_ITEM_HK and 
// MAGIC       vw_sat_lnk.sales_dt = vw_sat_unt_prc.sales_dt 
// MAGIC     
// MAGIC     INNER JOIN kroger_danonewave_us_retail_alert_im.DRFE_FORECAST_BASELINE_UNIT fcst_unt 
// MAGIC       ON fcst_unt.sales_dt = vw_sat_lnk.sales_dt and 
// MAGIC       fcst_unt.HUB_Organization_Unit_HK = vw_sat_lnk.HUB_Organization_Unit_HK and 
// MAGIC       fcst_unt.HUB_Retailer_Item_HK = vw_sat_lnk.HUB_Retailer_Item_HK
// MAGIC    
// MAGIC    )
// MAGIC
// MAGIC select * from CALC_LSV
// MAGIC where
// MAGIC SALES_DT = date('2020-09-01')
// MAGIC limit 100

// COMMAND ----------

// MAGIC %md
// MAGIC # New LSV derivation

// COMMAND ----------

val viewDdl = """
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

    
    FROM kroger_danonewave_us_dv.vw_sat_link_epos_summary vw_sat_lnk 
    INNER JOIN kroger_danonewave_us_dv.VW_SAT_RETAILER_ITEM_UNIT_PRICE vw_sat_unt_prc
      ON vw_sat_lnk.HUB_ORGANIZATION_UNIT_HK = vw_sat_unt_prc.HUB_ORGANIZATION_UNIT_HK and 
      vw_sat_lnk.HUB_RETAILER_ITEM_HK = vw_sat_unt_prc.HUB_RETAILER_ITEM_HK and 
      vw_sat_lnk.sales_dt = vw_sat_unt_prc.sales_dt 
    
    INNER JOIN kroger_danonewave_us_retail_alert_im.DRFE_FORECAST_BASELINE_UNIT fcst_unt 
      ON fcst_unt.sales_dt = vw_sat_lnk.sales_dt and 
      fcst_unt.HUB_Organization_Unit_HK = vw_sat_lnk.HUB_Organization_Unit_HK and 
      fcst_unt.HUB_Retailer_Item_HK = vw_sat_lnk.HUB_Retailer_Item_HK
    
    )
    
select * from CALC_LSV
"""

// COMMAND ----------

spark.sql(viewDdl).createOrReplaceTempView("tmp_vw_kroger_danonewave_lost_sales_value_new")

// COMMAND ----------

// MAGIC %md
// MAGIC # Select data from the new LSV derivation Temp View

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from tmp_vw_kroger_danonewave_lost_sales_value_new
// MAGIC where
// MAGIC SALES_DT = date('2020-09-01')
// MAGIC limit 100

// COMMAND ----------

// MAGIC %md
// MAGIC # Compare OLD and NEW Lsv derivation

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC SELECT
// MAGIC old_lsv.sales_dt,
// MAGIC old_lsv.HUB_Organization_Unit_HK,
// MAGIC old_lsv.HUB_Retailer_Item_HK,
// MAGIC old_lsv.LOST_SALES_AMT as OLD_LOST_SALES_AMT,
// MAGIC new_lsv.LOST_SALES_AMT as NEW_LOST_SALES_AMT
// MAGIC from
// MAGIC
// MAGIC kroger_danonewave_us_retail_alert_im.LOST_SALES_VALUE old_lsv,
// MAGIC tmp_vw_kroger_danonewave_lost_sales_value_new new_lsv
// MAGIC
// MAGIC where
// MAGIC
// MAGIC old_lsv.sales_dt = new_lsv.sales_dt and
// MAGIC old_lsv.HUB_Organization_Unit_HK = new_lsv.HUB_Organization_Unit_HK and
// MAGIC old_lsv.HUB_Retailer_Item_HK = new_lsv.HUB_Retailer_Item_HK and
// MAGIC old_lsv.SALES_DT = date('2020-09-01') and
// MAGIC new_lsv.SALES_DT = date('2020-09-01')

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC SELECT
// MAGIC old_lsv.HUB_Organization_Unit_HK,
// MAGIC old_lsv.HUB_Retailer_Item_HK,
// MAGIC old_lsv.sales_dt,
// MAGIC old_lsv.LOST_SALES_AMT as OLD_LOST_SALES_AMT,
// MAGIC new_lsv.LOST_SALES_AMT as NEW_LOST_SALES_AMT
// MAGIC from
// MAGIC
// MAGIC kroger_danonewave_us_retail_alert_im.LOST_SALES_VALUE old_lsv,
// MAGIC tmp_vw_kroger_danonewave_lost_sales_value_new new_lsv
// MAGIC
// MAGIC where
// MAGIC
// MAGIC old_lsv.sales_dt = new_lsv.sales_dt and
// MAGIC old_lsv.HUB_Organization_Unit_HK = new_lsv.HUB_Organization_Unit_HK and
// MAGIC old_lsv.HUB_Retailer_Item_HK = new_lsv.HUB_Retailer_Item_HK and
// MAGIC old_lsv.SALES_DT = new_lsv.SALES_DT and
// MAGIC
// MAGIC old_lsv.SALES_DT between date('2020-09-01') and date('2020-09-10')
// MAGIC
// MAGIC and
// MAGIC (
// MAGIC old_lsv.HUB_ORGANIZATION_UNIT_HK = '013065332c246c9c5c331dc2c39b1aeb'
// MAGIC or
// MAGIC old_lsv.HUB_ORGANIZATION_UNIT_HK = '03c4af842ac7bd2895e199980d9b9dc2'
// MAGIC )
// MAGIC ORDER BY
// MAGIC old_lsv.HUB_ORGANIZATION_UNIT_HK,
// MAGIC old_lsv.HUB_RETAILER_ITEM_HK,
// MAGIC old_lsv.SALES_DT

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC SELECT
// MAGIC old_lsv.sales_dt,
// MAGIC count(*)
// MAGIC from
// MAGIC kroger_danonewave_us_retail_alert_im.LOST_SALES_VALUE old_lsv inner join
// MAGIC tmp_vw_kroger_danonewave_lost_sales_value_new new_lsv
// MAGIC
// MAGIC on
// MAGIC old_lsv.sales_dt = new_lsv.sales_dt and
// MAGIC old_lsv.HUB_Organization_Unit_HK = new_lsv.HUB_Organization_Unit_HK and
// MAGIC old_lsv.HUB_Retailer_Item_HK = new_lsv.HUB_Retailer_Item_HK
// MAGIC
// MAGIC where
// MAGIC old_lsv.sales_dt between date('2020-09-01') and date('2020-09-22')
// MAGIC
// MAGIC group by 
// MAGIC old_lsv.sales_dt
// MAGIC   
// MAGIC order by
// MAGIC old_lsv.sales_dt

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC SELECT
// MAGIC old_lsv.sales_dt,
// MAGIC count(*)
// MAGIC from
// MAGIC kroger_danonewave_us_retail_alert_im.LOST_SALES_VALUE old_lsv left outer join
// MAGIC tmp_vw_kroger_danonewave_lost_sales_value_new new_lsv
// MAGIC
// MAGIC on
// MAGIC old_lsv.sales_dt = new_lsv.sales_dt and
// MAGIC old_lsv.HUB_Organization_Unit_HK = new_lsv.HUB_Organization_Unit_HK and
// MAGIC old_lsv.HUB_Retailer_Item_HK = new_lsv.HUB_Retailer_Item_HK
// MAGIC
// MAGIC where
// MAGIC old_lsv.sales_dt between date('2020-09-01') and date('2020-09-22')
// MAGIC and
// MAGIC new_lsv.sales_dt is null
// MAGIC
// MAGIC group by 
// MAGIC old_lsv.sales_dt
// MAGIC   
// MAGIC order by
// MAGIC old_lsv.sales_dt

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC SELECT
// MAGIC new_lsv.sales_dt,
// MAGIC count(*)
// MAGIC from
// MAGIC kroger_danonewave_us_retail_alert_im.LOST_SALES_VALUE old_lsv right outer join
// MAGIC tmp_vw_kroger_danonewave_lost_sales_value_new new_lsv
// MAGIC
// MAGIC on
// MAGIC old_lsv.sales_dt = new_lsv.sales_dt and
// MAGIC old_lsv.HUB_Organization_Unit_HK = new_lsv.HUB_Organization_Unit_HK and
// MAGIC old_lsv.HUB_Retailer_Item_HK = new_lsv.HUB_Retailer_Item_HK
// MAGIC
// MAGIC where
// MAGIC new_lsv.sales_dt between date('2020-09-01') and date('2020-09-22')
// MAGIC and
// MAGIC old_lsv.sales_dt is null
// MAGIC
// MAGIC group by 
// MAGIC new_lsv.sales_dt
// MAGIC   
// MAGIC order by
// MAGIC new_lsv.sales_dt

// COMMAND ----------

// MAGIC %md
// MAGIC # Get OSA Slow Sales data with the old LSV Derivation

// COMMAND ----------

// MAGIC %sql
// MAGIC select
// MAGIC HUB_ORGANIZATION_UNIT_HK,
// MAGIC HUB_RETAILER_ITEM_HK,
// MAGIC ALERT_MESSAGE_DESC,
// MAGIC LOST_SALES_AMT
// MAGIC from kroger_danonewave_us_retail_alert_im.alert_on_shelf_availability
// MAGIC where
// MAGIC sales_dt = date('2020-09-01')

// COMMAND ----------

// MAGIC %sql
// MAGIC select
// MAGIC HUB_ORGANIZATION_UNIT_HK,
// MAGIC HUB_RETAILER_ITEM_HK,
// MAGIC ALERT_MESSAGE_DESC,
// MAGIC SALES_DT,
// MAGIC LOST_SALES_AMT
// MAGIC from kroger_danonewave_us_retail_alert_im.alert_on_shelf_availability
// MAGIC where
// MAGIC SALES_DT between date('2020-09-01') and date('2020-09-10')
// MAGIC and
// MAGIC (
// MAGIC HUB_ORGANIZATION_UNIT_HK = '013065332c246c9c5c331dc2c39b1aeb'
// MAGIC or
// MAGIC HUB_ORGANIZATION_UNIT_HK = '03c4af842ac7bd2895e199980d9b9dc2'
// MAGIC )
// MAGIC ORDER BY
// MAGIC HUB_ORGANIZATION_UNIT_HK,
// MAGIC HUB_RETAILER_ITEM_HK,
// MAGIC SALES_DT

// COMMAND ----------

// SELECT s.date,sum(s.stats) as today_sum,
//        (SELECT sum(t.stats) FROM YourTable t
//         where t.date between s.date - 2 and s.date) as sum_3days
// FROM YourTable s
// GROUP BY s.date

// COMMAND ----------

// MAGIC %md
// MAGIC # Get OSA Slow Sales data with the new LSV Derivation

// COMMAND ----------

// 2020-09-26. Finally validated that this is correct

val sqlOsaSlowSales = f"""
SELECT
  llsv.HUB_ORGANIZATION_UNIT_HK
  , llsv.HUB_RETAILER_ITEM_HK
  , "Slow Sales" AS ALERT_MESSAGE_DESC
  , llsv.SALES_DT
  , sum(adsd.LOST_SALES_AMT) as LOST_SALES_AMT
  
FROM kroger_danonewave_us_dv.vw_sat_link_epos_summary sles

INNER JOIN (select * from tmp_vw_kroger_danonewave_lost_sales_value_new ) llsv -- Pulling data from the new LSV derivation TMP View
  ON sles.HUB_ORGANIZATION_UNIT_HK = llsv.HUB_ORGANIZATION_UNIT_HK
  AND sles.HUB_RETAILER_ITEM_HK = llsv.HUB_RETAILER_ITEM_HK
  AND sles.SALES_DT = llsv.SALES_DT
  
INNER JOIN (select * from tmp_vw_kroger_danonewave_lost_sales_value_new ) adsd -- Pulling data from the new LSV derivation TMP View
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
  
"""
spark.sql(sqlOsaSlowSales).createOrReplaceTempView("tmp_vw_kroger_danonewave_alert_on_shelf_availability_new")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from tmp_vw_kroger_danonewave_alert_on_shelf_availability_new
// MAGIC where
// MAGIC SALES_DT between date('2020-09-01') and date('2020-09-10')
// MAGIC and
// MAGIC (
// MAGIC HUB_ORGANIZATION_UNIT_HK = '013065332c246c9c5c331dc2c39b1aeb'
// MAGIC or
// MAGIC HUB_ORGANIZATION_UNIT_HK = '03c4af842ac7bd2895e199980d9b9dc2'
// MAGIC )

// COMMAND ----------

// MAGIC %sql
// MAGIC select
// MAGIC   sales_dt,
// MAGIC   ALERT_MESSAGE_DESC,
// MAGIC   count(*)
// MAGIC from kroger_danonewave_us_retail_alert_im.alert_on_shelf_availability
// MAGIC where
// MAGIC   SALES_DT between date('2020-06-01') and date('2020-09-05') and
// MAGIC   ALERT_MESSAGE_DESC = 'Slow Sales' and
// MAGIC   LOST_SALES_AMT >= 7
// MAGIC group by 
// MAGIC   sales_dt,
// MAGIC   ALERT_MESSAGE_DESC
// MAGIC order by
// MAGIC   sales_dt,
// MAGIC   ALERT_MESSAGE_DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC select
// MAGIC   sales_dt,
// MAGIC   ALERT_MESSAGE_DESC,
// MAGIC   count(*)
// MAGIC from tmp_vw_kroger_danonewave_alert_on_shelf_availability_new
// MAGIC where
// MAGIC   SALES_DT between date('2020-06-01') and date('2020-09-05') and
// MAGIC   ALERT_MESSAGE_DESC = 'Slow Sales' and
// MAGIC   LOST_SALES_AMT >= 7
// MAGIC group by 
// MAGIC   sales_dt,
// MAGIC   ALERT_MESSAGE_DESC
// MAGIC order by
// MAGIC   sales_dt,
// MAGIC   ALERT_MESSAGE_DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC select
// MAGIC   ALERT_MESSAGE_DESC,
// MAGIC   count(*)
// MAGIC from kroger_danonewave_us_retail_alert_im.alert_on_shelf_availability
// MAGIC where
// MAGIC   SALES_DT between date('2020-06-01') and date('2020-09-05') and
// MAGIC   ALERT_MESSAGE_DESC = 'Slow Sales' and
// MAGIC   LOST_SALES_AMT >= 7
// MAGIC group by 
// MAGIC   ALERT_MESSAGE_DESC
// MAGIC order by
// MAGIC   ALERT_MESSAGE_DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC select
// MAGIC   ALERT_MESSAGE_DESC,
// MAGIC   count(*)
// MAGIC from tmp_vw_kroger_danonewave_alert_on_shelf_availability_new
// MAGIC where
// MAGIC   SALES_DT between date('2020-06-01') and date('2020-09-05') and
// MAGIC   ALERT_MESSAGE_DESC = 'Slow Sales' and
// MAGIC   LOST_SALES_AMT >= 7
// MAGIC group by 
// MAGIC   ALERT_MESSAGE_DESC
// MAGIC order by
// MAGIC   ALERT_MESSAGE_DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT
// MAGIC   old_osa.ALERT_MESSAGE_DESC,
// MAGIC   count(*)
// MAGIC from
// MAGIC
// MAGIC kroger_danonewave_us_retail_alert_im.alert_on_shelf_availability old_osa
// MAGIC inner join 
// MAGIC tmp_vw_kroger_danonewave_alert_on_shelf_availability_new new_osa
// MAGIC
// MAGIC on
// MAGIC
// MAGIC old_osa.sales_dt = new_osa.sales_dt and
// MAGIC old_osa.HUB_Organization_Unit_HK = new_osa.HUB_Organization_Unit_HK and
// MAGIC old_osa.HUB_Retailer_Item_HK = new_osa.HUB_Retailer_Item_HK
// MAGIC
// MAGIC where
// MAGIC   old_osa.SALES_DT between date('2020-06-01') and date('2020-09-05') and
// MAGIC   old_osa.ALERT_MESSAGE_DESC = 'Slow Sales' and
// MAGIC   old_osa.LOST_SALES_AMT >= 7
// MAGIC
// MAGIC group by 
// MAGIC   old_osa.ALERT_MESSAGE_DESC
// MAGIC order by
// MAGIC   old_osa.ALERT_MESSAGE_DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT
// MAGIC   old_osa.ALERT_MESSAGE_DESC,
// MAGIC   count(*)
// MAGIC from
// MAGIC
// MAGIC kroger_danonewave_us_retail_alert_im.alert_on_shelf_availability old_osa
// MAGIC left outer join 
// MAGIC tmp_vw_kroger_danonewave_alert_on_shelf_availability_new new_osa
// MAGIC
// MAGIC on
// MAGIC
// MAGIC old_osa.sales_dt = new_osa.sales_dt and
// MAGIC old_osa.HUB_Organization_Unit_HK = new_osa.HUB_Organization_Unit_HK and
// MAGIC old_osa.HUB_Retailer_Item_HK = new_osa.HUB_Retailer_Item_HK
// MAGIC
// MAGIC where
// MAGIC   old_osa.SALES_DT between date('2020-06-01') and date('2020-09-05') and
// MAGIC   old_osa.ALERT_MESSAGE_DESC = 'Slow Sales' and
// MAGIC   old_osa.LOST_SALES_AMT >= 7 and
// MAGIC   
// MAGIC   new_osa.SALES_DT is null
// MAGIC
// MAGIC group by 
// MAGIC   old_osa.ALERT_MESSAGE_DESC
// MAGIC order by
// MAGIC   old_osa.ALERT_MESSAGE_DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT
// MAGIC   new_osa.ALERT_MESSAGE_DESC,
// MAGIC   count(*)
// MAGIC from
// MAGIC
// MAGIC tmp_vw_kroger_danonewave_alert_on_shelf_availability_new new_osa
// MAGIC left outer join
// MAGIC kroger_danonewave_us_retail_alert_im.alert_on_shelf_availability old_osa
// MAGIC  
// MAGIC on
// MAGIC
// MAGIC new_osa.sales_dt = old_osa.sales_dt and
// MAGIC new_osa.HUB_Organization_Unit_HK = old_osa.HUB_Organization_Unit_HK and
// MAGIC new_osa.HUB_Retailer_Item_HK = old_osa.HUB_Retailer_Item_HK
// MAGIC
// MAGIC where
// MAGIC
// MAGIC   new_osa.sales_dt between date('2020-09-01') and date('2020-09-22') and
// MAGIC   new_osa.ALERT_MESSAGE_DESC = 'Slow Sales' and
// MAGIC   new_osa.LOST_SALES_AMT >= 7 and
// MAGIC   
// MAGIC   old_osa.SALES_DT is null
// MAGIC
// MAGIC group by 
// MAGIC   new_osa.ALERT_MESSAGE_DESC
// MAGIC order by
// MAGIC   new_osa.ALERT_MESSAGE_DESC
// MAGIC   
// MAGIC -- Pull the records which are there in OLD but not in the NEW
// MAGIC -- and new_osa.sales_dt is null
// MAGIC
// MAGIC -- Pull the records which are there in OLD AND in the NEW
// MAGIC --and new_osa.sales_dt is not null

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT
// MAGIC   old_osa.sales_dt,
// MAGIC   old_osa.ALERT_MESSAGE_DESC,
// MAGIC   count(*)
// MAGIC from
// MAGIC
// MAGIC kroger_danonewave_us_retail_alert_im.alert_on_shelf_availability old_osa
// MAGIC inner join 
// MAGIC tmp_vw_kroger_danonewave_alert_on_shelf_availability_new new_osa
// MAGIC
// MAGIC on
// MAGIC
// MAGIC old_osa.sales_dt = new_osa.sales_dt and
// MAGIC old_osa.HUB_Organization_Unit_HK = new_osa.HUB_Organization_Unit_HK and
// MAGIC old_osa.HUB_Retailer_Item_HK = new_osa.HUB_Retailer_Item_HK
// MAGIC
// MAGIC where
// MAGIC
// MAGIC old_osa.sales_dt between date('2020-09-01') and date('2020-09-22') and
// MAGIC old_osa.ALERT_MESSAGE_DESC = 'Slow Sales'
// MAGIC
// MAGIC group by 
// MAGIC   old_osa.sales_dt,
// MAGIC   old_osa.ALERT_MESSAGE_DESC
// MAGIC order by
// MAGIC   old_osa.sales_dt,
// MAGIC   old_osa.ALERT_MESSAGE_DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT
// MAGIC   old_osa.sales_dt,
// MAGIC   old_osa.ALERT_MESSAGE_DESC,
// MAGIC   count(*)
// MAGIC from
// MAGIC
// MAGIC kroger_danonewave_us_retail_alert_im.alert_on_shelf_availability old_osa
// MAGIC left outer join 
// MAGIC tmp_vw_kroger_danonewave_alert_on_shelf_availability_new new_osa
// MAGIC
// MAGIC on
// MAGIC
// MAGIC old_osa.sales_dt = new_osa.sales_dt and
// MAGIC old_osa.HUB_Organization_Unit_HK = new_osa.HUB_Organization_Unit_HK and
// MAGIC old_osa.HUB_Retailer_Item_HK = new_osa.HUB_Retailer_Item_HK
// MAGIC
// MAGIC where
// MAGIC
// MAGIC old_osa.sales_dt between date('2020-09-01') and date('2020-09-22') and
// MAGIC old_osa.ALERT_MESSAGE_DESC = 'Slow Sales' and -- Pulling only Slow Sales Alerts from the OLD OSA table
// MAGIC
// MAGIC new_osa.SALES_DT is null
// MAGIC
// MAGIC group by 
// MAGIC   old_osa.sales_dt,
// MAGIC   old_osa.ALERT_MESSAGE_DESC
// MAGIC order by
// MAGIC   old_osa.sales_dt,
// MAGIC   old_osa.ALERT_MESSAGE_DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT
// MAGIC   new_osa.sales_dt,
// MAGIC   new_osa.ALERT_MESSAGE_DESC,
// MAGIC   count(*)
// MAGIC from
// MAGIC
// MAGIC tmp_vw_kroger_danonewave_alert_on_shelf_availability_new new_osa
// MAGIC left outer join
// MAGIC kroger_danonewave_us_retail_alert_im.alert_on_shelf_availability old_osa
// MAGIC  
// MAGIC on
// MAGIC
// MAGIC new_osa.sales_dt = old_osa.sales_dt and
// MAGIC new_osa.HUB_Organization_Unit_HK = old_osa.HUB_Organization_Unit_HK and
// MAGIC new_osa.HUB_Retailer_Item_HK = old_osa.HUB_Retailer_Item_HK
// MAGIC
// MAGIC where
// MAGIC
// MAGIC new_osa.sales_dt between date('2020-09-01') and date('2020-09-22') and
// MAGIC new_osa.ALERT_MESSAGE_DESC = 'Slow Sales' and -- Pulling only Slow Sales Alerts from the OLD OSA table
// MAGIC
// MAGIC old_osa.SALES_DT is null
// MAGIC
// MAGIC group by 
// MAGIC   new_osa.sales_dt,
// MAGIC   new_osa.ALERT_MESSAGE_DESC
// MAGIC order by
// MAGIC   new_osa.sales_dt,
// MAGIC   new_osa.ALERT_MESSAGE_DESC
// MAGIC   
// MAGIC -- Pull the records which are there in OLD but not in the NEW
// MAGIC -- and new_osa.sales_dt is null
// MAGIC
// MAGIC -- Pull the records which are there in OLD AND in the NEW
// MAGIC --and new_osa.sales_dt is not null

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT
// MAGIC   old_osa.SALES_DT,
// MAGIC   old_osa.HUB_ORGANIZATION_UNIT_HK,
// MAGIC   old_osa.HUB_RETAILER_ITEM_HK,
// MAGIC   old_osa.ALERT_MESSAGE_DESC,
// MAGIC   -- old_osa.ALERT_TYPE_NM,
// MAGIC   -- old_osa.ON_HAND_INVENTORY_QTY as OLD_ON_HAND_INVENTORY_QTY,
// MAGIC   old_osa.LOST_SALES_AMT as OLD_LOST_SALES_AMT,
// MAGIC   -- new_osa.ON_HAND_INVENTORY_QTY as NEW_ON_HAND_INVENTORY_QTY,
// MAGIC   new_osa.LOST_SALES_AMT as NEW_LOST_SALES_AMT
// MAGIC from
// MAGIC
// MAGIC kroger_danonewave_us_retail_alert_im.alert_on_shelf_availability old_osa
// MAGIC left outer join 
// MAGIC tmp_vw_kroger_danonewave_alert_on_shelf_availability_new new_osa
// MAGIC
// MAGIC on
// MAGIC
// MAGIC old_osa.sales_dt = new_osa.sales_dt and
// MAGIC old_osa.HUB_Organization_Unit_HK = new_osa.HUB_Organization_Unit_HK and
// MAGIC old_osa.HUB_Retailer_Item_HK = new_osa.HUB_Retailer_Item_HK 
// MAGIC
// MAGIC where
// MAGIC old_osa.ALERT_MESSAGE_DESC = 'Slow Sales' and -- Pulling only Slow Sales Alerts from the OLD OSA table
// MAGIC old_osa.sales_dt between date('2020-09-01') and date('2020-09-22') and
// MAGIC
// MAGIC new_osa.SALES_DT is null
// MAGIC
// MAGIC order by
// MAGIC   old_osa.sales_dt,
// MAGIC   old_osa.ALERT_MESSAGE_DESC
// MAGIC   
// MAGIC -- Pull the records which are there in OLD but not in the NEW
// MAGIC -- and new_osa.sales_dt is null
// MAGIC
// MAGIC -- Pull the records which are there in OLD AND in the NEW
// MAGIC --and new_osa.sales_dt is not null

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT
// MAGIC   new_osa.SALES_DT,
// MAGIC   new_osa.HUB_ORGANIZATION_UNIT_HK,
// MAGIC   new_osa.HUB_RETAILER_ITEM_HK,
// MAGIC   new_osa.ALERT_MESSAGE_DESC,
// MAGIC   -- old_osa.ALERT_TYPE_NM,
// MAGIC   -- old_osa.ON_HAND_INVENTORY_QTY as OLD_ON_HAND_INVENTORY_QTY,
// MAGIC   old_osa.LOST_SALES_AMT as OLD_LOST_SALES_AMT,
// MAGIC   -- new_osa.ON_HAND_INVENTORY_QTY as NEW_ON_HAND_INVENTORY_QTY,
// MAGIC   new_osa.LOST_SALES_AMT as NEW_LOST_SALES_AMT
// MAGIC from
// MAGIC
// MAGIC kroger_danonewave_us_retail_alert_im.alert_on_shelf_availability old_osa
// MAGIC right outer join 
// MAGIC tmp_vw_kroger_danonewave_alert_on_shelf_availability_new new_osa
// MAGIC
// MAGIC on
// MAGIC
// MAGIC old_osa.sales_dt = new_osa.sales_dt and
// MAGIC old_osa.HUB_Organization_Unit_HK = new_osa.HUB_Organization_Unit_HK and
// MAGIC old_osa.HUB_Retailer_Item_HK = new_osa.HUB_Retailer_Item_HK 
// MAGIC
// MAGIC where
// MAGIC new_osa.ALERT_MESSAGE_DESC = 'Slow Sales' and -- Pulling only Slow Sales Alerts from the OLD OSA table
// MAGIC new_osa.sales_dt between date('2020-09-01') and date('2020-09-22') and
// MAGIC
// MAGIC old_osa.SALES_DT is null
// MAGIC
// MAGIC order by
// MAGIC   new_osa.sales_dt,
// MAGIC   new_osa.ALERT_MESSAGE_DESC
// MAGIC   
// MAGIC -- Pull the records which are there in OLD but not in the NEW
// MAGIC -- and new_osa.sales_dt is null
// MAGIC
// MAGIC -- Pull the records which are there in OLD AND in the NEW
// MAGIC --and new_osa.sales_dt is not null

// COMMAND ----------

// MAGIC %sql
// MAGIC select
// MAGIC   ALERT_MESSAGE_DESC,
// MAGIC   count(*)
// MAGIC from kroger_danonewave_us_retail_alert_im.alert_on_shelf_availability
// MAGIC where
// MAGIC   SALES_DT between date('2020-06-01') and date('2020-09-05') and
// MAGIC   ALERT_MESSAGE_DESC <> 'Slow Sales'
// MAGIC group by 
// MAGIC   ALERT_MESSAGE_DESC
// MAGIC order by
// MAGIC   ALERT_MESSAGE_DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(distinct sales_dt)
// MAGIC from kroger_danonewave_us_retail_alert_im.alert_on_shelf_availability

// COMMAND ----------

// MAGIC %sql
// MAGIC select
// MAGIC   sales_dt,
// MAGIC   ALERT_MESSAGE_DESC,
// MAGIC   count(*)
// MAGIC from danonewave_drt_us_team_alerts_im.vw_retail_alerts_on_shelf_availability
// MAGIC where
// MAGIC   SALES_DT between date('2019-01-01') and date('2020-09-15') 
// MAGIC --   and ALERT_MESSAGE_DESC <> 'Slow Sales'
// MAGIC group by 
// MAGIC   sales_dt,
// MAGIC   ALERT_MESSAGE_DESC
// MAGIC order by
// MAGIC   sales_dt,
// MAGIC   ALERT_MESSAGE_DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC select
// MAGIC   ALERT_MESSAGE_DESC,
// MAGIC   count(*)
// MAGIC from kroger_danonewave_us_retail_alert_im.alert_on_shelf_availability
// MAGIC where
// MAGIC   SALES_DT between date('2020-06-01') and date('2020-09-05') and
// MAGIC   ALERT_MESSAGE_DESC = 'Slow Sales' and
// MAGIC   LOST_SALES_AMT >= 7
// MAGIC group by 
// MAGIC   ALERT_MESSAGE_DESC
// MAGIC order by
// MAGIC   ALERT_MESSAGE_DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC select
// MAGIC   ALERT_MESSAGE_DESC,
// MAGIC   count(*)
// MAGIC from danonewave_drt_us_team_alerts_im.vw_retail_alerts_on_shelf_availability
// MAGIC where
// MAGIC   SALES_DT between date('2020-06-01') and date('2020-09-05') and
// MAGIC   ALERT_MESSAGE_DESC = 'Slow Sales' and
// MAGIC   LOST_SALES_AMT >= 7
// MAGIC group by 
// MAGIC   ALERT_MESSAGE_DESC
// MAGIC order by
// MAGIC   ALERT_MESSAGE_DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Starbucks
// MAGIC select
// MAGIC   sales_dt,
// MAGIC   ALERT_MESSAGE_DESC,
// MAGIC   count(*)
// MAGIC from kroger_starbucks_us_retail_alert_im.alert_on_shelf_availability
// MAGIC where
// MAGIC   SALES_DT between date('2019-01-01') and date('2020-09-15') 
// MAGIC --   and ALERT_MESSAGE_DESC <> 'Slow Sales'
// MAGIC group by 
// MAGIC   sales_dt,
// MAGIC   ALERT_MESSAGE_DESC
// MAGIC order by
// MAGIC   sales_dt,
// MAGIC   ALERT_MESSAGE_DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Starbucks
// MAGIC select
// MAGIC   sales_dt,
// MAGIC   ALERT_MESSAGE_DESC,
// MAGIC   count(*)
// MAGIC from walmart_nestlewaters_us_retail_alert_im.alert_on_shelf_availability
// MAGIC where
// MAGIC   SALES_DT between date('2019-01-01') and date('2020-09-15') 
// MAGIC   and ALERT_MESSAGE_DESC <> 'Slow Sales'
// MAGIC group by 
// MAGIC   sales_dt,
// MAGIC   ALERT_MESSAGE_DESC
// MAGIC order by
// MAGIC   sales_dt,
// MAGIC   ALERT_MESSAGE_DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC select min(sales_dt), max(sales_dt)
// MAGIC from danonewave_drt_us_team_alerts_im.alert_on_shelf_availability

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from kroger_danonewave_us_dv.SAT_LINK_BOH 

// COMMAND ----------

// MAGIC %sql
// MAGIC select
// MAGIC inventory_dt, count(*)
// MAGIC from kroger_danonewave_us_dv.SAT_LINK_BOH 
// MAGIC where
// MAGIC inventory_dt between date('2020-08-01') and date('2020-09-15')
// MAGIC and on_hand_qty > 0
// MAGIC group by inventory_dt
