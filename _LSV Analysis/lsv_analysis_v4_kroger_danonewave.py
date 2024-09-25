# Databricks notebook source
from pyspark.sql import types as pyt
from pyspark.sql import functions as pyf

# COMMAND ----------

retailer_client = "kroger_danonewave_us"

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

sqlOsaAlerts1 = """
select
  *
from danonewave_drt_us_team_alerts_im.vw_retail_alerts_on_shelf_availability
where
  SALES_DT between date('2020-06-01') and date('2020-09-05') and
  ALERT_MESSAGE_DESC = 'Slow Sales' and
  LOST_SALES_AMT >= 7
"""

osa_alerts_using_old_lsv = spark.sql(sqlOsaAlerts1)

# COMMAND ----------

osa_alerts_using_old_lsv = osa_alerts_using_old_lsv.drop('RETAIL_CLIENT', 'ParentChainId', 'RECORD_SOURCE_CD', 'ALERT_MESSAGE_DESC', 'ALERT_TYPE_NM')

# COMMAND ----------

display(osa_alerts_using_old_lsv)

# COMMAND ----------

sqlOsaAlerts2 = """
select
  *
from tmp_vw_alert_on_shelf_availability_new
where
  SALES_DT between date('2020-06-01') and date('2020-09-05') and
  ALERT_MESSAGE_DESC = 'Slow Sales' and
  LOST_SALES_AMT >= 7
"""

osa_alerts_using_new_lsv = spark.sql(sqlOsaAlerts2)

# COMMAND ----------

display(osa_alerts_using_new_lsv)

# COMMAND ----------

osa_alerts_using_new_lsv = osa_alerts_using_new_lsv.drop('ALERT_MESSAGE_DESC')

# COMMAND ----------

combined_result = osa_alerts_using_old_lsv.join(osa_alerts_using_new_lsv.withColumnRenamed('LOST_SALES_AMT', 'LOST_SALES_AMT_NEW'), on=['HUB_ORGANIZATION_UNIT_HK','HUB_RETAILER_ITEM_HK', 'SALES_DT'], how='left')

combined_result.count()

# COMMAND ----------

combined_result.printSchema()

# COMMAND ----------

sqlHubOrganizationUnit = """
select
HUB_ORGANIZATION_UNIT_HK,
DIVISION_NBR,
STORE_NBR
from kroger_danonewave_us_dv.hub_organization_unit
"""

hub_organization_unit = spark.sql(sqlHubOrganizationUnit)

# COMMAND ----------

combined_result2 = combined_result.join(hub_organization_unit, on=['HUB_Organization_Unit_HK'], how='inner')

# COMMAND ----------

combined_result2.printSchema()

# COMMAND ----------

combined_result2.write.format('delta').save("/mnt/processed/hugh/lsv-analysis/kroger_danone_us", header='true')

# COMMAND ----------

sqlSatOrganizationUnit = """
SELECT DISTINCT 
sou.Mgt_Div_Nbr,
sou.store_banner,
sou.division_desc
from 
kroger_danonewave_us_dv.sat_organization_unit sou
"""

sat_organization_unit = spark.sql(sqlSatOrganizationUnit)

# COMMAND ----------

display(sat_organization_unit)

# COMMAND ----------

sqlHubAndSatOrganizationUnit = """
SELECT DISTINCT 
hou.organization_unit_num,
hou.division_nbr,
hou.store_nbr,
sou.store_banner,
sou.division_desc
from 
kroger_danonewave_us_dv.hub_organization_unit hou 
inner join
kroger_danonewave_us_dv.sat_organization_unit sou on hou.HUB_Organization_Unit_HK = sou.HUB_Organization_Unit_HK
where store_nbr = 354
"""

hub_and_sat_organization_unit = spark.sql(sqlHubAndSatOrganizationUnit)

# COMMAND ----------

display(hub_and_sat_organization_unit)

# COMMAND ----------

# results.coalesce(1).write.format('csv').save("/mnt/processed/hugh/lsv-analysis/{0}".format(retailer_client), header='true')

# COMMAND ----------

min_and_max_alert_dates = """
select min(sales_dt), max(sales_dt)
from {}_retail_alert_im.alert_on_shelf_availability

""".format(retailer_client)

# COMMAND ----------

display(spark.sql(min_and_max_alert_dates))

# COMMAND ----------

combined_result2 = spark.read.format('delta')\
    .options(header='true', inferSchema='true')\
    .load('/mnt/processed/hugh/lsv-analysis/kroger_danone_us')

combined_result2.count()

# COMMAND ----------

combined_result2.printSchema()

# COMMAND ----------

display(combined_result2)

# COMMAND ----------

kroger_danonewave_us_division_nbr = spark.read.format('csv')\
    .options(header='true', inferSchema='false')\
    .load('/mnt/processed/hugh/lsv-analysis/kroger_danonewave_us/kroger_danonewave_us_division_nbr.csv')

kroger_danonewave_us_division_nbr.count()

# COMMAND ----------

kroger_danonewave_us_division_nbr = kroger_danonewave_us_division_nbr.withColumn('DIVISION_NBR', pyf.lpad(kroger_danonewave_us_division_nbr['DIVISION_NBR'], 3, '0'))

# COMMAND ----------

kroger_danonewave_us_division_nbr.printSchema()

# COMMAND ----------

display(kroger_danonewave_us_division_nbr)

# COMMAND ----------

kroger_danonewave_us_20200907 = spark.read.format('csv')\
    .options(header='true', inferSchema='false')\
    .load('/mnt/processed/hugh/lsv-analysis/kroger_danonewave_us/kroger_danonewave_us_20200907.csv')

kroger_danonewave_us_20200907.count()

# COMMAND ----------

kroger_danonewave_us_20200907 = kroger_danonewave_us_20200907.withColumn('LSV', kroger_danonewave_us_20200907['LSV'].cast(pyt.DecimalType(6,2)))
kroger_danonewave_us_20200907 = kroger_danonewave_us_20200907.withColumn('Call Completed Date', kroger_danonewave_us_20200907['Call Completed Date'].cast(pyt.DateType()))

# COMMAND ----------

kroger_danonewave_us_20200907 = kroger_danonewave_us_20200907.withColumn('SALES_DT', pyf.date_add(pyf.date_format('Call Completed Date', 'yyyy-MM-dd'), -2))

# COMMAND ----------

# kroger_danonewave_us_20200907 = kroger_danonewave_us_20200907.withColumnRenamed('LSV', 'LOST_SALES_AMT')

# COMMAND ----------

kroger_danonewave_us_slow_sales_alerts = kroger_danonewave_us_20200907.join(kroger_danonewave_us_division_nbr, on=['Subbanner Description'], how='inner')

# COMMAND ----------

kroger_danonewave_us_slow_sales_alerts.count()

# COMMAND ----------

kroger_danonewave_us_slow_sales_alerts.printSchema()

# COMMAND ----------

display(kroger_danonewave_us_slow_sales_alerts)

# COMMAND ----------

analysis_df = combined_result2.join(kroger_danonewave_us_slow_sales_alerts,
                                    on=['RETAILER_ITEM_ID', 'STORE_NBR', 'DIVISION_NBR', 'SALES_DT'], 
                                    how='inner')

# COMMAND ----------

analysis_df.count()

# COMMAND ----------

analysis_df.printSchema()

# COMMAND ----------

display(analysis_df)

# COMMAND ----------

# temp_df1 = kroger_danonewave_us_20200907.where((pyf.col('Subbanner Description') == 'Kroger Ralphs So Cal') & 
#                                               (pyf.col('RETAILER_ITEM_ID') == '0074236526497') & 
#                                               (pyf.col('STORE_NBR') == '00006'))

# COMMAND ----------

temp_df1 = kroger_danonewave_us_slow_sales_alerts.where((pyf.col('Subbanner Description') == 'Kroger Cincinnati') & 
                                                        (pyf.col('RETAILER_ITEM_ID') == '0004667501351') & 
                                                        (pyf.col('STORE_NBR') == '00923'))

# COMMAND ----------

display(temp_df1)

# COMMAND ----------

# temp_df2 = combined_result2.where((pyf.col('DIVISION_NBR') == '703') & 
#                                   (pyf.col('RETAILER_ITEM_ID') == '0074236526497') & 
#                                   (pyf.col('STORE_NBR') == '00006'))

# COMMAND ----------

temp_df2 = combined_result2.where((pyf.col('DIVISION_NBR') == '703') & 
                                  (pyf.col('RETAILER_ITEM_ID') == '0002529300451') & 
                                  (pyf.col('STORE_NBR') == '00006'))

# COMMAND ----------

temp_df2 = combined_result2.where((pyf.col('DIVISION_NBR') == '014') & 
                                  (pyf.col('RETAILER_ITEM_ID') == '0004667501351') & 
                                  (pyf.col('STORE_NBR') == '00923'))

# COMMAND ----------

display(temp_df2)

# COMMAND ----------


