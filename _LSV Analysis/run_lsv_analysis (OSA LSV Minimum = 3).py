# Databricks notebook source
# Python3 code to iterate over a list 
list = [
  'asda_generalmills_uk',
  'morrisons_generalmills_uk',
  'sainsburys_generalmills_uk',
  'tesco_generalmills_uk',
  'asda_nestlecereals_uk',
  'morrisons_nestlecereals_uk',
  'sainsburys_nestlecereals_uk',
  'tesco_nestlecereals_uk'
] 

# COMMAND ----------

# Using for loop 
for i in list: 
    retailer_client = i
    
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
    
    spark.sql(viewDdl).createOrReplaceTempView("tmp_vw_lost_sales_value_new")
    
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
    
    spark.sql(sqlOsaSlowSales).createOrReplaceTempView("tmp_vw_alert_on_shelf_availability_new")
    
    sqlCountOsaAlerts = """
    select

    (select
      count(*)
    from {0}_retail_alert_im.alert_on_shelf_availability
    where
      SALES_DT between date('2020-06-01') and date('2020-09-05') and
      ALERT_MESSAGE_DESC = 'Slow Sales' and
      LOST_SALES_AMT >= 3) num_slow_sales_alerts_using_old_lsv,

    (select
      count(*)
    from {1}_retail_alert_im.alert_on_shelf_availability
    where
      SALES_DT between date('2020-06-01') and date('2020-09-05') and
      ALERT_MESSAGE_DESC <> 'Slow Sales' and
      LOST_SALES_AMT >= 3) num_non_slow_sales_alerts_using_old_lsv,

    (select
      count(*)
    from tmp_vw_alert_on_shelf_availability_new
    where
      SALES_DT between date('2020-06-01') and date('2020-09-05') and
      ALERT_MESSAGE_DESC = 'Slow Sales' and
      LOST_SALES_AMT >= 3) num_osa_alerts_using_new_lsv

    """.format(retailer_client, retailer_client)

    results = spark.sql(sqlCountOsaAlerts)
    
    results.coalesce(1).write.format('csv').save("/mnt/processed/hugh/lsv-analysis/{0}".format(retailer_client), header='true', overwrite='true')

# COMMAND ----------

# import org.apache.spark.sql.DataFrame
# def getOnShelfAvaibilityAlerts(_companyID: String): DataFrame = { 

#   val OnShelfAvaibilityAlerts = 
#   spark.sql(
#   s"""
#   SELECT
#   z.ParentChainId
#   , z.Retail_Client
#   , z.HUB_ORGANIZATION_UNIT_HK
#   , z.ORGANIZATION_UNIT_NUM
#   , z.OutletId
#   , z.HUB_RETAILER_ITEM_HK
#   , z.RETAILER_ITEM_ID
#   , current_timestamp() AS LOAD_TS
#   , z.RECORD_SOURCE_CD
#   , z.ALERT_MESSAGE_DESC
#   , z.ALERT_TYPE_NM
#   , z.ON_HAND_INVENTORY_QTY
#   , z.LOST_SALES_AMT
#   , z.CompanyId
#   , z.ProductId
#   , z.ManufacturerId
#   , z.Lkp_productGroupId
#   , z.ProductBrandId
#   , z.SALES_DT
# FROM (SELECT
#         osa.ParentChainId
#         , osa.Retail_Client
#         , osa.HUB_ORGANIZATION_UNIT_HK
#         , osa.ORGANIZATION_UNIT_NUM
#         , o.OutletId
#         , osa.HUB_RETAILER_ITEM_HK
#         , osa.RETAILER_ITEM_ID
#         , osa.RECORD_SOURCE_CD
#         , osa.ALERT_MESSAGE_DESC
#         , osa.ALERT_TYPE_NM
#         , osa.ON_HAND_INVENTORY_QTY
#         , osa.LOST_SALES_AMT
#         , osa.SALES_DT
#         , p.CompanyId
#         , p.ProductId
#         , p.ManufacturerId
#         , p.Lkp_productGroupId
#         , p.ProductBrandId
#         , ROW_NUMBER() OVER(PARTITION BY osa.Sales_DT, osa.Retail_Client, osa.ParentChainId, osa.ORGANIZATION_UNIT_NUM ORDER BY osa.LOST_SALES_AMT DESC) rnum
#       FROM ${Team_Name}_${Ctry_Cd}_team_alerts_im.vw_retail_alerts_on_shelf_availability osa
      
#       INNER JOIN ${tempHoldTableName} tmp
#         ON osa.Sales_DT = tmp.Sales_DT
#         AND osa.Retail_Client = tmp.Retail_Client
#         AND osa.RECORD_SOURCE_CD = tmp.RECORD_SOURCE_CD
      
#       INNER JOIN BOBv2.vw_BOBv2_Outlet o
#         ON osa.ParentChainId = o.ParentChainId
#         AND osa.ORGANIZATION_UNIT_NUM = o.ChainRefExternal
      
#       INNER JOIN BOBv2.vw_bobv2_Product p
#         ON p.CompanyId = ${CompanyId}
#         AND osa.ParentChainId = p.ParentChainId
#         AND RIGHT("00000000000000000000" + LTRIM(RTRIM(osa.RETAILER_ITEM_ID)), 20) = RIGHT("00000000000000000000" + LTRIM(RTRIM(p.RefExternal)), 20)
      
#       INNER JOIN (SELECT DISTINCT OutletId
#                   FROM BOBv2.vw_BOBv2_DailyCallfileVisit
#                   WHERE
#                     CompanyId = ${CompanyId}) cfv
#         ON o.OutletId = cfv.OutletId
      
#       INNER JOIN BOBv2.vw_bobv2_caps c
#         ON CapType = "OSA LSV Minimum"
#         AND p.CompanyId = c.CompanyId
#         AND (((lower(RIGHT("${Team_Name}", 4)) = "_syn") AND (p.ManufacturerId = c.ManufacturerId))
#             OR
#             ((lower(RIGHT("${Team_Name}", 4)) = "_drt")))
#         AND p.ManufacturerId = CASE WHEN c.ManufacturerId IS NULL THEN p.ManufacturerId ELSE c.ManufacturerId END
#         AND p.Lkp_productGroupId = CASE WHEN c.Lkp_productGroupId IS NULL THEN p.Lkp_productGroupId ELSE c.Lkp_productGroupId END
#         AND p.ProductBrandId = CASE WHEN c.ProductBrandId IS NULL THEN p.ProductBrandId ELSE c.ProductBrandId END
      
#       WHERE
#         nvl(osa.LOST_SALES_AMT, 0) >= c.CapValue) z

# INNER JOIN BOBv2.vw_bobv2_caps c
#   ON CapType = "OSARows"
#   AND z.CompanyId = c.CompanyId
#   AND (((lower(RIGHT("${Team_Name}", 4)) = "_syn") AND (z.ManufacturerId = c.ManufacturerId))
#       OR
#       ((lower(RIGHT("${Team_Name}", 4)) = "_drt")))
#   AND z.ManufacturerId = CASE WHEN c.ManufacturerId IS NULL THEN z.ManufacturerId ELSE c.ManufacturerId END
#   AND z.Lkp_productGroupId = CASE WHEN c.Lkp_productGroupId IS NULL THEN z.Lkp_productGroupId ELSE c.Lkp_productGroupId END
#   AND z.ProductBrandId = CASE WHEN c.ProductBrandId IS NULL THEN z.ProductBrandId ELSE c.ProductBrandId END
# WHERE
#   rnum <= c.CapValue
#  """
#   )
#   OnShelfAvaibilityAlerts
# }
