// Databricks notebook source
dbutils.widgets.text("CompanyId", "", "CompanyId")
dbutils.widgets.text("Team_Name", "", "Team_Name")
dbutils.widgets.text("recordSourceCode", "", "recordSourceCode")
dbutils.widgets.text("Ctry_Cd", "", "Ctry_Cd")
dbutils.widgets.text("customer_nm", "", "customer_nm")

// COMMAND ----------

val CompanyId = dbutils.widgets.get("CompanyId")
val Team_Name = dbutils.widgets.get("Team_Name")
val recordSourceCode = dbutils.widgets.get("recordSourceCode")
val Ctry_Cd = dbutils.widgets.get("Ctry_Cd")
val customer_nm = dbutils.widgets.get("customer_nm").toString()
val tempHoldTableName = if(customer_nm.size > 0 ) s"${Team_Name}_${Ctry_Cd}_team_alerts_im.${customer_nm}_tmp_hold_audit_team_alerts_osa_sales_dt" else s"${Team_Name}_${Ctry_Cd}_team_alerts_im.tmp_hold_audit_team_alerts_osa_sales_dt"

// COMMAND ----------

def create_tmp_hold_audit_team_alerts_osa_sales_dt(Team_Name: String, Ctry_Cd: String): Unit = {  
val refreshView = s"REFRESH table ${Team_Name}_${Ctry_Cd}_team_alerts_im.vw_retailer_alert_on_shelf_availability_sales_dt"
  val refreshTempHoldTable = s"REFRESH TABLE ${tempHoldTableName}"
  val dropTempHoldTable = s"DROP TABLE IF EXISTS ${tempHoldTableName}"
val tempHoldTable = spark.sql(s"""
  SELECT a.Retail_Client, a.Sales_DT, a.RECORD_SOURCE_CD
FROM ${Team_Name}_${Ctry_Cd}_team_alerts_im.vw_retailer_alert_on_shelf_availability_sales_dt a
LEFT JOIN (SELECT Retail_Client, MAX(Sales_DT) AS Sales_DT
            FROM ${Team_Name}_${Ctry_Cd}_team_alerts_im.alert_on_shelf_availability
            GROUP BY Retail_Client) b
  ON a.Retail_Client = b.Retail_Client
WHERE
  a.RECORD_SOURCE_CD = '${recordSourceCode}'
  AND a.Sales_DT > nvl(b.Sales_DT, "2019-01-01")
  """)
  
  val filterTempHoldTable = if(customer_nm.size > 0) tempHoldTable.filter($"Retail_Client".contains(customer_nm)) else tempHoldTable

  spark.sql(refreshView)
  spark.sql(dropTempHoldTable)
  filterTempHoldTable.write.mode("overwrite").saveAsTable(tempHoldTableName)
  spark.sql(refreshTempHoldTable)
}

// COMMAND ----------

import org.apache.spark.sql.DataFrame
def getOnShelfAvaibilityAlerts(_companyID: String): DataFrame = { 

  val OnShelfAvaibilityAlerts = 
  spark.sql(
  s"""
  SELECT
  z.ParentChainId
  , z.Retail_Client
  , z.HUB_ORGANIZATION_UNIT_HK
  , z.ORGANIZATION_UNIT_NUM
  , z.OutletId
  , z.HUB_RETAILER_ITEM_HK
  , z.RETAILER_ITEM_ID
  , current_timestamp() AS LOAD_TS
  , z.RECORD_SOURCE_CD
  , z.ALERT_MESSAGE_DESC
  , z.ALERT_TYPE_NM
  , z.ON_HAND_INVENTORY_QTY
  , z.LOST_SALES_AMT
  , z.CompanyId
  , z.ProductId
  , z.ManufacturerId
  , z.Lkp_productGroupId
  , z.ProductBrandId
  , z.SALES_DT
FROM (SELECT
        osa.ParentChainId
        , osa.Retail_Client
        , osa.HUB_ORGANIZATION_UNIT_HK
        , osa.ORGANIZATION_UNIT_NUM
        , o.OutletId
        , osa.HUB_RETAILER_ITEM_HK
        , osa.RETAILER_ITEM_ID
        , osa.RECORD_SOURCE_CD
        , osa.ALERT_MESSAGE_DESC
        , osa.ALERT_TYPE_NM
        , osa.ON_HAND_INVENTORY_QTY
        , osa.LOST_SALES_AMT
        , osa.SALES_DT
        , p.CompanyId
        , p.ProductId
        , p.ManufacturerId
        , p.Lkp_productGroupId
        , p.ProductBrandId
        , ROW_NUMBER() OVER(PARTITION BY osa.Sales_DT, osa.Retail_Client, osa.ParentChainId, osa.ORGANIZATION_UNIT_NUM ORDER BY osa.LOST_SALES_AMT DESC) rnum
      FROM ${Team_Name}_${Ctry_Cd}_team_alerts_im.vw_retail_alerts_on_shelf_availability osa
      INNER JOIN ${tempHoldTableName} tmp
        ON osa.Sales_DT = tmp.Sales_DT
        AND osa.Retail_Client = tmp.Retail_Client
        AND osa.RECORD_SOURCE_CD = tmp.RECORD_SOURCE_CD
      INNER JOIN BOBv2.vw_BOBv2_Outlet o
        ON osa.ParentChainId = o.ParentChainId
        AND osa.ORGANIZATION_UNIT_NUM = o.ChainRefExternal
      INNER JOIN BOBv2.vw_bobv2_Product p
        ON p.CompanyId = ${CompanyId}
        AND osa.ParentChainId = p.ParentChainId
        AND RIGHT("00000000000000000000" + LTRIM(RTRIM(osa.RETAILER_ITEM_ID)), 20) = RIGHT("00000000000000000000" + LTRIM(RTRIM(p.RefExternal)), 20)
      INNER JOIN (SELECT DISTINCT OutletId
                  FROM BOBv2.vw_BOBv2_DailyCallfileVisit
                  WHERE
                    CompanyId = ${CompanyId}) cfv
        ON o.OutletId = cfv.OutletId
      INNER JOIN BOBv2.vw_bobv2_caps c
        ON CapType = "OSA LSV Minimum"
        AND p.CompanyId = c.CompanyId
        AND (((lower(RIGHT("${Team_Name}", 4)) = "_syn") AND (p.ManufacturerId = c.ManufacturerId))
            OR
            ((lower(RIGHT("${Team_Name}", 4)) = "_drt")))
        AND p.ManufacturerId = CASE WHEN c.ManufacturerId IS NULL THEN p.ManufacturerId ELSE c.ManufacturerId END
        AND p.Lkp_productGroupId = CASE WHEN c.Lkp_productGroupId IS NULL THEN p.Lkp_productGroupId ELSE c.Lkp_productGroupId END
        AND p.ProductBrandId = CASE WHEN c.ProductBrandId IS NULL THEN p.ProductBrandId ELSE c.ProductBrandId END
      WHERE
        nvl(osa.LOST_SALES_AMT, 0) >= c.CapValue) z
INNER JOIN BOBv2.vw_bobv2_caps c
  ON CapType = "OSARows"
  AND z.CompanyId = c.CompanyId
  AND (((lower(RIGHT("${Team_Name}", 4)) = "_syn") AND (z.ManufacturerId = c.ManufacturerId))
      OR
      ((lower(RIGHT("${Team_Name}", 4)) = "_drt")))
  AND z.ManufacturerId = CASE WHEN c.ManufacturerId IS NULL THEN z.ManufacturerId ELSE c.ManufacturerId END
  AND z.Lkp_productGroupId = CASE WHEN c.Lkp_productGroupId IS NULL THEN z.Lkp_productGroupId ELSE c.Lkp_productGroupId END
  AND z.ProductBrandId = CASE WHEN c.ProductBrandId IS NULL THEN z.ProductBrandId ELSE c.ProductBrandId END
WHERE
  rnum <= c.CapValue
 """
  )
  OnShelfAvaibilityAlerts
}

// COMMAND ----------

import org.apache.spark.sql.functions._
val customers = List("walmart")
val teamName = Team_Name.split("_")(0).toLowerCase() 

spark.conf.set("spark.sql.shuffle.partitions", 10)
create_tmp_hold_audit_team_alerts_osa_sales_dt(Team_Name, Ctry_Cd)

if(customers.contains(teamName)){ 
  // if for retailer decoupling is enabled team name would contain retailer name as well, like retailerName_client_drt (i.e. asda_nestlecereals_drt)
  // else it would client_drt only (i.e. nestlecereals_drt)
  println(s"${Team_Name} has two paritions")
  val onShelfAvailAlerts = getOnShelfAvaibilityAlerts(CompanyId)
  
  val onShelfAvailAlertsWithRetailer = onShelfAvailAlerts.withColumn("customer_nm", if(Team_Name.split("_")(1) == "syn") split($"Retail_Client", "_").getItem(1) else split($"Retail_Client", "_").getItem(0))
  val OnlyRetailerAlerts = onShelfAvailAlertsWithRetailer.filter($"customer_nm" === lit(customer_nm))
  OnlyRetailerAlerts.write.insertInto(s"${Team_Name}_${Ctry_Cd}_team_alerts_im.alert_on_shelf_availability")
}else{
   println(s"${Team_Name} has only one paritions")
   val onShelfAvailAlerts = getOnShelfAvaibilityAlerts(CompanyId)
   onShelfAvailAlerts.write.insertInto(s"${Team_Name}_${Ctry_Cd}_team_alerts_im.alert_on_shelf_availability")
  
}
