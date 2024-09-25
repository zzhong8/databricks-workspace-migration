# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC select distinct
# MAGIC RETAILER_ITEM_ID
# MAGIC from
# MAGIC retaillink_walmart_campbellssnack_us_dv.vw_latest_sat_epos_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct
# MAGIC RETAILER_ITEM_ID
# MAGIC from
# MAGIC retaillink_walmart_nestlewaters_us_dv.vw_latest_sat_epos_summary

# COMMAND ----------

sql = "SELECT \
          \"walmart_rbusahygiene_us\" AS RETAIL_CLIENT \
          , 950 AS ParentChainId \
          , s.SALES_DT \
          , s.HUB_ORGANIZATION_UNIT_HK \
          , s.HUB_RETAILER_ITEM_HK \
          , s.POS_ITEM_QTY \
          , s.POS_AMT \
          , s.ON_HAND_INVENTORY_QTY \
          , ri.RETAILER_ITEM_ID \
          , ou.ORGANIZATION_UNIT_NUM \
          , d.BASELINE_POS_ITEM_QTY \
        FROM walmart_rbusahygiene_us_dv.vw_sat_link_epos_summary s \
        INNER JOIN walmart_rbusahygiene_us_dv.hub_retailer_item ri \
          ON s.HUB_RETAILER_ITEM_HK = ri.HUB_RETAILER_ITEM_HK \
        INNER JOIN walmart_rbusahygiene_us_dv.hub_organization_unit ou \
          ON s.HUB_ORGANIZATION_UNIT_HK = ou.HUB_ORGANIZATION_UNIT_HK \
        LEFT JOIN walmart_rbusahygiene_us_retail_alert_im.vw_drfe_forecast_baseline_unit d \
        /* This could be replaced with expected baseline table */ \
          ON s.SALES_DT = d.SALES_DT \
          AND s.HUB_ORGANIZATION_UNIT_HK = d.HUB_ORGANIZATION_UNIT_HK \
          AND s.HUB_RETAILER_ITEM_HK = d.HUB_RETAILER_ITEM_HK"

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select * from outlet
# MAGIC
# MAGIC WHERE Chainid in (select Chainid from BOBv2.chain WHERE parentchainid=955)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use BOBv2;
# MAGIC
# MAGIC select * from vw_BOBv2_DailyCallfileVisit where notes <> ''

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select * from vw_BOBv2_DailyCallfileVisit 
# MAGIC where notes <> ''
# MAGIC and CompanyId = 559
# MAGIC and OutletId in (select OutletId from outlet where Chainid in (select Chainid from BOBv2.chain WHERE parentchainid=955))

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select * from vw_BOBv2_DailyCallfileVisit 
# MAGIC where notes <> ''
# MAGIC and CompanyId = 603
# MAGIC and OutletId in (select OutletId from outlet where Chainid in (select Chainid from BOBv2.chain WHERE parentchainid=955))

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select * from vw_BOBv2_DailyCallfileVisit 
# MAGIC where CompanyId = 607

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select * from vw_BOBv2_DailyCallfileVisit 
# MAGIC where notes <> ''
# MAGIC and CompanyId = 603

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select * from Company

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select * from Company
# MAGIC where FullName like "%ater%"

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select * from Company
# MAGIC where FullName like "%estle%"

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select * from vw_BOBv2_Product  
# MAGIC where CompanyId = 603 -- Kroger Danone

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select * from Product  
# MAGIC where CompanyId = 603 -- Kroger Danone

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select * from vw_BOBv2_Product  
# MAGIC where CompanyId = 607 -- Walmart Nestlewaters??

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select * from vw_BOBv2_Product  
# MAGIC where CompanyId = 347 -- Walmart Nestlewaters??

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select * from vw_BOBv2_Product  
# MAGIC where CompanyId = 609 -- Nestle UK??

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select distinct(ParentChainId)
# MAGIC from chain
# MAGIC where lower(FullName) like '%kroger%' 

# COMMAND ----------


