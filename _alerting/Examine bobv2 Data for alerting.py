# Databricks notebook source
df_standard_responses_sql_query = """
  SELECT DISTINCT
  -- objective_typ,
  -- nars_response_text,
  standard_response_text,
  standard_response_cd,
  measurement_duration
  FROM acosta_retail_report_im.ds_intervention_summary
  WHERE
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  coalesce(mdm_banner_id, -1) = {banner_id}
  ORDER BY
  standard_response_cd
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)

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
# MAGIC select * from company

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
# MAGIC where FullName like "%estle%"

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select distinct ParentChainId from vw_BOBv2_Product  
# MAGIC where CompanyId = 609 -- Nestle Core

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select * from Chain
# MAGIC where ParentChainId in (1178, 24, 199, 23, 16)

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select * from vw_BOBv2_Product  
# MAGIC where CompanyId = 609 -- Nestle Core

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select * from vw_BOBv2_DailyCallfileVisit 
# MAGIC where notes <> ''
# MAGIC and CompanyId = 609

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC  
# MAGIC select distinct RefExternal from vw_bobv2_Product  
# MAGIC where CompanyId = 609 -- Nestle Core
# MAGIC and ParentChainId = 16 -- Tesco

# COMMAND ----------

# MAGIC %sql
# MAGIC use tesco_nestlecore_uk_dv;
# MAGIC
# MAGIC select distinct retailer_item_id from hub_retailer_item

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC  
# MAGIC select * from vw_bobv2_outlet  
# MAGIC where ParentChainId = 16 -- Tesco

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC  
# MAGIC select distinct ChainRefExternal from vw_bobv2_outlet  
# MAGIC where ParentChainId = 16 -- Tesco

# COMMAND ----------

# MAGIC %sql
# MAGIC use tesco_nestlecore_uk_dv;
# MAGIC
# MAGIC select distinct ORGANIZATION_UNIT_NUM from hub_organization_unit

# COMMAND ----------

# MAGIC %sql
# MAGIC use tesco_nestlecore_uk_dv;
# MAGIC
# MAGIC select distinct HUB_ORGANIZATION_UNIT_HK from hub_organization_unit

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
# MAGIC where CompanyId == 567

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
# MAGIC where CompanyId = 567 -- Walmart Nestlewaters??

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
# MAGIC where CompanyId = 567

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select distinct(ParentChainId)
# MAGIC from chain
# MAGIC where lower(FullName) like '%walmart%' 

# COMMAND ----------


