# Databricks notebook source
# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select * from vw_BOBv2_DailyCallfileVisit 
# MAGIC and CompanyId = 609

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select * from vw_BOBv2_Product  
# MAGIC where CompanyId = 609 -- Nestle Core
# MAGIC and ParentChainId = 16 -- Tesco

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
# MAGIC
# MAGIC select distinct
# MAGIC iin.mdm_country_id,
# MAGIC iin.mdm_country_nm,
# MAGIC iin.mdm_holding_id,
# MAGIC iin.mdm_holding_nm,
# MAGIC iin.mdm_client_id,
# MAGIC iin.mdm_client_nm,
# MAGIC iin.mdm_banner_id,
# MAGIC iin.mdm_banner_nm,
# MAGIC iin.actionable_flg,
# MAGIC iin.epos_retailer_item_id,
# MAGIC il.upc
# MAGIC from 
# MAGIC acosta_retail_analytics_im.vw_ds_intervention_input_nars iin
# MAGIC join 
# MAGIC acosta_retail_analytics_im.vw_fact_question_response fqr
# MAGIC on 
# MAGIC iin.response_id = fqr.response_id
# MAGIC join
# MAGIC acosta_retail_analytics_im.vw_dimension_itemlevel il
# MAGIC on 
# MAGIC fqr.item_level_id = il.item_level_id
# MAGIC where
# MAGIC iin.mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC iin.mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC iin.mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC coalesce(iin.mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC iin.objective_typ = 'DLA'
# MAGIC and
# MAGIC iin.call_date >= '2022-01-01'
# MAGIC order by 
# MAGIC iin.mdm_country_id,
# MAGIC iin.mdm_country_nm,
# MAGIC iin.mdm_holding_id,
# MAGIC iin.mdm_holding_nm,
# MAGIC iin.mdm_client_id,
# MAGIC iin.mdm_client_nm,
# MAGIC iin.mdm_banner_id,
# MAGIC iin.mdm_banner_nm,
# MAGIC il.upc

# COMMAND ----------


