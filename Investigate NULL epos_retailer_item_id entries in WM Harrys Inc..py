# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC show tables in acosta_retail_analytics_im

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.vw_dimension_itemlevel

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables in bobv2

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bobv2.manufacturer

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc bobv2.vw_bobv2_product

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc bobv2.company

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc bobv2.manufacturer

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC objective_typ='Data Led Alerts'
# MAGIC and
# MAGIC epos_retailer_item_id is NULL
# MAGIC and
# MAGIC actionable_flg is not NULL
# MAGIC and
# MAGIC call_date >= '2022-02-01'
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.vw_fact_question_response

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.fact_questionresponse

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     iin.*,
# MAGIC     
# MAGIC     fqr.store_id,
# MAGIC     fqr.client_id,
# MAGIC     fqr.call_complete_status_id,
# MAGIC     fqr.response_type,
# MAGIC
# MAGIC     fqr.item_level_id,
# MAGIC     fqr.item_dimension_id,
# MAGIC     fqr.item_entity_id,
# MAGIC
# MAGIC     fqr.response,
# MAGIC     fqr.response_value
# MAGIC
# MAGIC from 
# MAGIC acosta_retail_analytics_im.vw_ds_intervention_input_nars iin
# MAGIC join 
# MAGIC acosta_retail_analytics_im.vw_fact_question_response fqr
# MAGIC on 
# MAGIC iin.response_id = fqr.response_id
# MAGIC where
# MAGIC iin.mdm_country_id = 1 -- US
# MAGIC and
# MAGIC iin.mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC iin.mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(iin.mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC iin.objective_typ='Data Led Alerts'
# MAGIC and
# MAGIC iin.epos_retailer_item_id is NULL
# MAGIC and
# MAGIC iin.actionable_flg is not NULL
# MAGIC and
# MAGIC iin.call_date >= '2022-02-01'
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     iin.*,
# MAGIC     
# MAGIC     qr.store_id,
# MAGIC     qr.client_id,
# MAGIC     qr.call_complete_status_id,
# MAGIC     qr.response_type,
# MAGIC
# MAGIC     qr.item_level_id,
# MAGIC     qr.item_dimension_id,
# MAGIC     qr.item_entity_id,
# MAGIC     
# MAGIC     il.upc,
# MAGIC     
# MAGIC     qr.scan_code,
# MAGIC     qr.scan_datetime,
# MAGIC     qr.scan_code_upc,
# MAGIC
# MAGIC     qr.response,
# MAGIC     qr.response_value,
# MAGIC     qr.action_completed,
# MAGIC     qr.system_response,
# MAGIC     qr.action_required
# MAGIC
# MAGIC from 
# MAGIC acosta_retail_analytics_im.vw_ds_intervention_input_nars iin
# MAGIC join 
# MAGIC acosta_retail_analytics_im.fact_questionresponse qr
# MAGIC on 
# MAGIC iin.response_id = qr.response_id
# MAGIC join
# MAGIC acosta_retail_analytics_im.vw_dimension_itemlevel il
# MAGIC on 
# MAGIC qr.item_level_id = il.item_level_id
# MAGIC where
# MAGIC iin.mdm_country_id = 1 -- US
# MAGIC and
# MAGIC iin.mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC iin.mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(iin.mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC iin.objective_typ='Data Led Alerts'
# MAGIC and
# MAGIC iin.epos_retailer_item_id is NULL
# MAGIC and
# MAGIC iin.actionable_flg is not NULL
# MAGIC and
# MAGIC iin.call_date >= '2022-02-01'
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     iin.*,
# MAGIC     
# MAGIC     qr.store_id,
# MAGIC     qr.client_id,
# MAGIC     qr.call_complete_status_id,
# MAGIC     qr.response_type,
# MAGIC
# MAGIC     qr.item_level_id,
# MAGIC     qr.item_dimension_id,
# MAGIC     qr.item_entity_id,
# MAGIC     
# MAGIC     qr.scan_code,
# MAGIC     qr.scan_datetime,
# MAGIC     qr.scan_code_upc,
# MAGIC
# MAGIC     qr.response,
# MAGIC     qr.response_value,
# MAGIC     qr.action_completed,
# MAGIC     qr.system_response,
# MAGIC     qr.action_required
# MAGIC
# MAGIC from 
# MAGIC acosta_retail_analytics_im.vw_ds_intervention_input_nars iin
# MAGIC join 
# MAGIC acosta_retail_analytics_im.fact_questionresponse qr
# MAGIC on 
# MAGIC iin.response_id = qr.response_id
# MAGIC where
# MAGIC iin.mdm_country_id = 1 -- US
# MAGIC and
# MAGIC iin.mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC iin.mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(iin.mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC iin.epos_retailer_item_id is NULL
# MAGIC and
# MAGIC iin.actionable_flg is not NULL
# MAGIC and
# MAGIC iin.call_date >= '2022-02-01'
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     iin.*,
# MAGIC     
# MAGIC     qr.store_id,
# MAGIC     qr.client_id,
# MAGIC     qr.call_complete_status_id,
# MAGIC     qr.response_type,
# MAGIC
# MAGIC     qr.item_level_id,
# MAGIC     qr.item_dimension_id,
# MAGIC     qr.item_entity_id,
# MAGIC     
# MAGIC     qr.scan_code,
# MAGIC     qr.scan_datetime,
# MAGIC     qr.scan_code_upc,
# MAGIC
# MAGIC     qr.response,
# MAGIC     qr.response_value,
# MAGIC     qr.action_completed,
# MAGIC     qr.system_response,
# MAGIC     qr.action_required
# MAGIC
# MAGIC from 
# MAGIC acosta_retail_analytics_im.vw_ds_intervention_input_nars iin
# MAGIC join 
# MAGIC acosta_retail_analytics_im.fact_questionresponse qr
# MAGIC on 
# MAGIC iin.response_id = qr.response_id
# MAGIC where
# MAGIC iin.mdm_country_id = 1 -- US
# MAGIC and
# MAGIC iin.mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC iin.mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(iin.mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC iin.objective_typ='Data Led Alerts'
# MAGIC and
# MAGIC iin.epos_retailer_item_id is NULL
# MAGIC and
# MAGIC iin.actionable_flg is not NULL
# MAGIC and
# MAGIC iin.call_date >= '2022-02-01'
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     iin.*,
# MAGIC     
# MAGIC     fqr.store_id,
# MAGIC     fqr.client_id,
# MAGIC     fqr.call_complete_status_id,
# MAGIC     fqr.response_type,
# MAGIC
# MAGIC     fqr.item_level_id,
# MAGIC     fqr.item_dimension_id,
# MAGIC     fqr.item_entity_id,
# MAGIC     
# MAGIC     il.upc,
# MAGIC
# MAGIC     fqr.response,
# MAGIC     fqr.response_value
# MAGIC
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
# MAGIC iin.mdm_country_id = 1 -- US
# MAGIC and
# MAGIC iin.mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC iin.mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(iin.mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC iin.objective_typ='Data Led Alerts'
# MAGIC and
# MAGIC iin.epos_retailer_item_id is NULL
# MAGIC and
# MAGIC iin.actionable_flg is not NULL
# MAGIC and
# MAGIC iin.call_date >= '2022-02-01'
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc bobv2.product

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select ProductId, UniversalProductCode from bobv2.product
# MAGIC where CompanyId = 567 -- Walmart USA_DLA
# MAGIC and RIGHT("00000000000000000000" + LTRIM(RTRIM(UniversalProductCode)), 20) = RIGHT("00000000000000000000" + LTRIM(RTRIM('085523500744')), 20)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bobv2.product
# MAGIC where CompanyId = 567 -- Walmart USA_DLA
# MAGIC and RIGHT("00000000000000000000" + LTRIM(RTRIM(UniversalProductCode)), 20) = RIGHT("00000000000000000000" + LTRIM(RTRIM('085523500744')), 20)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc bobv2.vw_bobv2_product

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select ProductId, RefExternal from bobv2.vw_bobv2_product
# MAGIC where CompanyId = 567 -- Walmart USA_DLA

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct retailer_item_id from retaillink_walmart_harrysinc_us_dv.vw_latest_sat_epos_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc  
# MAGIC retaillink_walmart_harrysinc_us_dv.vw_latest_sat_epos_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from  
# MAGIC retaillink_walmart_harrysinc_us_dv.vw_latest_sat_epos_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select  
# MAGIC sales.retailer_item_id, p.UniversalProductCode, p.FullName, sum(sales.pos_item_qty) as total_sales_units, sum(sales.pos_amt) as total_sales_dollars
# MAGIC from
# MAGIC retaillink_walmart_harrysinc_us_dv.vw_latest_sat_epos_summary sales
# MAGIC left join
# MAGIC bobv2.vw_bobv2_product bp
# MAGIC on
# MAGIC right("00000000000000000000" + ltrim(rtrim(sales.RETAILER_ITEM_ID)), 20) = right("00000000000000000000" + ltrim(rtrim(bp.RefExternal)), 20)
# MAGIC left join
# MAGIC bobv2.product p
# MAGIC on
# MAGIC bp.ProductId = p.ProductId
# MAGIC -- where 
# MAGIC -- bp.CompanyId = 567 -- Walmart USA_DLA
# MAGIC -- and
# MAGIC -- p.CompanyId = 567 -- Walmart USA_DLA
# MAGIC where
# MAGIC p.UniversalProductCode is not NULL
# MAGIC and
# MAGIC sales.sales_dt >= '2022-01-01'
# MAGIC group by
# MAGIC sales.retailer_item_id, p.UniversalProductCode, p.FullName
# MAGIC order by
# MAGIC total_sales_dollars
# MAGIC desc

# COMMAND ----------


