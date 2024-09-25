# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC show tables from retail_alert_asda_nestlecore_uk_im

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct 
# MAGIC mdm_client_nm,
# MAGIC mdm_banner_nm,
# MAGIC epos_retailer_item_id,
# MAGIC upc
# MAGIC from acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC upc is not null
# MAGIC order by
# MAGIC mdm_client_nm,
# MAGIC mdm_banner_nm,
# MAGIC epos_retailer_item_id,
# MAGIC upc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show create table retail_alert_asda_beiersdorf_uk_im.loess_forecast_baseline_unit_upc

# COMMAND ----------

# CREATE TABLE `retail_alert_asda_beiersdorf_uk_im`.`loess_forecast_baseline_unit_upc` (
#   `HUB_ORGANIZATION_UNIT_HK` STRING,
#   `HUB_RETAILER_ITEM_HK` STRING,
#   `RETAILER_ITEM_ID` STRING,
#   `UPC` STRING,
#   `LOAD_TS` TIMESTAMP,
#   `RECORD_SOURCE_CD` STRING,
#   `BASELINE_POS_ITEM_QTY` DECIMAL(15,2),
#   `UNADJUSTED_BASELINE_POS_ITEM_QTY` DECIMAL(15,2),
#   `SALES_DT` DATE)
# USING org.apache.spark.sql.parquet
# OPTIONS (
#   `compression` 'snappy')
# PARTITIONED BY (SALES_DT)
# LOCATION 'abfss://data@eus2psag2dpcoredatalake.dfs.core.windows.net/informationmart/retail_alert/asda_beiersdorf_uk/loess_forecast_baseline_unit_upc'

# COMMAND ----------

# CREATE VIEW `acosta_retail_analytics_im`.`vw_ds_intervention_input_nars` (
#   `mdm_country_id`,
#   `mdm_country_nm`,
#   `mdm_holding_id`,
#   `mdm_holding_nm`,
#   `mdm_banner_id`,
#   `mdm_banner_nm`,
#   `store_acosta_number`,
#   `epos_organization_unit_num`,
#   `mdm_client_id`,
#   `mdm_client_nm`,
#   `epos_retailer_item_id`,
#   `objective_typ`,
#   `call_date`,
#   `call_id`,
#   `response_id`,
#   `nars_response_text`,
#   `sku_id`,
#   `upc`,
#   `standard_response_text`,
#   `standard_response_cd`,
#   `intervention_rank`,
#   `intervention_group`,
#   `intervention_start_day`,
#   `intervention_end_day`,
#   `actionable_flg`)
# TBLPROPERTIES (
#   'transient_lastDdlTime' = '1695832062')
# AS With
# Questions as
# (
# Select question_id, 'DLA' as obj_typ from acosta_retail_analytics_im.dimension_question
# where (question_type like 'Data Led Alerts%' or  question_type like 'J&J%DLA')
# Union ALL
# Select question_id, 'Opportunity' as obj_typ from acosta_retail_analytics_im.dimension_question
# WHERE date_partition = 999999
# AND (category_name = 'UK Opportunities – Own Choice' or question_name = 'Opportunities')
# AND (question_type <> 'Data Led Alerts' or question_type is null)
# ),
# gc_kroger_stores as
# (
#   select distinct
  
#   o.ChainRefExternal as customer_store_number, 
#   cfv.notes as store_acosta_number
#   from
#     bobv2.vw_BOBv2_DailyCallfileVisit cfv,
#     BOBv2.vw_bobv2_outlet o
#   where
#     cfv.OutletId = o.OutletId and
#     o.ParentChainId=955 and 
#     trim(coalesce(cfv.notes, '')) <> '' and 
#     cfv.notes not like '%-%' 
# ),
# interventions_parameters as
# (
# Select Distinct
# p.mdm_country_id,
# p.mdm_country_nm,
# p.mdm_holding_id,
# p.mdm_holding_nm,
# p.mdm_banner_id,
# p.mdm_banner_nm,
# p.mdm_client_id,
# p.mdm_client_nm,
# CASE WHEN map.objective_typ = 'Data Led Alerts' then 'DLA' when map.objective_typ = 'UK Opportunities' then 'Opportunity' else p.objective_typ end as objective_typ,
# map.nars_response_text       AS nars_response_text,
# map.standard_response_text   AS standard_response_text,
# map.standard_response_cd     AS standard_response_cd,
# p.intervention_rank,
# p.intervention_group,
# p.intervention_start_day,
# p.intervention_end_day,
# p.actionable_flg,
# c.active_flg
# from acosta_retail_analytics_im.interventions_parameters p
# Inner join acosta_retail_analytics_im.interventions_retailer_client_config c
# on c.mdm_client_id = p.mdm_client_id
# and c.mdm_holding_id = p.mdm_holding_id
# AND COALESCE(c.mdm_banner_id, -123) = COALESCE(p.mdm_banner_id, -123)
# Inner join acosta_retail_analytics_im.interventions_response_mapping map
# on map.objective_typ = p.objective_typ
# AND map.standard_response_text = p.standard_response_text
# and map.mdm_country_id = c.mdm_country_id
# ),
# apl
# (Select Distinct sku_id, whse_code, item_level_id, upc, client_id, store_id, holding_id, country_id, banner_id, customer_store_number from acosta_retail_analytics_im.fact_apl
# where lower(status) = lower('Auth') and rn = 1   
# ),
# shelfcd as
# (select *, hp.ShelfCode,  row_number() over(partition by ShelfCode order by hp.active , 
# ModifiedDate desc  ) as hp_rank  from mdm_raw.vw_lookup_product lkp inner join mdm_raw.hp_shelfcode hp on lkp.sku_id=hp.SkuID
# )

# SELECT DISTINCT conf.mdm_country_id AS mdm_country_id,
#                 conf.mdm_country_nm AS mdm_country_nm,
#                 conf.mdm_holding_id AS mdm_holding_id,
#                 conf.mdm_holding_nm AS mdm_holding_nm,
#                 conf.mdm_banner_id AS mdm_banner_id,
#                 conf.mdm_banner_nm AS mdm_banner_nm,
#                 fa.store_id AS store_acosta_number,
#                 Coalesce(Coalesce(ks.customer_store_number, apl.customer_store_number), s.customer_store_number) AS epos_organization_unit_num,
#                 conf.mdm_client_id AS mdm_client_id,
#                 conf.mdm_client_nm AS mdm_client_nm,
#                 case when fa.holding_id = 91 then concat('0', apl.upc)
#                 when q.obj_typ = 'DLA' then coalesce(fa.shelfcode, apl.whse_Code) 
#                 When fa.holding_id = 71 then hp.ShelfCode 
#                 else apl.whse_Code end as epos_retailer_item_id,
#                 conf.objective_typ AS objective_typ,
#                 fa.call_completed_date AS call_date,
#                 fa.call_id,
#                 fa.response_id,
#                 fa.response AS nars_response_text,
#                 fa.item_dimension_id as sku_id,
#                 case when fa.holding_id = 71 then hp.Product_UPC else  apl.upc end as upc,
#                 conf.standard_response_text,
#                 conf.standard_response_cd ,
#                 conf.intervention_rank,
#                 conf.intervention_group,
#                 conf.intervention_start_day,
#                 conf.intervention_end_day,
#                 conf.actionable_flg
# FROM acosta_retail_analytics_im.fact_question_response fa
# INNER JOIN Questions q ON fa.question_id = q.question_id
# INNER JOIN acosta_retail_analytics_im.vw_dimension_store s ON s.store_id = fa.store_id
# LEFT JOIN apl ON apl.store_id = fa.store_id AND apl.sku_id = fa.item_dimension_id
# Left join gc_kroger_stores as ks ON fa.store_id = ks.store_acosta_number
# Left join (select sku_id,ShelfCode, Product_UPC from shelfcd where hp_rank=1) hp on hp.sku_id=fa.item_dimension_id
# Inner Join interventions_parameters as conf
#            ON conf.mdm_country_id = fa.country_id
#            AND conf.mdm_client_id = fa.client_id
#            AND conf.standard_response_text = fa.response
#            AND conf.objective_typ = q.obj_typ
#            AND conf.mdm_holding_id = fa.holding_id
#            AND COALESCE(conf.mdm_banner_id, -1) =
#            CASE WHEN conf.mdm_banner_id IS NULL THEN -1 ELSE s.banner_id END
# WHERE          conf.active_flg = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables from retail_alert_morrisons_nestlecore_uk_im

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables from retail_alert_sainsburys_nestlecore_uk_im

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables from retail_alert_tesco_nestlecore_uk_im

# COMMAND ----------


