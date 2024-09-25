# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC use acosta_retail_analytics_im;
# MAGIC
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.dim_profile

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.fact_question_response

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.fact_calls

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.interventions_retailer_client_config

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.interventions_response_mapping
# MAGIC order by mdm_country_id, objective_typ, standard_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.interventions_response_mapping
# MAGIC where mdm_country_id = 30
# MAGIC order by objective_typ, standard_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct mdm_country_id, mdm_country_nm, standard_response_cd, nars_response_text from acosta_retail_analytics_im.interventions_response_mapping
# MAGIC where mdm_country_id = 30
# MAGIC and nars_response_text not like '%,%'
# MAGIC order by standard_response_cd, nars_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.interventions_response_mapping
# MAGIC where objective_typ = 'Data Led Alerts'
# MAGIC order by mdm_country_id, objective_typ, standard_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.interventions_parameters

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.interventions_parameters
# MAGIC order by mdm_country_id, objective_typ, standard_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC       objective_typ,
# MAGIC       standard_response_cd,
# MAGIC       standard_response_text,
# MAGIC       intervention_rank,
# MAGIC       intervention_group,
# MAGIC       intervention_start_day,
# MAGIC       intervention_end_day,
# MAGIC       actionable_flg
# MAGIC   FROM acosta_retail_analytics_im.interventions_parameters
# MAGIC   WHERE mdm_country_id = 30 -- UK

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC --       objective_typ,
# MAGIC       standard_response_cd,
# MAGIC       standard_response_text,
# MAGIC       intervention_rank,
# MAGIC       intervention_group,
# MAGIC       intervention_start_day,
# MAGIC       intervention_end_day,
# MAGIC       actionable_flg
# MAGIC   FROM acosta_retail_analytics_im.interventions_parameters
# MAGIC   WHERE mdm_country_id = 30 -- UK
# MAGIC   and standard_response_text like '%,%'
# MAGIC   order by standard_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC mdm_country_id,
# MAGIC mdm_country_nm,
# MAGIC mdm_holding_id,
# MAGIC mdm_holding_nm,
# MAGIC mdm_client_id,
# MAGIC mdm_client_nm,
# MAGIC --       objective_typ,
# MAGIC       standard_response_cd,
# MAGIC       standard_response_text,
# MAGIC       intervention_rank,
# MAGIC       intervention_group,
# MAGIC       intervention_start_day,
# MAGIC       intervention_end_day,
# MAGIC       actionable_flg
# MAGIC   FROM acosta_retail_analytics_im.interventions_parameters
# MAGIC   WHERE mdm_country_id = 30 -- UK
# MAGIC   and standard_response_text not like '%,%'
# MAGIC   order by standard_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC       objective_typ,
# MAGIC       standard_response_cd,
# MAGIC       standard_response_text,
# MAGIC       intervention_rank,
# MAGIC       intervention_group,
# MAGIC       intervention_start_day,
# MAGIC       intervention_end_day,
# MAGIC       actionable_flg
# MAGIC   FROM acosta_retail_analytics_im.interventions_parameters
# MAGIC   WHERE mdm_country_id = 30 -- UK
# MAGIC   AND objective_typ = 'Data Led Alerts' -- 360 Data Led Alerts

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.ds_intervention_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) FROM morrisons_nestlecore_uk_retail_alert_im.alert_on_shelf_availability
# MAGIC where SALES_DT >= '2022-01-01'
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- standard_response_text in
# MAGIC -- ("Increased Facings",
# MAGIC -- "Merchandising Fixture",
# MAGIC -- "Pallet Corner",
# MAGIC -- "Promotion Re-Charge")
# MAGIC order by
# MAGIC standard_response_text,
# MAGIC mdm_banner_id,
# MAGIC objective_typ

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.interventions_response_mapping
# MAGIC where objective_typ = 'Data Led Alerts'
# MAGIC order by mdm_country_id, objective_typ, standard_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'Data Led Alerts'
# MAGIC order by
# MAGIC standard_response_text,
# MAGIC mdm_banner_id,
# MAGIC objective_typ

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC call_date >= '2022-10-01'
# MAGIC and
# MAGIC call_date <= '2022-10-31'
# MAGIC and
# MAGIC epos_retailer_item_id
# MAGIC in
# MAGIC (
# MAGIC 5510017,
# MAGIC 5826568, 
# MAGIC 9220190, 
# MAGIC 6019058, 
# MAGIC 5733204, 
# MAGIC 6524742
# MAGIC )
# MAGIC order by
# MAGIC epos_retailer_item_id, call_date, epos_organization_unit_num

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC call_date >= '2022-09-01'
# MAGIC and
# MAGIC call_date <= '2022-09-30'
# MAGIC and
# MAGIC epos_retailer_item_id
# MAGIC in
# MAGIC (
# MAGIC 5510017,
# MAGIC 5826568, 
# MAGIC 9220190, 
# MAGIC 6019058, 
# MAGIC 5733204, 
# MAGIC 6524742
# MAGIC )
# MAGIC order by
# MAGIC epos_retailer_item_id, call_date, epos_organization_unit_num

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC call_date >= '2022-09-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC actionable_flg is not NULL
# MAGIC and
# MAGIC call_date >= '2022-09-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC call_date >= '2022-09-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, response_id, count(*) AS num_responses FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC call_date >= '2022-09-01'
# MAGIC group by call_date, response_id
# MAGIC order by num_responses desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, response_id, count(*) AS num_responses FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC call_date >= '2022-09-01'
# MAGIC group by call_date, response_id
# MAGIC order by num_responses desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC objective_typ, epos_organization_unit_num, epos_retailer_item_id, response_id, call_date, count(response_id) as num_responses 
# MAGIC FROM 
# MAGIC acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC call_date >= '2022-09-01'
# MAGIC group by 
# MAGIC objective_typ, epos_organization_unit_num, epos_retailer_item_id, response_id, call_date
# MAGIC order by 
# MAGIC num_responses desc,
# MAGIC objective_typ, epos_organization_unit_num, epos_retailer_item_id, call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC objective_typ, mdm_country_id, mdm_client_id, mdm_holding_id, epos_organization_unit_num, epos_retailer_item_id, response_id, call_date, count(response_id) as num_responses 
# MAGIC FROM 
# MAGIC acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC -- mdm_country_id = 30 -- UK
# MAGIC -- and
# MAGIC -- mdm_client_id = 16320 -- Nestle UK
# MAGIC -- and
# MAGIC -- mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- objective_typ = 'Data Led Alerts'
# MAGIC -- and
# MAGIC call_date >= '2022-09-01'
# MAGIC group by 
# MAGIC objective_typ, mdm_country_id, mdm_client_id, mdm_holding_id, epos_organization_unit_num, epos_retailer_item_id, response_id, call_date
# MAGIC order by 
# MAGIC num_responses desc,
# MAGIC objective_typ, mdm_country_id, mdm_client_id, mdm_holding_id, epos_organization_unit_num, epos_retailer_item_id, call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, epos_organization_unit_num, epos_retailer_item_id, count(*) as num_responses FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC call_date >= '2022-09-01'
# MAGIC group by call_date, epos_organization_unit_num, epos_retailer_item_id
# MAGIC order by num_responses desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, epos_organization_unit_num, epos_retailer_item_id, count(*) as num_responses FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC call_date >= '2022-09-01'
# MAGIC and
# MAGIC actionable_flg is not NULL
# MAGIC group by call_date, epos_organization_unit_num, epos_retailer_item_id
# MAGIC order by num_responses desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC call_date >= '2022-09-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC call_date >= '2022-09-01'
# MAGIC and
# MAGIC actionable_flg is not NULL
# MAGIC and
# MAGIC call_date >= '2022-09-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *
# MAGIC from acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where call_date = '2022-09-08'
# MAGIC   and call_id = 53722833

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.vw_ds_intervention_input_dla

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_dla
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC call_date >= '2022-09-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc premium360_analytics_im.vw_fact_answer_alerts

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from premium360_analytics_im.vw_fact_answer_alerts

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct shelfcode from premium360_analytics_im.vw_fact_answer_alerts
# MAGIC where holding_id = 71 -- walmart

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct fa.shelfcode from premium360_analytics_im.vw_fact_answer_alerts fa
# MAGIC Inner join premium360_analytics_im.vw_dimension_question q
# MAGIC on fa.question_id = q.question_id
# MAGIC where fa.holding_id = 71 -- walmart 
# MAGIC and fa.source in ('P360_DP')
# MAGIC and fa.calendar_key >='20220612'
# MAGIC and q.question_type = 'Data Led Alerts'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc premium360_analytics_im.vw_dimension_question

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from premium360_analytics_im.vw_dimension_question

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc vw_fact_question_response

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC -- coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC -- and
# MAGIC -- objective_typ='DLA'
# MAGIC standard_response_text in
# MAGIC ("Merchandising Display",
# MAGIC "Merchandising Fixture")
# MAGIC and
# MAGIC call_date >= '2022-03-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC -- coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC -- and
# MAGIC -- objective_typ='DLA'
# MAGIC standard_response_text in
# MAGIC ("Merchandising Display",
# MAGIC "Merchandising Fixture")
# MAGIC and
# MAGIC call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC call_date >= '2022-09-01'
# MAGIC and
# MAGIC call_date <= '2022-09-30'
# MAGIC and
# MAGIC epos_retailer_item_id
# MAGIC in
# MAGIC (
# MAGIC 5510017,
# MAGIC 5826568, 
# MAGIC 9220190, 
# MAGIC 6019058, 
# MAGIC 5733204, 
# MAGIC 6524742
# MAGIC )
# MAGIC order by
# MAGIC epos_retailer_item_id, call_date, epos_organization_unit_num

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT mdm_banner_id, call_date, count(*), sum(total_intervention_effect), sum(total_impact) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC -- and
# MAGIC -- objective_typ='DLA'
# MAGIC -- standard_response_text in
# MAGIC -- ("Merchandising Display",
# MAGIC -- "Merchandising Fixture")
# MAGIC and
# MAGIC call_date >= '2022-03-01'
# MAGIC group by mdm_banner_id, call_date
# MAGIC order by mdm_banner_id, call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT mdm_banner_id, call_date, count(*), sum(total_intervention_effect), sum(total_impact) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC call_date >= '2022-03-01'
# MAGIC group by mdm_banner_id, call_date
# MAGIC order by mdm_banner_id, call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- objective_typ = 'Data Led Alerts'
# MAGIC -- and
# MAGIC -- coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'Data Led Alerts'
# MAGIC -- and
# MAGIC -- coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC is_complete = 'true'
# MAGIC and
# MAGIC call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC is_complete = 'false'
# MAGIC and
# MAGIC call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC call_date >= '2022-03-01'
# MAGIC and
# MAGIC is_complete = 'false'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7744 -- Morrisons
# MAGIC and
# MAGIC call_date >= '2022-03-01'
# MAGIC and
# MAGIC is_complete = 'false'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7744 -- Morrisons
# MAGIC and
# MAGIC call_date >= '2022-03-01'
# MAGIC and
# MAGIC is_complete = 'true'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, sum(total_intervention_effect) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, sum(total_intervention_effect) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'Data Led Alerts'
# MAGIC -- and
# MAGIC -- coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, sum(total_intervention_effect) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7743
# MAGIC and
# MAGIC call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, sum(total_intervention_effect) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7744
# MAGIC and
# MAGIC call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, sum(total_intervention_effect) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7745
# MAGIC and
# MAGIC call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, sum(total_intervention_effect) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, sum(total_impact) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC call_date >= '2022-07-27'
# MAGIC and
# MAGIC is_complete = 'true'
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC call_date >= '2022-06-01'
# MAGIC and
# MAGIC call_date < '2022-07-01'
# MAGIC and
# MAGIC is_complete = 'false'
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7744 -- Morrisons
# MAGIC and
# MAGIC call_date >= '2022-06-01'
# MAGIC and
# MAGIC call_date < '2022-07-01'
# MAGIC and
# MAGIC is_complete = 'false'
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7744 -- Morrisons
# MAGIC and
# MAGIC call_date >= '2022-06-20'
# MAGIC and
# MAGIC call_date < '2023-07-01'
# MAGIC and
# MAGIC is_complete = 'false'
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7744 -- Morrisons
# MAGIC and
# MAGIC call_date >= '2022-06-20'
# MAGIC and
# MAGIC call_date < '2023-07-01'
# MAGIC and
# MAGIC is_complete = 'true'
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC call_date >= '2022-07-28'
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT mdm_banner_id, call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC epos_retailer_item_id is not NULL
# MAGIC and
# MAGIC actionable_flg is not NULL
# MAGIC and
# MAGIC call_date >= '2022-03-01'
# MAGIC group by mdm_banner_id, call_date
# MAGIC order by mdm_banner_id, call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC epos_retailer_item_id is not NULL
# MAGIC and
# MAGIC actionable_flg is not NULL
# MAGIC and
# MAGIC call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------


