# Databricks notebook source
from pprint import pprint

import pyspark.sql.functions as pyf

from acosta.measurement import required_columns, process_notebook_inputs

import acosta
from acosta.alerting.preprocessing import read_pos_data

print(acosta.__version__)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use tesco_nestlecore_uk_retail_alert_im;
# MAGIC
# MAGIC show tables

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from alert_inventory_cleanup
# MAGIC where SALES_DT = '2021-06-13'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use tesco_nestlecore_uk_dv;
# MAGIC
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc hub_retailer_item 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc sat_retailer_item

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc BOBv2.vw_BOBv2_Product

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from BOBv2.vw_BOBv2_Product

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc vw_latest_sat_retailer_item

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc alert_inventory_cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from alert_on_shelf_availability

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc alert_on_shelf_availability

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from alert_inventory_cleanup
# MAGIC where SALES_DT = '2021-06-13'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from alert_inventory_cleanup
# MAGIC where SALES_DT = '2021-06-13'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc vw_alert_inventory_cleanup

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_UK_retail_analytics_im;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_UK_retail_analytics_im;
# MAGIC
# MAGIC select * from vw_fact_question_response a join
# MAGIC (select distinct store_id from vw_dimension_store where banner_id = 7746) b
# MAGIC on a.store_id = b.store_id
# MAGIC where holding_id = 3257
# MAGIC and channel_id = 1
# MAGIC and client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)
# MAGIC and master_client_id = 16320
# MAGIC and country_id = 30
# MAGIC and call_completed_date >= '2021-03-14'
# MAGIC and call_completed_date <= '2021-03-20';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_UK_retail_analytics_im;
# MAGIC
# MAGIC select * from vw_fact_question_response a join
# MAGIC (select distinct store_id from vw_dimension_store where banner_id = 7746) b
# MAGIC on a.store_id = b.store_id
# MAGIC where holding_id = 3257
# MAGIC and channel_id = 1
# MAGIC and client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)
# MAGIC and master_client_id = 16320
# MAGIC and country_id = 30
# MAGIC and ((tm_executed_emp_id is not null) or (um_executed_emp_id is not null))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_UK_retail_analytics_im;
# MAGIC
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW COLUMNS IN vw_dimension_store

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from vw_dimension_call_type
# MAGIC where channel_id = 1
# MAGIC and country_id = 30

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show columns in vw_dimension_question

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC distinct
# MAGIC --question_id,
# MAGIC --question,
# MAGIC question_name,
# MAGIC question_group,
# MAGIC question_category,
# MAGIC -- question_start_date,
# MAGIC -- question_end_date,
# MAGIC question_status,
# MAGIC question_type,
# MAGIC response_type,
# MAGIC priority,
# MAGIC ask_choice_id,
# MAGIC ask_choice_description,
# MAGIC question_code,
# MAGIC client_id,
# MAGIC master_client_id,
# MAGIC channel_id,
# MAGIC holding_id,
# MAGIC country_id,
# MAGIC active,
# MAGIC deleted,
# MAGIC -- insert_datetime,
# MAGIC -- update_datetime,
# MAGIC STATUS,
# MAGIC acat_response_id,
# MAGIC no_filter_apl,
# MAGIC library_id,
# MAGIC compliance_op,
# MAGIC compliance_eq,
# MAGIC compliance_min,
# MAGIC compliance_max,
# MAGIC category_name,
# MAGIC question_reference,
# MAGIC client_audit,
# MAGIC void_origin
# MAGIC from vw_dimension_question
# MAGIC where holding_id = 3257
# MAGIC and channel_id = 1
# MAGIC and client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)
# MAGIC and country_id = 30

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC question_id,
# MAGIC question,
# MAGIC question_name,
# MAGIC question_group,
# MAGIC question_category,
# MAGIC question_start_date,
# MAGIC question_end_date,
# MAGIC question_status,
# MAGIC question_type,
# MAGIC response_type,
# MAGIC priority,
# MAGIC ask_choice_id,
# MAGIC ask_choice_description,
# MAGIC question_code,
# MAGIC client_id,
# MAGIC master_client_id,
# MAGIC channel_id,
# MAGIC holding_id,
# MAGIC country_id,
# MAGIC active,
# MAGIC deleted,
# MAGIC insert_datetime,
# MAGIC update_datetime,
# MAGIC STATUS,
# MAGIC acat_response_id,
# MAGIC no_filter_apl,
# MAGIC library_id,
# MAGIC compliance_op,
# MAGIC compliance_eq,
# MAGIC compliance_min,
# MAGIC compliance_max,
# MAGIC category_name,
# MAGIC question_reference,
# MAGIC client_audit,
# MAGIC void_origin
# MAGIC from vw_dimension_question
# MAGIC where holding_id = 3257
# MAGIC and channel_id = 1
# MAGIC and client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)
# MAGIC and country_id = 30
# MAGIC and question_start_date = '2021-03-16'
# MAGIC and question_end_date <> '2021-03-16'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*)
# MAGIC from vw_dimension_question
# MAGIC where holding_id = 3257 -- Acosta Retail UK
# MAGIC and channel_id = 1
# MAGIC and client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)
# MAGIC and country_id = 30
# MAGIC and question_start_date = '2021-06-10'
# MAGIC and question_end_date = '2021-06-10'
# MAGIC and question like '%Inventory Cleanup%'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*)
# MAGIC from vw_dimension_question
# MAGIC where holding_id = 3257
# MAGIC and channel_id = 1
# MAGIC and client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)
# MAGIC and country_id = 30
# MAGIC and question_start_date = '2021-03-16'
# MAGIC and question_end_date = '2021-03-16'
# MAGIC and question like '%Inventory Cleanup%'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *
# MAGIC from vw_dimension_question
# MAGIC where holding_id = 3257
# MAGIC and channel_id = 1
# MAGIC and client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)
# MAGIC and country_id = 30
# MAGIC and question_start_date = '2021-03-16'
# MAGIC and question_end_date = '2021-03-16'
# MAGIC and question like '%Inventory Cleanup%'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *
# MAGIC from vw_dimension_question
# MAGIC where holding_id = 3257 -- Acosta Retail UK
# MAGIC and channel_id = 1
# MAGIC and client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)
# MAGIC and country_id = 30 -- UK
# MAGIC and question_start_date = '2021-03-16'
# MAGIC and question_end_date = '2021-03-16'
# MAGIC and question like '%On Shelf Availability%'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *
# MAGIC from vw_dimension_question
# MAGIC where holding_id = 3257 -- Acosta Retail UK
# MAGIC and channel_id = 1
# MAGIC and client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)
# MAGIC and country_id = 30 -- UK
# MAGIC and question_start_date = '2021-06-24'
# MAGIC and question_end_date = '2021-06-24'
# MAGIC and question like '%On Shelf Availability%'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC count(*)
# MAGIC from vw_dimension_question
# MAGIC where holding_id = 3257 -- Acosta Retail UK
# MAGIC and channel_id = 1
# MAGIC and client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)
# MAGIC and country_id = 30 -- UK
# MAGIC and question_start_date = '2021-03-16'
# MAGIC and question_end_date = '2021-03-16'
# MAGIC and question like '%On Shelf Availability%'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC count(*)
# MAGIC from vw_dimension_question
# MAGIC where holding_id = 3257
# MAGIC and channel_id = 1
# MAGIC and client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)
# MAGIC and country_id = 30
# MAGIC and question_start_date = '2021-03-16'
# MAGIC and question_end_date = '2021-03-16'
# MAGIC and (question like '%On Shelf Availability (Availability (Phantom Inventory/Book Stock Error))%' or
# MAGIC      question like '%On Shelf Availability (Slow Sales)%')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC count(*)
# MAGIC from vw_dimension_question
# MAGIC where holding_id = 3257
# MAGIC and channel_id = 1
# MAGIC and client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)
# MAGIC and country_id = 30
# MAGIC and question_start_date = '2021-03-15'
# MAGIC and question_end_date = '2021-03-15'
# MAGIC and (question like '%On Shelf Availability (Availability (Phantom Inventory/Book Stock Error))%' or
# MAGIC      question like '%On Shelf Availability (Slow Sales)%')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC count(*)
# MAGIC from vw_dimension_question
# MAGIC where holding_id = 3257
# MAGIC and channel_id = 1
# MAGIC and client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)
# MAGIC and country_id = 30
# MAGIC and insert_datetime like '2021-03-16%'
# MAGIC and update_datetime like '2021-03-16%'
# MAGIC and (question like '%On Shelf Availability (Availability (Phantom Inventory/Book Stock Error))%' or
# MAGIC      question like '%On Shelf Availability (Slow Sales)%')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW COLUMNS IN nestle_temp_fix_vw_fact_question_response

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from nestle_temp_fix_vw_fact_question_response;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select min(response_last_modified_date), max(response_last_modified_date), min(call_completed_date), max(call_completed_date) from nestle_temp_fix_vw_fact_question_response;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct holding_id, channel_id, client_id, master_client_id from nestle_temp_fix_vw_fact_question_response;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct holding_id, channel_id, client_id, master_client_id from nestle_temp_fix_vw_fact_question_response
# MAGIC where call_completed_date like '2021-03%';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct holding_id, channel_id, client_id, master_client_id from nestle_temp_fix_vw_fact_question_response
# MAGIC where client_id = 16320
# MAGIC and master_client_id = 16320
# MAGIC and call_completed_date like '2021-03%';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from nestle_temp_fix_vw_fact_question_response;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from nestle_temp_fix_vw_fact_question_response
# MAGIC where client_id = 16320
# MAGIC and master_client_id = 16320
# MAGIC and call_completed_date like '2021-03-24';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct channel_id, response from nestle_temp_fix_vw_fact_question_response
# MAGIC where client_id = 16320
# MAGIC and master_client_id = 16320
# MAGIC and call_completed_date like '2021-03-24';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct channel_id, response from vw_fact_question_response
# MAGIC where channel_id = 1
# MAGIC and client_id = 16320
# MAGIC and master_client_id = 16320
# MAGIC and call_completed_date like '2021-03-24';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct team_executed_code, team_planned_code from vw_fact_question_response
# MAGIC where channel_id = 1
# MAGIC and holding_id = 3257
# MAGIC and client_id = 16320
# MAGIC and master_client_id = 16320
# MAGIC and call_completed_date = '2021-03-24';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct team_executed_code, team_planned_code from nestle_temp_fix_vw_fact_question_response
# MAGIC where channel_id = 1
# MAGIC and holding_id = 3257
# MAGIC and client_id = 16320
# MAGIC and master_client_id = 16320
# MAGIC and call_completed_date = '2021-03-24';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_UK_retail_analytics_im;
# MAGIC
# MAGIC SHOW COLUMNS IN vw_fact_question_response

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_UK_retail_analytics_im;
# MAGIC
# MAGIC select count(*) from vw_fact_question_response a join
# MAGIC (select distinct store_id from vw_dimension_store where banner_id = 7746) b
# MAGIC on a.store_id = b.store_id join
# MAGIC (select distinct question_id from vw_dimension_question
# MAGIC where holding_id = 3257 -- Acosta Retail UK
# MAGIC and channel_id = 1
# MAGIC and client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)
# MAGIC and country_id = 30 -- UK
# MAGIC -- and question_start_date = '2021-03-16'
# MAGIC -- and question_end_date = '2021-03-16'
# MAGIC and question like '%On Shelf Availability%') c 
# MAGIC on a.question_id = c.question_id
# MAGIC and call_completed_date >= '2021-06-24'
# MAGIC and call_completed_date <= '2021-06-24';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_UK_retail_analytics_im;
# MAGIC
# MAGIC select a.* from vw_fact_question_response a join
# MAGIC (select distinct store_id from vw_dimension_store where banner_id = 7746) b
# MAGIC on a.store_id = b.store_id join
# MAGIC (select distinct question_id from vw_dimension_question
# MAGIC where holding_id = 3257 -- Acosta Retail UK
# MAGIC and channel_id = 1
# MAGIC and client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)
# MAGIC and country_id = 30 -- UK
# MAGIC -- and question_start_date = '2021-03-16'
# MAGIC -- and question_end_date = '2021-03-16'
# MAGIC and question like '%On Shelf Availability%') c 
# MAGIC on a.question_id = c.question_id
# MAGIC and call_completed_date >= '2021-06-16'
# MAGIC and call_completed_date <= '2021-06-30';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_UK_retail_analytics_im;
# MAGIC
# MAGIC select a.* from vw_fact_question_response a join
# MAGIC (select distinct store_id from vw_dimension_store where banner_id = 7746) b
# MAGIC on a.store_id = b.store_id join
# MAGIC (select distinct question_id from vw_dimension_question
# MAGIC where holding_id = 3257 -- Acosta Retail UK
# MAGIC and channel_id = 1
# MAGIC and client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)
# MAGIC and country_id = 30 -- UK
# MAGIC -- and question_start_date = '2021-03-16'
# MAGIC -- and question_end_date = '2021-03-16'
# MAGIC and question like '%On Shelf Availability%') c 
# MAGIC on a.question_id = c.question_id
# MAGIC and call_completed_date >= '2021-06-16'
# MAGIC and call_completed_date <= '2021-06-30';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from vw_dimension_client

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from vw_dimension_client
# MAGIC where description like '%estle%'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW COLUMNS IN vw_fact_question_response

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from vw_fact_question_response;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from vw_fact_question_response;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select min(response_last_modified_date), max(response_last_modified_date), min(call_completed_date), max(call_completed_date) from vw_fact_question_response;

# COMMAND ----------



# COMMAND ----------

country_id = 1  # US
client_id = 460 # Minute Maid
holding_id = 71 # Walmart
banner_id = -1  # default

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct mdm_holding_nm, mdm_holding_id FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct mdm_holding_nm, mdm_holding_id FROM acosta_retail_analytics_im.ds_intervention_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct mdm_client_nm, mdm_client_id FROM acosta_retail_analytics_im.ds_intervention_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct mdm_client_nm, mdm_client_id FROM acosta_retail_analytics_im.ds_intervention_summary

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im = """
  SELECT
  mdm_country_nm,
  mdm_holding_nm,
  mdm_banner_nm,
  mdm_client_nm,
  mdm_country_id,
  mdm_client_id,
  mdm_holding_id,
  mdm_banner_id,
  COUNT(total_intervention_effect),
  SUM(total_intervention_effect),
  SUM(total_qintervention_effect),
  SUM(total_impact),
  SUM(total_qimpact),
  substr(call_date, 1, 7) AS call_month
  FROM 
  acosta_retail_analytics_im.ds_intervention_summary
  WHERE
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  coalesce(mdm_banner_id, -1) = {banner_id} AND
  (
      call_date like '2020-11%' OR
      call_date like '2020-12%' OR
      call_date like '2021-01%'
  )
  GROUP BY
  mdm_country_nm,
  mdm_holding_nm,
  mdm_banner_nm,
  mdm_client_nm,
  mdm_country_id,
  mdm_client_id,
  mdm_holding_id,
  mdm_banner_id,
  call_month
  ORDER BY
  call_month
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)

# COMMAND ----------

df_acosta_retail_analytics_im = spark.sql(df_sql_query_acosta_retail_analytics_im)
display(df_acosta_retail_analytics_im)

# 2020-11: total_intervention_effect = 202798.15 total_impact = $663054.19
# 2020-12: total_intervention_effect =  97769.01 total_impact = $342320.76
# 2021-01: total_intervention_effect = 118837.61 total_impact = $379548.55

# COMMAND ----------

client_id = 1161 # Atkins Nutritional

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im = """
  SELECT
  mdm_country_nm,
  mdm_holding_nm,
  mdm_banner_nm,
  mdm_client_nm,
  mdm_country_id,
  mdm_client_id,
  mdm_holding_id,
  mdm_banner_id,
  COUNT(total_intervention_effect),
  SUM(total_intervention_effect),
  SUM(total_qintervention_effect),
  SUM(total_impact),
  SUM(total_qimpact),
  substr(call_date, 1, 7) AS call_month
  FROM 
  acosta_retail_analytics_im.ds_intervention_summary
  WHERE
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  coalesce(mdm_banner_id, -1) = {banner_id} AND
  (
      call_date like '2020-11%' OR
      call_date like '2020-12%' OR
      call_date like '2021-01%'
  )
  GROUP BY
  mdm_country_nm,
  mdm_holding_nm,
  mdm_banner_nm,
  mdm_client_nm,
  mdm_country_id,
  mdm_client_id,
  mdm_holding_id,
  mdm_banner_id,
  call_month
  ORDER BY
  call_month
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)

# COMMAND ----------

df_acosta_retail_analytics_im = spark.sql(df_sql_query_acosta_retail_analytics_im)
display(df_acosta_retail_analytics_im)

# 2020-11: total_intervention_effect = 10966.26 total_impact = $77308.05
# 2020-12: total_intervention_effect =  2042.96 total_impact = $13480.39
# 2021-01: total_intervention_effect =  5643.50 total_impact = $40994.71

# COMMAND ----------

country_id = 1  # US
client_id = 460 # Minute Maid
holding_id = 71 # Walmart
banner_id = -1  # default

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC mdm_country_nm,
# MAGIC mdm_holding_nm,
# MAGIC mdm_banner_nm,
# MAGIC mdm_client_nm,
# MAGIC mdm_country_id,
# MAGIC mdm_client_id,
# MAGIC mdm_holding_id,
# MAGIC mdm_banner_id,
# MAGIC COUNT(total_intervention_effect),
# MAGIC SUM(total_intervention_effect),
# MAGIC SUM(total_qintervention_effect),
# MAGIC SUM(total_impact),
# MAGIC SUM(total_qimpact),
# MAGIC substr(call_date, 1, 7) AS call_month
# MAGIC FROM 
# MAGIC acosta_retail_analytics_im.ds_intervention_summary
# MAGIC WHERE
# MAGIC mdm_country_id = 1 AND -- US
# MAGIC mdm_client_id = 460 AND -- Minute Maid
# MAGIC mdm_holding_id = 71 AND -- Walmart
# MAGIC coalesce(mdm_banner_id, -1) = -1 AND -- default
# MAGIC (
# MAGIC     call_date like '2020-11%' OR
# MAGIC     call_date like '2020-12%' OR
# MAGIC     call_date like '2021-01%'
# MAGIC ) AND -- calls happened between 2020-11 and 2021-01
# MAGIC load_ts like '2021-03%' -- loaded in March
# MAGIC GROUP BY
# MAGIC mdm_country_nm,
# MAGIC mdm_holding_nm,
# MAGIC mdm_banner_nm,
# MAGIC mdm_client_nm,
# MAGIC mdm_country_id,
# MAGIC mdm_client_id,
# MAGIC mdm_holding_id,
# MAGIC mdm_banner_id,
# MAGIC call_month
# MAGIC ORDER BY
# MAGIC call_month

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC use nars_raw;
# MAGIC
# MAGIC select * from dpau
