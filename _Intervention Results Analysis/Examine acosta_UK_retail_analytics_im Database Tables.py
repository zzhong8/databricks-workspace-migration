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
# MAGIC use acosta_UK_retail_analytics_im;
# MAGIC
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW COLUMNS IN vw_fact_question_response

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW COLUMNS IN nestle_temp_fix_vw_fact_question_response

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW COLUMNS IN vw_dimension_calendar

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW COLUMNS IN vw_dimension_call_complete_status

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW COLUMNS IN vw_dimension_call_status

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW COLUMNS IN vw_dimension_call_type

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW COLUMNS IN vw_dimension_client

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct * FROM vw_dimension_client

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW COLUMNS IN vw_dimension_itemlevel

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW COLUMNS IN vw_dimension_store

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct banner_id, banner_description, holding_id, holding_description from vw_dimension_store

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct subbanner_id, subbanner_description from vw_dimension_store
# MAGIC where banner_id = 7746
# MAGIC and holding_id = 3257
# MAGIC and channel_id = 1
# MAGIC and country_id = 30

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct division_id, division_description from vw_dimension_store
# MAGIC where banner_id = 7746
# MAGIC and holding_id = 3257
# MAGIC and channel_id = 1
# MAGIC and country_id = 30

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct market_id, market_description from vw_dimension_store
# MAGIC where banner_id = 7746
# MAGIC and holding_id = 3257
# MAGIC and channel_id = 1
# MAGIC and country_id = 30

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct region_id, subregion_id from vw_dimension_store
# MAGIC where banner_id = 7746
# MAGIC and holding_id = 3257
# MAGIC and channel_id = 1
# MAGIC and country_id = 30

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from vw_dimension_store
# MAGIC where banner_id = 7746 -- Tesco
# MAGIC and holding_id = 3257 -- Acosta Retail UK
# MAGIC and channel_id = 1
# MAGIC and country_id = 30 -- UK

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW COLUMNS IN vw_dimension_itemlevel

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from vw_dimension_itemlevel
# MAGIC where client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct
# MAGIC category_description,
# MAGIC department_description
# MAGIC from vw_dimension_itemlevel
# MAGIC where client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct
# MAGIC sub_subcategory_description,
# MAGIC subcategory_description,
# MAGIC category_description,
# MAGIC department_description
# MAGIC from vw_dimension_itemlevel
# MAGIC where client_id = 16320 -- Nestle UK (16319 is KraftHeinz UK)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_UK_retail_analytics_im;
# MAGIC
# MAGIC select distinct retail_team_id from vw_fact_question_response a join
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
# MAGIC SHOW COLUMNS IN vw_dimension_employee;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from vw_dimension_employee;

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select min(response_last_modified_date), max(response_last_modified_date), min(call_completed_date), max(call_completed_date) from nestle_temp_fix_vw_fact_question_response;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW COLUMNS IN vw_fact_question_response;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from vw_fact_question_response
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select min(response_last_modified_date), max(response_last_modified_date), min(call_completed_date), max(call_completed_date) from vw_fact_question_response;

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW COLUMNS IN vw_dimension_call_complete_status;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from vw_dimension_call_complete_status;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW COLUMNS IN vw_dimension_call_status

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from vw_dimension_call_status;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW COLUMNS IN vw_dimension_call_type

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from vw_dimension_call_type

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW COLUMNS IN vw_dimension_client

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from vw_dimension_client

# COMMAND ----------

# %sql

# SHOW COLUMNS IN vw_dimension_employee

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW COLUMNS IN vw_dimension_itemlevel

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW COLUMNS IN vw_dimension_question

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW COLUMNS IN vw_dimension_retail_team

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW COLUMNS IN vw_dimension_store

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW COLUMNS IN vw_dimension_team

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from vw_dimension_team

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW COLUMNS IN vw_fact_calls

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW COLUMNS IN vw_fact_question_response

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW COLUMNS IN vw_fqr_max_last_modified_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from vw_fqr_max_last_modified_date

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

df_sql_query_acosta_retail_analytics_im2 = """
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
  ) AND
  (
      load_ts like '2020-11%' OR
      load_ts like '2020-12%' OR
      load_ts like '2021-01%' OR
      load_ts like '2021-02%'
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

df_acosta_retail_analytics_im2 = spark.sql(df_sql_query_acosta_retail_analytics_im2)
display(df_acosta_retail_analytics_im2)

# 2020-11: total_intervention_effect = 14789.16 total_impact = $47545.91
# 2020-12: total_intervention_effect =  5264.89 total_impact = $19420.90
# 2021-01: total_intervention_effect = 3882.73 total_impact = $12066.03

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im3 = """
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
  ) AND
  load_ts like '2021-03%'
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

df_acosta_retail_analytics_im3 = spark.sql(df_sql_query_acosta_retail_analytics_im3)
display(df_acosta_retail_analytics_im3)

# 2020-11: total_intervention_effect = 188008.99 total_impact = $615508.28
# 2020-12: total_intervention_effect =  92504.12 total_impact = $322899.86
# 2021-01: total_intervention_effect = 114710.59 total_impact = $366934.23

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC use nars_raw;
# MAGIC
# MAGIC select * from dpau
