# Databricks notebook source
from pprint import pprint

import pyspark.sql.functions as pyf

from acosta.measurement import required_columns, process_notebook_inputs

import acosta
from acosta.alerting.preprocessing import read_pos_data

print(acosta.__version__)

# COMMAND ----------

# MAGIC %sql
# MAGIC use acosta_retail_analytics_im;
# MAGIC
# MAGIC show tables

# COMMAND ----------

country_id = 1  # US
client_id = 9663 # Barilla America Inc
holding_id = 2301 # Target
banner_id = -1  # default

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM nars_raw.dpau

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM nars_raw.dpau -- Need to look at dpau.shelfcode
# MAGIC where retailer_id = 2301 -- Target

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM vw_ds_intervention_input_dla
# MAGIC where mdm_holding_id = 2301 -- Target

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where intervention_rank is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where mdm_holding_id = 2301 -- Target

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where mdm_holding_id = 2301 -- Target
# MAGIC and epos_retailer_item_id is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct mdm_holding_nm, mdm_holding_id FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct mdm_client_nm, mdm_client_id FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars

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
# MAGIC SELECT * FROM acosta_retail_analytics_im.interventions_retailer_client_config

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.interventions_parameters
# MAGIC where mdm_country_id = 1  -- US
# MAGIC and mdm_client_id = 9663 -- Barilla America Inc
# MAGIC and mdm_holding_id = 2301 -- Target

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.interventions_response_mapping
# MAGIC where mdm_country_id = 1  -- US

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where mdm_holding_id = 2301 -- Target

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im0 = """
  SELECT
  mdm_country_nm,
  mdm_holding_nm,
  mdm_banner_nm,
  mdm_client_nm,
  mdm_country_id,
  mdm_client_id,
  mdm_holding_id,
  mdm_banner_id,
  COUNT(response_id),
  substr(call_date, 1, 7) AS call_month
  FROM 
  acosta_retail_analytics_im.vw_ds_intervention_input_nars
  WHERE
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  coalesce(mdm_banner_id, -1) = {banner_id} AND
  (
      call_date like '2022-04%' OR
      call_date like '2022-05%' OR
      call_date like '2022-06%' OR
      call_date like '2022-07%'
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

df_acosta_retail_analytics_im0 = spark.sql(df_sql_query_acosta_retail_analytics_im0)
display(df_acosta_retail_analytics_im0)

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im1 = """
  SELECT
  mdm_country_nm,
  mdm_holding_nm,
  mdm_banner_nm,
  mdm_client_nm,
  mdm_country_id,
  mdm_client_id,
  mdm_holding_id,
  mdm_banner_id,
  COUNT(response_id),
  call_date
  FROM 
  acosta_retail_analytics_im.vw_ds_intervention_input_nars
  WHERE
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  coalesce(mdm_banner_id, -1) = {banner_id} AND
  (
      call_date like '2022-04%' OR
      call_date like '2022-05%' OR
      call_date like '2022-06%' OR
      call_date like '2022-07%' OR
      call_date like '2022-08%' OR
      call_date like '2022-09%' OR
      call_date like '2022-10%' OR
      call_date like '2022-11%' OR
      call_date like '2022-12%'
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
  call_date
  ORDER BY
  call_date
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)

# COMMAND ----------

df_acosta_retail_analytics_im1 = spark.sql(df_sql_query_acosta_retail_analytics_im1)
display(df_acosta_retail_analytics_im1)

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
      call_date like '2020%' OR
      call_date like '2021%'
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

# COMMAND ----------

country_id = 1  # US
client_id = 9663 # Barilla
holding_id = 71 # Walmart
banner_id = -1  # default

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im7 = """
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
      call_date like '2021-03%' OR
      call_date like '2021-04%' OR
      call_date like '2021-05%' OR
      call_date like '2021-06%'
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

df_acosta_retail_analytics_im7 = spark.sql(df_sql_query_acosta_retail_analytics_im7)
display(df_acosta_retail_analytics_im7)

# WM Barilla
# Results from 2021-07-08

# 2021-04: total_intervention_effect = 793942.43 total_impact = $5924545.02
# 2021-05: total_intervention_effect = 605923.57 total_impact = $4792003.44
# 2021-06: total_intervention_effect = 436031.08 total_impact = $3082559.35

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im8 = """
  SELECT *,
  substr(call_date, 1, 7) AS call_month
  FROM 
  acosta_retail_analytics_im.ds_intervention_summary
  WHERE
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  coalesce(mdm_banner_id, -1) = {banner_id} AND
  total_intervention_effect >= 100 AND
  (
      call_date like '2021-04%' OR
      call_date like '2022-05%'
  )
  ORDER BY
  total_intervention_effect desc
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)

# COMMAND ----------

df_acosta_retail_analytics_im8 = spark.sql(df_sql_query_acosta_retail_analytics_im8)
display(df_acosta_retail_analytics_im8)

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im9 = """
  SELECT *,
  substr(call_date, 1, 7) AS call_month
  FROM 
  acosta_retail_analytics_im.ds_intervention_summary
  WHERE
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  coalesce(mdm_banner_id, -1) = {banner_id} AND
  total_intervention_effect >= 100 AND
  (
      call_date like '2021-05%'
  )
  ORDER BY
  total_intervention_effect desc
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)

# COMMAND ----------

df_acosta_retail_analytics_im9 = spark.sql(df_sql_query_acosta_retail_analytics_im9)
display(df_acosta_retail_analytics_im9)

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC java.time.LocalDate.now

# COMMAND ----------


