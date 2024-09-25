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
# MAGIC DESC acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC WEEKOFYEAR('2021-04-03'),
# MAGIC WEEKOFYEAR('2021-04-04'),
# MAGIC WEEKOFYEAR(DATE_ADD(CAST('2021-04-04' AS DATE), 1)),
# MAGIC WEEKOFYEAR('2021-04-05')

# COMMAND ----------

country_id = 30  # UK
client_id = 16320 # Nestle UK
holding_id = 3257 # AcostaRetailUK

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct
# MAGIC standard_response_text,
# MAGIC nars_response_text
# MAGIC FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7746 -- Tesco

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
# MAGIC SELECT distinct 
# MAGIC mdm_country_id, 
# MAGIC mdm_country_nm, 
# MAGIC mdm_holding_nm, 
# MAGIC mdm_holding_id,
# MAGIC mdm_banner_nm, 
# MAGIC mdm_banner_id 
# MAGIC FROM 
# MAGIC acosta_retail_analytics_im.ds_intervention_summary
# MAGIC ORDER BY
# MAGIC mdm_country_id,
# MAGIC mdm_holding_id,
# MAGIC mdm_banner_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct mdm_client_nm, mdm_client_id FROM acosta_retail_analytics_im.ds_intervention_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC
# MAGIC WHERE
# MAGIC mdm_country_id = 30 AND
# MAGIC mdm_client_id = 16320 AND
# MAGIC mdm_holding_id = 3257

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct objective_typ, standard_response_text FROM acosta_retail_analytics_im.ds_intervention_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct objective_typ, standard_response_text FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC
# MAGIC WHERE
# MAGIC mdm_country_id = 30 AND
# MAGIC mdm_client_id = 16320 AND
# MAGIC mdm_holding_id = 3257

# COMMAND ----------

banner_id = 7746  # Tesco

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im = """
  SELECT
  mdm_country_nm,
  mdm_holding_nm,
  --mdm_banner_nm,
  mdm_client_nm,
  --mdm_country_id,
  --mdm_client_id,
  --mdm_holding_id,
  --mdm_banner_id,
  COUNT(total_intervention_effect),
  SUM(total_intervention_effect),
  --SUM(total_qintervention_effect),
  SUM(total_impact),
  --SUM(total_qimpact),
  WEEKOFYEAR(DATE_ADD(call_date, 1)) AS call_week
  FROM 
  acosta_retail_analytics_im.ds_intervention_summary
  WHERE
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  coalesce(mdm_banner_id, -1) = {banner_id} AND -- default
  (
      call_date >= '2021-01-01' AND
      call_date <= '2021-06-01'
  ) AND -- calls happened between 2021-01-01 and 2021-06-01
  load_ts like '2021-0%' -- loaded in 2021
  GROUP BY
  mdm_country_nm,
  mdm_holding_nm,
  mdm_banner_nm,
  mdm_client_nm,
  call_week
  ORDER BY
  call_week
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)

# COMMAND ----------

df_acosta_retail_analytics_im = spark.sql(df_sql_query_acosta_retail_analytics_im)
display(df_acosta_retail_analytics_im)

# Tesco NestleCore
# Results from 2021-01-01 to 2021-06-01

# COMMAND ----------


