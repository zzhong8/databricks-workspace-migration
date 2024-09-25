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

country_id = 1  # US
client_id = 851 # Campbells
holding_id = 91 # Kroger
banner_id = -1  # default

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * --,
# MAGIC -- MONTH(call_date, 1) AS call_month 
# MAGIC FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 91 -- Kroger
# MAGIC and
# MAGIC mdm_client_id = 851 -- Campbells
# MAGIC -- and
# MAGIC -- call_date >= '2021-11-01'
# MAGIC -- and
# MAGIC -- call_date <= '2021-11-30'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT nars_response_text, count(nars_response_text) as number_of_responses 
# MAGIC FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 91 -- Kroger
# MAGIC and
# MAGIC mdm_client_id = 851 -- Campbells
# MAGIC and
# MAGIC call_date >= '2021-11-01'
# MAGIC and
# MAGIC call_date <= '2021-11-30'
# MAGIC and
# MAGIC nars_response_text != 'Resolved on Entry, No Action Taken'
# MAGIC group by
# MAGIC nars_response_text
# MAGIC order by
# MAGIC number_of_responses
# MAGIC desc

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
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 91 -- Kroger
# MAGIC and
# MAGIC mdm_client_id = 851 -- Campbells
# MAGIC -- and
# MAGIC -- call_date >= '2021-11-01'
# MAGIC -- and
# MAGIC -- call_date <= '2021-11-30'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT nars_response_text, count(nars_response_text) as number_of_responses FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 91 -- Kroger
# MAGIC and
# MAGIC mdm_client_id = 851 -- Campbells
# MAGIC and
# MAGIC call_date >= '2021-11-01'
# MAGIC and
# MAGIC call_date <= '2021-11-30'
# MAGIC and 
# MAGIC is_complete = 'true'
# MAGIC group by
# MAGIC nars_response_text
# MAGIC order by
# MAGIC number_of_responses
# MAGIC desc

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im1 = """
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
      call_date >= '2021-01-01'
  ) AND -- calls happened between 2021-01-24 and 2021-12-31
  load_ts like '2021%' -- loaded in 2021
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

df_acosta_retail_analytics_im1 = spark.sql(df_sql_query_acosta_retail_analytics_im1)
display(df_acosta_retail_analytics_im1)

# Kroger Campbells
# Results from 2021-01-31 to 2021-12-31

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im2 = """
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
  MONTH(call_date) AS call_month
  FROM 
  acosta_retail_analytics_im.ds_intervention_summary
  WHERE
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  coalesce(mdm_banner_id, -1) = {banner_id} AND -- default
  (
      call_date >= '2021-01-01'
  ) AND -- calls happened between 2021-01-24 and 2021-12-31
  load_ts like '2021%' -- loaded in 2021
  GROUP BY
  mdm_country_nm,
  mdm_holding_nm,
  mdm_banner_nm,
  mdm_client_nm,
  call_month
  ORDER BY
  call_month
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)

# COMMAND ----------

df_acosta_retail_analytics_im2 = spark.sql(df_sql_query_acosta_retail_analytics_im2)
display(df_acosta_retail_analytics_im2)

# Kroger Campbells
# Results from 2021-01-31 to 2021-12-31

# COMMAND ----------


