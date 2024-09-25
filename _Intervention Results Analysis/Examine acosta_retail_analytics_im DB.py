# Databricks notebook source
from pprint import pprint

import pyspark.sql.functions as pyf

from acosta.measurement import required_columns, process_notebook_inputs

import acosta
from acosta.alerting.preprocessing import read_pos_data

print(acosta.__version__)

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

# Minute Maid
# Results from 2021-03-16, 2021-03-17 and 2021-03-25

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

# Atkins Nutritional
# Results from 2021-03-16, 2021-03-17 and 2021-03-25

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

# Minute Maid
# Results from 2021-03-16, 2021-03-17 and 2021-03-25 with LOAD_TS before 2021-03

# 2020-11: total_intervention_effect = 14789.16 total_impact = $47545.91
# 2020-12: total_intervention_effect =  5264.89 total_impact = $19420.90
# 2021-01: total_intervention_effect = 4127.02 total_impact = $12614.32

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

# Minute Maid
# Results from 2021-03-16, 2021-03-17 and 2021-03-25 with LOAD_TS in 2021-03

# 2020-11: total_intervention_effect = 188008.99 total_impact = $615508.28
# 2020-12: total_intervention_effect =  92504.12 total_impact = $322899.86
# 2021-01: total_intervention_effect = 114710.59 total_impact = $366934.23

# COMMAND ----------

# %sql 

# use nars_raw;

# select * from dpau

# COMMAND ----------

country_id = 1  # US
client_id = 460 # Minute Maid
holding_id = 71 # Walmart
banner_id = -1  # default

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im4 = """
  SELECT
  mdm_country_nm,
  mdm_holding_nm,
  mdm_banner_nm,
  mdm_client_nm,
  mdm_country_id,
  mdm_client_id,
  mdm_holding_id,
  mdm_banner_id,
  COUNT(call_id),
  substr(call_date, 1, 7) AS call_month
  FROM 
  acosta_retail_analytics_im.vw_ds_intervention_input_nars
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

df_acosta_retail_analytics_im4 = spark.sql(df_sql_query_acosta_retail_analytics_im4)
display(df_acosta_retail_analytics_im4)

# Minute Maid
# Results from 2021-03-16, 2021-03-17 and 2021-03-25

# 2020-11: total of interventions = 89261
# 2020-12: total of interventions = 66369
# 2021-01: total of interventions = 79633

# COMMAND ----------

client_id = 1161 # Atkins Nutritional

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im5 = """
  SELECT
  mdm_country_nm,
  mdm_holding_nm,
  mdm_banner_nm,
  mdm_client_nm,
  mdm_country_id,
  mdm_client_id,
  mdm_holding_id,
  mdm_banner_id,
  COUNT(call_id),
  substr(call_date, 1, 7) AS call_month
  FROM 
  acosta_retail_analytics_im.vw_ds_intervention_input_nars
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

df_acosta_retail_analytics_im5 = spark.sql(df_sql_query_acosta_retail_analytics_im5)
display(df_acosta_retail_analytics_im5)

# Atkins Nutritional
# Results from 2021-03-16, 2021-03-17 and 2021-03-25

# 2020-11: total of interventions = 103621
# 2020-12: total of interventions = 71206
# 2021-01: total of interventions = 95607

# COMMAND ----------

client_id = 417 # Georgia Pacific

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im6 = """
  SELECT
  mdm_country_nm,
  mdm_holding_nm,
  mdm_banner_nm,
  mdm_client_nm,
  mdm_country_id,
  mdm_client_id,
  mdm_holding_id,
  mdm_banner_id,
  COUNT(call_id),
  substr(call_date, 1, 7) AS call_month
  FROM 
  acosta_retail_analytics_im.vw_ds_intervention_input_nars
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

df_acosta_retail_analytics_im6 = spark.sql(df_sql_query_acosta_retail_analytics_im6)
display(df_acosta_retail_analytics_im6)

# Georgia Pacific
# Results from 2021-03-16, 2021-03-17 and 2021-03-25

# 2020-11: total of interventions = 114942
# 2020-12: total of interventions = 114451
# 2021-01: total of interventions = 123615

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

df_acosta_retail_analytics_im0 = spark.sql(df_sql_query_acosta_retail_analytics_im0)
display(df_acosta_retail_analytics_im0)

# Georgia Pacific
# Results from 2021-03-16, 2021-03-17 and 2021-03-25

# 2020-11: total_intervention_effect = 235344.09 total_impact = $1798958.36
# 2020-12: total_intervention_effect = 250255.99 total_impact = $1899671.36
# 2021-01: total_intervention_effect = 232389.06 total_impact = $1635133.38

# COMMAND ----------


