# Databricks notebook source
from pprint import pprint

import pyspark.sql.functions as pyf

from acosta.measurement import required_columns, process_notebook_inputs

import acosta
from acosta.alerting.preprocessing import read_pos_data

print(acosta.__version__)

# COMMAND ----------

country_id = 1  # US
client_id = 460 # Minute Maid
holding_id = 71 # Walmart
banner_id = -1  # default

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

# MAGIC %sql
# MAGIC desc acosta_retail_analytics_im.vw_dimension_product

# COMMAND ----------

# MAGIC %sql
# MAGIC select client_id, item_dimension_id, upc, item_description, holding_description from acosta_retail_analytics_im.vw_dimension_itemlevel
# MAGIC where client_id = 778 -- Kens DRT
# MAGIC and holding_description = 'Wal-Mart'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acosta_retail_analytics_im.vw_dimension_product as p
# MAGIC where p.client_id = 778 -- Kens DRT
# MAGIC and p.holding_id = 71 -- Walmart

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select p.item_id, p.item_description, count(p.item_id) from acosta_retail_analytics_im.vw_dimension_product as p
# MAGIC JOIN acosta_retail_analytics_im.fact_questionresponse as qr on p.item_id = qr.item_dimension_id
# MAGIC join acosta_retail_analytics_im.fact_calls AS c on c.call_id = qr.call_id
# MAGIC where qr.item_entity_id = 'PRSK'
# MAGIC and p.client_id = 778 -- Kens DRT
# MAGIC and p.holding_id = 71 -- Walmart
# MAGIC and c.call_type_code like '%DRT%'
# MAGIC group by p.item_id, p.item_description
# MAGIC order by p.item_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retaillink_walmart_kens_us_dv.vw_latest_sat_retailer_item

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retaillink_walmart_kens_us_dv.sat_retailer_item

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bobv2.company

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bobv2.vw_bobv2_product where CompanyId = 614

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bobv2.product where CompanyId = 614

# COMMAND ----------

# MAGIC %sql
# MAGIC use bobv2;
# MAGIC
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC use nars_raw;
# MAGIC
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC desc nars_raw.agac

# COMMAND ----------


