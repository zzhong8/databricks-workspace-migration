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
client_id = 882 # DanoneU.S.LLC
holding_id = 91 # Kroger
banner_id = -1  # default

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) FROM retail_alert_kroger_danonewave_us_im.alert_inventory_cleanup
# MAGIC where SALES_DT >= '2022-10-01'
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) FROM retail_alert_kroger_danonewave_us_im.alert_inventory_cleanup
# MAGIC where SALES_DT >= '2022-10-01'
# MAGIC and ON_HAND_INVENTORY_QTY is not null
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) FROM retail_alert_kroger_danonewave_us_im.alert_inventory_cleanup
# MAGIC where SALES_DT >= '2022-10-01'
# MAGIC and ON_HAND_INVENTORY_QTY is null
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) FROM retail_alert_kroger_danonewave_us_im.vw_latest_alert_on_shelf_availability
# MAGIC where SALES_DT >= '2022-10-01'
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) FROM retail_alert_kroger_danonewave_us_im.alert_on_shelf_availability
# MAGIC where SALES_DT >= '2022-10-01'
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) FROM retail_alert_kroger_danonewave_us_im.alert_on_shelf_availability
# MAGIC where SALES_DT >= '2022-10-01'
# MAGIC and ON_HAND_INVENTORY_QTY is not null
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) FROM retail_alert_kroger_danonewave_us_im.alert_on_shelf_availability
# MAGIC where SALES_DT >= '2022-10-01'
# MAGIC and ON_HAND_INVENTORY_QTY is null
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM retail_alert_kroger_danonewave_us_im.alert_on_shelf_availability
# MAGIC where SALES_DT == '2022-02-14'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM retail_alert_kroger_danonewave_us_im.alert_on_shelf_availability
# MAGIC where SALES_DT == '2023-02-14'

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
# MAGIC SELECT distinct call_date FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where mdm_client_id = 882 and mdm_holding_id = 91
# MAGIC order by call_date desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where mdm_client_id = 882 and mdm_holding_id = 91
# MAGIC order by call_date desc

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
  call_date,
  COUNT(total_intervention_effect),
  SUM(total_intervention_effect),
  --SUM(total_qintervention_effect),
  SUM(total_impact)
  --SUM(total_qimpact)
  FROM 
  acosta_retail_analytics_im.ds_intervention_summary
  WHERE
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  coalesce(mdm_banner_id, -1) = {banner_id} AND -- default
  (
      call_date >= '2022-10-01' AND
      call_date <= '2023-12-31'
  ) -- calls happened between 2021-01-24 and 2021-12-31
  GROUP BY
  mdm_country_nm,
  mdm_holding_nm,
  mdm_banner_nm,
  mdm_client_nm,
  call_date
  ORDER BY
  call_date
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)

# COMMAND ----------

df_acosta_retail_analytics_im = spark.sql(df_sql_query_acosta_retail_analytics_im)
display(df_acosta_retail_analytics_im)

# Danone
# Results from 2021-01-31 to 2021-12-31

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im2 = """
SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
where
mdm_country_id = 1 -- US
and
mdm_client_id = 882 -- Danone
and
mdm_holding_id = 91 -- Kroger
and
coalesce(mdm_banner_id, -1) = -1 -- default
and
objective_typ <> 'DLA'
and
epos_retailer_item_id is not NULL
and
actionable_flg is not NULL
and
call_date >= '2022-02-01'
group by call_date
order by call_date
"""

# COMMAND ----------

df_acosta_retail_analytics_im2 = spark.sql(df_sql_query_acosta_retail_analytics_im2)
display(df_acosta_retail_analytics_im2)

# Danone
# Results from 2021-01-31 to 2021-12-31

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im3 = """
SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
where
mdm_country_id = 1 -- US
and
mdm_client_id = 882 -- Danone
and
mdm_holding_id = 91 -- Kroger
and
coalesce(mdm_banner_id, -1) = -1 -- default
and
objective_typ = 'DLA'
and
epos_retailer_item_id is not NULL
and
actionable_flg is not NULL
and
call_date >= '2022-02-01'
group by call_date
order by call_date
"""

# COMMAND ----------

df_acosta_retail_analytics_im3 = spark.sql(df_sql_query_acosta_retail_analytics_im3)
display(df_acosta_retail_analytics_im3)

# Danone
# Results from 2021-01-31 to 2021-12-31

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im4 = """
SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
where
mdm_country_id = 1 -- US
and
mdm_client_id = 882 -- Danone
and
mdm_holding_id = 91 -- Kroger
and
coalesce(mdm_banner_id, -1) = -1 -- default
and
call_date >= '2022-10-01'
group by call_date
order by call_date
"""

# COMMAND ----------

df_acosta_retail_analytics_im4 = spark.sql(df_sql_query_acosta_retail_analytics_im4)
display(df_acosta_retail_analytics_im4)

# Danone
# Results from 2022-10-01 to present

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im5 = """
SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
where
mdm_country_id = 1 -- US
and
mdm_client_id = 882 -- Danone
and
mdm_holding_id <> 71 -- not Walmart
and
coalesce(mdm_banner_id, -1) = -1 -- default
and
call_date >= '2022-10-01'
group by call_date
order by call_date
"""

# COMMAND ----------

df_acosta_retail_analytics_im5 = spark.sql(df_sql_query_acosta_retail_analytics_im4)
display(df_acosta_retail_analytics_im5)

# Danone
# Results from 2022-10-01 to present
