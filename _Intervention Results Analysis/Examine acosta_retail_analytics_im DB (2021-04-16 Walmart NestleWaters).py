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
# MAGIC show tables in acosta_retail_analytics_im

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct * from acosta_retail_analytics_im.interventions_parameters
# MAGIC
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct * from acosta_retail_analytics_im.interventions_parameters
# MAGIC
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 13429 -- NestleWatersN.A
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct * from acosta_retail_analytics_im.interventions_parameters
# MAGIC
# MAGIC where
# MAGIC standard_response_text = 'Warehouse Out'

# COMMAND ----------

# %sql

# use acosta_retail_analytics_im;

# UPDATE interventions_parameters
# SET intervention_group = null, intervention_start_day = null, intervention_end_day = null, actionable_flg = null
# WHERE standard_response_text = 'Warehouse Out'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct * from acosta_retail_analytics_im.interventions_response_mapping
# MAGIC
# MAGIC where
# MAGIC mdm_country_id = 1 -- US

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct * from acosta_retail_analytics_im.interventions_retailer_client_config
# MAGIC
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct * from acosta_retail_analytics_im.interventions_retailer_client_config
# MAGIC
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 13429 -- NestleWatersN.A
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default

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
client_id = 13429 # NestleWatersN.A
holding_id = 71 # Walmart
banner_id = -1  # default

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC
# MAGIC where
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct mdm_holding_nm, mdm_holding_id FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct nars_response_text, standard_response_text FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 13429 -- NestleWatersN.A
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESC acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct nars_response_text, standard_response_text FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco

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
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco

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
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC call_date >= '2021-06-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco

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
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco

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

df_sql_query_acosta_retail_analytics_im = """
  SELECT
  mdm_country_nm,
  mdm_holding_nm,
  mdm_client_nm,
  epos_organization_unit_num,
  call_id,
  response_id,
  total_intervention_effect,
  total_impact,
  WEEKOFYEAR(DATE_ADD(call_date, 1)) AS call_week
  FROM 
  acosta_retail_analytics_im.ds_intervention_summary
  WHERE
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  coalesce(mdm_banner_id, -1) = {banner_id} AND -- default
  (
      call_date >= '2021-02-07' AND
      call_date <= '2021-04-10'
  ) AND -- calls happened between 2021-02-07 and 2021-04-10
  load_ts like '2021-0%' -- loaded in 2021
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)

# COMMAND ----------

df_acosta_retail_analytics_im = spark.sql(df_sql_query_acosta_retail_analytics_im)

# COMMAND ----------

display(df_acosta_retail_analytics_im)

# COMMAND ----------

df_acosta_retail_analytics_im.count() # Count of ALL measured alerts for all stores for Walmart NestleWaters from 2021-02-07 to 2021-03-27 (7 weeks)

# TODO

# 1) filter down to week Sunday 2021-03-14 to Saturday 2021-03-27 (week 14 and week 15)
# 2) filter only test stores
# 3) filter only for alerts that have lsv >= 7
# 4) check counts for week 14 and week 15 and compare to 540 and 622

# COMMAND ----------

stores = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load('/mnt/processed/alerting/fieldTest/experiment-nestlewaters_v2.csv')

# COMMAND ----------

display(stores)

# COMMAND ----------

test_stores = stores.filter((stores.experiment_group == 'test')).select('Store#').withColumnRenamed('Store#', 'epos_organization_unit_num')
control_stores = stores.filter((stores.experiment_group == 'control')).select('Store#').withColumnRenamed('Store#', 'epos_organization_unit_num')

display(test_stores)

# COMMAND ----------

df_acosta_retail_analytics_im_test = df_acosta_retail_analytics_im.join(test_stores, df_acosta_retail_analytics_im.epos_organization_unit_num == test_stores.epos_organization_unit_num, "inner")

df_acosta_retail_analytics_im_test.count()

# COMMAND ----------

df_acosta_retail_analytics_im_control = df_acosta_retail_analytics_im.join(control_stores, df_acosta_retail_analytics_im.epos_organization_unit_num == control_stores.epos_organization_unit_num, "inner")

df_acosta_retail_analytics_im_control.count()

# COMMAND ----------

df_acosta_retail_analytics_im_test.createOrReplaceTempView("test_alerts")

df_sql_query_acosta_retail_analytics_im_test_agg = """
  SELECT
  mdm_holding_nm,
  mdm_client_nm,
  COUNT(total_intervention_effect),
  SUM(total_intervention_effect),
  SUM(total_impact),
  call_week
  FROM 
  test_alerts
  GROUP BY
  mdm_holding_nm,
  mdm_client_nm,
  call_week
  ORDER BY
  call_week
"""

# COMMAND ----------

df_acosta_retail_analytics_im_test_agg = spark.sql(df_sql_query_acosta_retail_analytics_im_test_agg)
display(df_acosta_retail_analytics_im_test_agg)

# COMMAND ----------

df_acosta_retail_analytics_im_control.createOrReplaceTempView("control_alerts")

df_sql_query_acosta_retail_analytics_im_control_agg = """
  SELECT
  mdm_holding_nm,
  mdm_client_nm,
  COUNT(total_intervention_effect),
  SUM(total_intervention_effect),
  SUM(total_impact),
  call_week
  FROM 
  control_alerts
  GROUP BY
  mdm_holding_nm,
  mdm_client_nm,
  call_week
  ORDER BY
  call_week
"""

# COMMAND ----------

df_acosta_retail_analytics_im_control_agg = spark.sql(df_sql_query_acosta_retail_analytics_im_control_agg)
display(df_acosta_retail_analytics_im_control_agg)

# COMMAND ----------

intervention_grp = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load('/mnt/processed/alerting/fieldTest/Intervention_Parameters_Table.csv')

# COMMAND ----------

display(intervention_grp)

# COMMAND ----------


