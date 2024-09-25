# Databricks notebook source
from pprint import pprint

import pyspark.sql.functions as pyf

from acosta.measurement import required_columns, process_notebook_inputs

import acosta
from acosta.alerting.preprocessing import read_pos_data

print(acosta.__version__)

# COMMAND ----------

country_id = 1  # US
client_id = 16540 # Harrys Inc (aka Wildcat)
holding_id = 2301 # Target
banner_id = -1  # default

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables in acosta_retail_analytics_im

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct * from acosta_retail_analytics_im.interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc (aka Wildcat)
# MAGIC and
# MAGIC mdm_holding_id = 2301 -- Target
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC order by
# MAGIC objective_typ,
# MAGIC lower(standard_response_text)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct * from acosta_retail_analytics_im.interventions_response_mapping
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC order by
# MAGIC objective_typ,
# MAGIC lower(standard_response_text)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct * from acosta_retail_analytics_im.interventions_retailer_client_config
# MAGIC
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 2301 -- Target
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC order by 
# MAGIC mdm_client_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC WEEKOFYEAR('2022-12-03'),
# MAGIC WEEKOFYEAR('2022-12-04'),
# MAGIC WEEKOFYEAR(DATE_ADD(CAST('2022-12-04' AS DATE), 1)),
# MAGIC WEEKOFYEAR('2022-12-05')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc (aka Wildcat)
# MAGIC and
# MAGIC mdm_holding_id = 2301 -- Target
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT WEEKOFYEAR(DATE_ADD(call_date, 1)) AS call_week, count(response_id) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc (aka Wildcat)
# MAGIC and
# MAGIC mdm_holding_id = 2301 -- Target
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC group by 
# MAGIC call_week
# MAGIC ORDER BY
# MAGIC call_week

# COMMAND ----------

ds_intervention_summary%sql

SELECT count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
where
mdm_country_id = 1 -- US
and
mdm_client_id = 16540 -- Harrys Inc (aka Wildcat)
and
mdm_holding_id = 2301 -- Target
and
coalesce(mdm_banner_id, -1) = -1 -- default

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT WEEKOFYEAR(DATE_ADD(call_date, 1)) AS call_week, count(response_id) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc (aka Wildcat)
# MAGIC and
# MAGIC mdm_holding_id = 2301 -- Target
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC group by 
# MAGIC call_week
# MAGIC ORDER BY
# MAGIC call_week

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc (aka Wildcat)
# MAGIC and
# MAGIC mdm_holding_id = 2301 -- Target
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct mdm_holding_id, mdm_holding_nm FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC order by mdm_holding_id, mdm_holding_nm

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct call_date FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc (aka Wildcat)
# MAGIC and
# MAGIC mdm_holding_id = 2301 -- Target
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC order by call_date

# COMMAND ----------

# Get the list of completely measured responses by week
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
      call_date >= '2022-06-02' AND
      call_date <= '2022-12-31'
  ) AND -- calls happened between 2022-06-02 and 2022-12-31
  load_ts like '2022-%' -- loaded in 2022
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

# Danone
# Results from 2022-06-02 and 2022-12-31

# COMMAND ----------


