# Databricks notebook source
from pprint import pprint

import pyspark.sql.functions as pyf

from acosta.measurement import required_columns, process_notebook_inputs

import acosta
from acosta.alerting.preprocessing import read_pos_data

print(acosta.__version__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check the Status of the Intervention Configuration Setup for the Balance of the UK Clients

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct * from acosta_retail_analytics_im.interventions_retailer_client_config
# MAGIC
# MAGIC where
# MAGIC mdm_country_id = 1 -- UK
# MAGIC -- and
# MAGIC -- mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC order by 
# MAGIC mdm_holding_id,
# MAGIC mdm_client_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Kroger Tyson

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use market6_kroger_tyson_us_dv;
# MAGIC show tables 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select substr(sales_dt, 1, 7) AS sales_month, count(distinct(sales_dt)), count(POS_AMT), sum(POS_AMT) from market6_kroger_tyson_us_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt >= '2022-01-01'
# MAGIC group by sales_month
# MAGIC order by sales_month

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use retail_alert_kroger_tyson_us_im;
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from retail_alert_kroger_tyson_us_im.alert_on_shelf_availability

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from retail_alert_kroger_tyson_us_im.vw_latest_alert_on_shelf_availability

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.interventions_response_mapping
# MAGIC where mdm_country_id = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.interventions_parameters
# MAGIC where mdm_country_id = 1 -- US
# MAGIC and mdm_holding_id = 91 -- Kroger
# MAGIC and mdm_client_id = 642 -- Tyson Foods

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct
# MAGIC objective_typ,
# MAGIC standard_response_cd,
# MAGIC standard_response_text 
# MAGIC from acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where mdm_country_id = 1 -- US
# MAGIC and mdm_holding_id = 91 -- Kroger
# MAGIC and mdm_client_id = 642 -- Tyson Foods

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where mdm_country_id = 1 -- US
# MAGIC and mdm_holding_id = 91 -- Kroger
# MAGIC and mdm_client_id = 642 -- Tyson Foods

# COMMAND ----------


