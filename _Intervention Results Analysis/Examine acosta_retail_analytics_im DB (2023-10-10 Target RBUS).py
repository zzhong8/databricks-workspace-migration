# Databricks notebook source
from pprint import pprint

import numpy as np
import pandas as pd

import matplotlib.pyplot as graph
import seaborn as sns

import pyspark.sql.types as pyt
import pyspark.sql.functions as pyf

from acosta.measurement import required_columns, process_notebook_inputs

import acosta
from acosta.alerting.preprocessing import read_pos_data

print(acosta.__version__)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from acosta_retail_analytics_im.ds_intervention_audit_raw
# MAGIC
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 1992 -- RB
# MAGIC and
# MAGIC mdm_holding_id = 2301 -- Target
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show create table acosta_retail_analytics_im.ds_intervention_audit_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.ds_intervention_audit_raw
# MAGIC
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 1992 -- RB
# MAGIC and
# MAGIC mdm_holding_id = 2301 -- Target
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default 
# MAGIC order by
# MAGIC response_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.ds_intervention_audit_summary
# MAGIC
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 1992 -- RB
# MAGIC and
# MAGIC mdm_holding_id = 2301 -- Target
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default 

# COMMAND ----------

country_id = 1  # US
client_id = 1992 # RB
holding_id = 2301 # Target

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.interventions_parameters

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.ds_intervention_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct * from acosta_retail_analytics_im.interventions_retailer_client_config
# MAGIC
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 1992 -- RB
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables in retail_alert_target_rbusa_us_im

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc retail_alert_target_rbusa_us_im.loess_forecast_baseline_unit_upc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from retail_alert_target_rbusa_us_im.loess_forecast_baseline_unit_upc 
# MAGIC where sales_dt >= '2021-10-25'
# MAGIC limit 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from retail_alert_target_rbusa_us_im.loess_forecast_baseline_unit_upc 
# MAGIC where sales_dt >= '2021-10-01'
# MAGIC limit 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 2301 -- Target
# MAGIC and
# MAGIC mdm_client_id = 1992 -- RB
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 2301 -- Target
# MAGIC and
# MAGIC mdm_client_id = 1992 -- RB
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC order by 
# MAGIC standard_response_cd,
# MAGIC standard_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 2301 -- Target
# MAGIC and
# MAGIC mdm_client_id = 1992 -- RB
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct mdm_client_id, standard_response_cd, standard_response_text from acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 2301 -- Target
# MAGIC and
# MAGIC mdm_client_id = 1992 -- RB
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC order by 
# MAGIC standard_response_cd,
# MAGIC standard_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM bigred_target_rbusa_us_dv.vw_latest_sat_epos_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT sales_dt, count(*) FROM bigred_target_rbusa_us_dv.vw_latest_sat_epos_summary
# MAGIC group by sales_dt
# MAGIC order by sales_dt desc

# COMMAND ----------

# MAGIC %sql
# MAGIC desc acosta_retail_analytics_im.ds_intervention_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 2301 -- Target
# MAGIC and
# MAGIC mdm_client_id = 1992 -- RB
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 2301 -- Target
# MAGIC and
# MAGIC mdm_client_id = 1992 -- RB
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC order by
# MAGIC call_date

# COMMAND ----------


