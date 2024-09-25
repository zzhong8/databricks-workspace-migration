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
# MAGIC select * from bobv2.vw_bobv2_caps
# MAGIC order by CompanyId,	Company,	ProductBrandId,	ProductBrand,	Lkp_productGroupId,	ProductGroupName,	ParentId,	ManufacturerId,	Manufacturer,	CapType
# MAGIC

# COMMAND ----------

country_id = 1  # US
client_id = 1992 # RB
holding_id = 71 # Walmart

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

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct * from acosta_retail_analytics_im.interventions_retailer_client_config_gen2_migration

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct * from acosta_retail_analytics_im.interventions_retailer_client_config
# MAGIC
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaUK

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc retail_alert_asda_nestlecore_uk_im.drfe_forecast_baseline_unit

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from retail_alert_asda_nestlecore_uk_im.drfe_forecast_baseline_unit limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc retail_alert_asda_nestlecore_uk_im.loess_forecast_baseline_unit

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from retail_alert_asda_nestlecore_uk_im.loess_forecast_baseline_unit limit 10

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
# MAGIC show tables in retail_alert_walmart_rbusa_us_im

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables in retail_alert_asda_droetker_uk_im

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 17685 -- Dr Oetker
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc retail_alert_walmart_rbusa_us_im.loess_forecast_baseline_unit_upc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from retail_alert_asda_droetker_uk_im.loess_forecast_baseline_unit

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from retail_alert_asda_droetker_uk_im.loess_forecast_baseline_unit_upc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from retail_alert_walmart_rbusa_us_im.loess_forecast_baseline_unit_upc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct * from acosta_retail_analytics_im.interventions_retailer_client_config
# MAGIC
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC mdm_client_id = 1992 -- RB
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct standard_response_cd, standard_response_text from acosta_retail_analytics_im.interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
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
# MAGIC select distinct mdm_client_id, standard_response_cd, standard_response_text from acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id <> 1992 -- Not RB
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC objective_typ in ("DLA", 'Data Led Alerting')
# MAGIC order by 
# MAGIC standard_response_cd,
# MAGIC standard_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct mdm_client_id, standard_response_cd, standard_response_text from acosta_retail_analytics_im.interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id <> 1992 -- Not RB
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC objective_typ in ("DLA", 'Data Led Alerting')
# MAGIC order by 
# MAGIC standard_response_cd,
# MAGIC standard_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.interventions_response_mapping
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC order by 
# MAGIC standard_response_cd,
# MAGIC standard_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
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
# MAGIC mdm_holding_id <> 71 -- Other retailers besides Walmart
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
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC mdm_client_id = 1992 -- RB

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct mdm_client_id, standard_response_cd, standard_response_text from acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
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
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id <> 71 -- Other retailers besides Walmart
# MAGIC and
# MAGIC mdm_client_id = 1992 -- RB

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC mdm_client_id = 1992 -- RB

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM retaillink_walmart_rbusa_us_dv.vw_latest_sat_epos_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from retail_alert_walmart_rbusa_us_im.alert_on_shelf_availability
# MAGIC order by sales_dt desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select sales_dt, count(*) from retail_alert_walmart_rbusa_us_im.alert_on_shelf_availability
# MAGIC group by sales_dt
# MAGIC order by sales_dt desc

# COMMAND ----------

# MAGIC %sql
# MAGIC desc acosta_retail_analytics_im.ds_intervention_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select sales_dt, count(*) from bigred_target_wildcat_us_dv.vw_latest_sat_epos_summary
# MAGIC group by sales_dt
# MAGIC order by sales_dt desc

# COMMAND ----------


