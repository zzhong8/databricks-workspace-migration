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
# MAGIC show tables in retail_alert_boots_perfettivanmelle_uk_im

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables in retail_alert_kroger_barilla_us_im

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables in retail_alert_walmart_danone_ca_im

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables in retail_alert_walmart_danoneusllc_us_im

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables in retail_alert_walmart_harrysinc_us_im

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables in retail_alert_target_rbusa_us_im

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables in retail_alert_tesco_beiersdorf_uk_im

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc retail_alert_tesco_beiersdorf_uk_im.alert_osa_low_expected_sale_grouping

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from retail_alert_tesco_beiersdorf_uk_im.alert_osa_low_expected_sale_grouping limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from retail_alert_tesco_beiersdorf_uk_im.alert_osa_invalid_alert_grouping limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select SALES_DT, count(*) from retail_alert_tesco_beiersdorf_uk_im.alert_osa_invalid_alerts
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select SALES_DT, count(*) from retail_alert_tesco_beiersdorf_uk_im.alert_osa_low_expected_sale
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from retail_alert_tesco_beiersdorf_uk_im.alert_osa_low_expected_sale limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from retail_alert_tesco_beiersdorf_uk_im.alert_osa_low_expected_sale_grouping

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables in team_retail_alert_perfettivanmelle_drt_uk_im

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc team_retail_alert_perfettivanmelle_drt_uk_im.alert_inventory_cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select Retail_Client, SALES_DT, count(*) from team_retail_alert_perfettivanmelle_drt_uk_im.alert_inventory_cleanup
# MAGIC group by Retail_Client, sales_dt
# MAGIC order by Retail_Client, sales_dt desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc team_retail_alert_perfettivanmelle_drt_uk_im.alert_on_shelf_availability

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select Retail_Client, SALES_DT, count(*) from team_retail_alert_perfettivanmelle_drt_uk_im.alert_on_shelf_availability
# MAGIC group by Retail_Client, sales_dt
# MAGIC order by Retail_Client, sales_dt desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc team_retail_alert_perfettivanmelle_drt_uk_im.retail_alerts_inventory_cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc team_retail_alert_perfettivanmelle_drt_uk_im.vw_retail_alerts_inventory_cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select SALES_DT, count(*) from team_retail_alert_perfettivanmelle_drt_uk_im.vw_retail_alerts_inventory_cleanup
# MAGIC where Retail_Client = 'asda_perfettivanmelle_uk'
# MAGIC group by sales_dt
# MAGIC order by sales_dt desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc team_retail_alert_perfettivanmelle_drt_uk_im.vw_retail_alerts_on_shelf_availability

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select SALES_DT, count(*) from team_retail_alert_perfettivanmelle_drt_uk_im.vw_retail_alerts_on_shelf_availability
# MAGIC where Retail_Client = 'asda_perfettivanmelle_uk'
# MAGIC group by sales_dt
# MAGIC order by sales_dt desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from team_retail_alert_perfettivanmelle_drt_uk_im.vw_retailer_alert_inventory_cleanup_sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from team_retail_alert_perfettivanmelle_drt_uk_im.vw_retailer_alert_on_shelf_availability_sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.interventions_retailer_client_config
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.interventions_retailer_client_config_gen2_migration
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.interventions_retailer_client_config_gen2_migration
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM horizon_sainsburys_perfettivanmelle_uk_dv.vw_latest_sat_epos_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT sales_dt, count(*) FROM horizon_sainsburys_perfettivanmelle_uk_dv.vw_latest_sat_epos_summary
# MAGIC group by sales_dt
# MAGIC order by sales_dt desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM rsi_boots_perfettivanmelle_uk_dv.vw_latest_sat_epos_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT sales_dt, count(*) FROM rsi_boots_perfettivanmelle_uk_dv.vw_latest_sat_epos_summary
# MAGIC group by sales_dt
# MAGIC order by sales_dt desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT sales_dt, count(*) FROM msd_morrisons_perfettivanmelle_uk_dv.vw_latest_sat_epos_summary
# MAGIC group by sales_dt
# MAGIC order by sales_dt desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT sales_dt, count(*) FROM msd_morrisons_perfettivanmelle_uk_dv.vw_latest_sat_epos_summary
# MAGIC group by sales_dt
# MAGIC order by sales_dt desc

# COMMAND ----------


