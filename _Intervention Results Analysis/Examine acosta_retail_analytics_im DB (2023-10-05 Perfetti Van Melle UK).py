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
# MAGIC desc acosta_retail_analytics_im.interventions_parameters

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7745 -- Sainsburys
# MAGIC order by standard_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7996 -- Boots
# MAGIC order by standard_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show create table acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7996 -- Boots
# MAGIC and
# MAGIC call_date >= '2021-10-01'
# MAGIC limit 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7745 -- Sainsburys
# MAGIC and
# MAGIC call_date >= '2021-10-01'
# MAGIC limit 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7744 -- Morrisons
# MAGIC and
# MAGIC epos_retailer_item_id IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7745 -- Sainsburys
# MAGIC and
# MAGIC epos_retailer_item_id IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT objective_typ FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7745 -- Sainsburys

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7745 -- Sainsburys
# MAGIC GROUP BY
# MAGIC call_date
# MAGIC ORDER BY
# MAGIC call_date
# MAGIC DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from retail_alert_sainsburys_perfettivanmelle_uk_im.loess_forecast_baseline_unit_upc 
# MAGIC where sales_dt >= '2021-10-01'
# MAGIC limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select sales_dt, count(*) from retail_alert_sainsburys_perfettivanmelle_uk_im.loess_forecast_baseline_unit_upc 
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from retail_alert_boots_perfettivanmelle_uk_im.loess_forecast_baseline_unit_upc 
# MAGIC where sales_dt >= '2023-09-01'
# MAGIC order by SALES_DT desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select sales_dt, count(*) from retail_alert_boots_beiersdorf_uk_im.loess_forecast_baseline_unit_upc 
# MAGIC -- group by SALES_DT
# MAGIC -- order by SALES_DT
# MAGIC
# MAGIC select sales_dt, count(*) from retail_alert_boots_perfettivanmelle_uk_im.loess_forecast_baseline_unit_upc 
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT desc

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
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC call_date >= '2021-10-01'
# MAGIC limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(*) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7744 -- Morrisons
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7745 -- Sainsburys

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(*) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7745 -- Sainsburys
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(*) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7744 -- Morrisons
# MAGIC group by call_date
# MAGIC order by call_date desc

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

# MAGIC %md
# MAGIC #### Before 2023-10-05 IVM Runs for Perfetti

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC call_date >= '2023-09-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(*) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC call_date >= '2023-09-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7744 -- Morrisons

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7744 -- Morrisons
# MAGIC and
# MAGIC call_date >= '2023-09-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(*) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7744 -- Morrisons
# MAGIC and
# MAGIC call_date >= '2023-09-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7746 -- Tesco

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7746 -- Tesco
# MAGIC and
# MAGIC call_date >= '2023-09-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(*) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7746 -- Tesco
# MAGIC and
# MAGIC call_date >= '2023-09-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %md
# MAGIC #### After 2023-10-05 IVM Runs for Perfetti

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC call_date >= '2023-09-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(*) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC call_date >= '2023-09-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7744 -- Morrisons

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7744 -- Morrisons
# MAGIC and
# MAGIC call_date >= '2023-09-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(*) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7744 -- Morrisons
# MAGIC and
# MAGIC call_date >= '2023-09-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7746 -- Tesco

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7746 -- Tesco
# MAGIC and
# MAGIC call_date >= '2023-09-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(*) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7746 -- Tesco
# MAGIC and
# MAGIC call_date >= '2023-09-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713 -- Perfetti Van Melle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7746 -- Tesco
# MAGIC and
# MAGIC call_date >= '2023-09-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM team_retail_alert_droetker_drt_uk_im.alert_on_shelf_availability
# MAGIC   WHERE Retail_Client = 'morrisons_droetker_uk'
# MAGIC   AND sales_dt >= '2024-01-01'
# MAGIC   AND sales_dt <= '2024-12-31'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM team_retail_alert_perfettivanmelle_drt_uk_im.alert_on_shelf_availability
# MAGIC   WHERE Retail_Client = 'morrisons_perfettivanmelle_uk'
# MAGIC   AND sales_dt >= '2024-01-01'
# MAGIC   AND sales_dt <= '2024-12-31'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM team_retail_alert_target_syn_us_im.alert_on_shelf_availability
# MAGIC   WHERE Retail_Client = 'target_barilla_us'
# MAGIC   AND sales_dt >= '2024-01-01'
# MAGIC   AND sales_dt <= '2024-12-31'
