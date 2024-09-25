# Databricks notebook source
from pprint import pprint

import pyspark.sql.functions as pyf

from acosta.measurement import required_columns, process_notebook_inputs

import acosta
from acosta.alerting.preprocessing import read_pos_data

print(acosta.__version__)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct sales_dt) from msd_morrisons_beiersdorf_uk_dv.vw_latest_sat_epos_summary_ivm_temp; --181

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct sales_dt) from msd_morrisons_brownforman_uk_dv.vw_latest_sat_epos_summary_ivm_temp; --182

# COMMAND ----------

# MAGIC %sql
# MAGIC select sales_dt, count(*) from msd_morrisons_beiersdorf_uk_dv.vw_latest_sat_epos_summary_ivm_temp
# MAGIC group by sales_dt
# MAGIC order by sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC select sales_dt, count(*) from msd_morrisons_brownforman_uk_dv.vw_latest_sat_epos_summary_ivm_temp
# MAGIC group by sales_dt
# MAGIC order by sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from msd_morrisons_beiersdorf_uk_dv.vw_latest_sat_epos_summary_ivm_temp
# MAGIC where sales_dt = '2024-05-22'
# MAGIC limit 20

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from msd_morrisons_beiersdorf_uk_dv.vw_latest_sat_epos_summary_ivm_temp
# MAGIC where sales_dt >= '2023-11-30'
# MAGIC and sales_dt <= '2025-05-28'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from msd_morrisons_beiersdorf_uk_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt >= '2023-11-30'
# MAGIC and sales_dt <= '2025-05-28'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from msd_morrisons_brownforman_uk_dv.vw_latest_sat_epos_summary_ivm_temp
# MAGIC where sales_dt = '2024-05-22'
# MAGIC limit 20

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from msd_morrisons_brownforman_uk_dv.vw_latest_sat_epos_summary_ivm_temp
# MAGIC where sales_dt >= '2023-11-30'
# MAGIC and sales_dt <= '2025-05-29'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from msd_morrisons_brownforman_uk_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt >= '2023-11-30'
# MAGIC and sales_dt <= '2025-05-29'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acosta_retail_analytics_im.interventions_retailer_client_config_gen2_migration
# MAGIC where mdm_client_id = 17742 -- Brown Forman

# COMMAND ----------

# MAGIC %sql
# MAGIC select call_date, count(*) from acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where mdm_client_id = 17742 -- Brown Forman
# MAGIC and mdm_country_id = 30
# MAGIC and mdm_holding_id = 3257
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC select call_date, count(*) from acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where mdm_client_id = 17742 -- Brown Forman
# MAGIC and mdm_country_id = 30
# MAGIC and mdm_holding_id = 3257
# MAGIC and coalesce(mdm_banner_id, -1) = 7744 -- Morrisons
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acosta_retail_analytics_im.vw_ds_intervention_input_nars where mdm_client_id = 17742
# MAGIC and
# MAGIC         mdm_country_id = 30
# MAGIC         and mdm_client_id = 17742
# MAGIC         and mdm_holding_id = 3257
# MAGIC         and coalesce(mdm_banner_id, -1) = 7744
# MAGIC         -- and call_date >= '2024-01-01'
# MAGIC         -- and call_date <= '2024-05-31'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acosta_retail_analytics_im.vw_ds_intervention_input_nars where mdm_client_id = 14035

# COMMAND ----------

country_id = 30  # UK
client_id = 17781 # BAT
# holding_id = 71 # Walmart
banner_id = -1  # default

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * from acosta_retail_analytics_im.interventions_retailer_client_config
# MAGIC where mdm_country_id = 30
# MAGIC and mdm_client_id = 17781

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * from acosta_retail_analytics_im.interventions_retailer_client_config_gen2_migration
# MAGIC where mdm_country_id = 30
# MAGIC and mdm_client_id = 17781

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) from retaillink_asda_bat_uk_dv.vw_latest_sat_epos_summary
# MAGIC where SALES_DT >= '2001-01-01'
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, RETAIL_CLIENT, count(*) as Final_Num_OSA_Alerts FROM team_retail_alert_beiersdorf_drt_uk_im.alert_on_shelf_availability
# MAGIC where
# MAGIC SALES_DT >= '2024-01-01'
# MAGIC group by retail_client, SALES_DT
# MAGIC order by retail_client, SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, RETAIL_CLIENT, count(*) as Final_Num_Inv_Cleanup_Alerts FROM team_retail_alert_beiersdorf_drt_uk_im.alert_inventory_cleanup
# MAGIC where
# MAGIC SALES_DT >= '2024-01-01'
# MAGIC group by retail_client, SALES_DT
# MAGIC order by retail_client, SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, RETAIL_CLIENT, count(*) as Final_Num_OSA_Alerts FROM team_retail_alert_bat_drt_uk_im.alert_on_shelf_availability
# MAGIC where
# MAGIC SALES_DT >= '2024-01-01'
# MAGIC group by retail_client, SALES_DT
# MAGIC order by retail_client, SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, RETAIL_CLIENT, count(*) as Final_Num_Inv_Cleanup_Alerts FROM team_retail_alert_bat_drt_uk_im.alert_inventory_cleanup
# MAGIC where
# MAGIC SALES_DT >= '2024-01-01'
# MAGIC group by retail_client, SALES_DT
# MAGIC order by retail_client, SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, RETAIL_CLIENT, count(*) as Final_Num_OSA_Alerts FROM team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
# MAGIC where
# MAGIC SALES_DT >= '2024-01-01'
# MAGIC group by retail_client, SALES_DT
# MAGIC order by retail_client, SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, RETAIL_CLIENT, count(*) as Final_Num_Inv_Cleanup_Alerts FROM team_retail_alert_nestlecore_drt_uk_im.alert_inventory_cleanup
# MAGIC where
# MAGIC SALES_DT >= '2024-01-01'
# MAGIC group by retail_client, SALES_DT
# MAGIC order by retail_client, SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) FROM retail_alert_asda_beiersdorf_uk_im.alert_on_shelf_availability
# MAGIC where SALES_DT >= '2023-10-01'
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) FROM retail_alert_morrisons_beiersdorf_uk_im.alert_on_shelf_availability
# MAGIC where SALES_DT >= '2023-10-01'
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) FROM retail_alert_tesco_beiersdorf_uk_im.alert_on_shelf_availability
# MAGIC where SALES_DT >= '2023-10-01'
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) FROM retail_alert_asda_bat_uk_im.alert_on_shelf_availability
# MAGIC where SALES_DT >= '2023-10-01'
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) FROM retail_alert_morrisons_bat_uk_im.alert_on_shelf_availability
# MAGIC where SALES_DT >= '2023-10-01'
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) FROM retail_alert_tesco_bat_uk_im.alert_on_shelf_availability
# MAGIC where SALES_DT >= '2023-10-01'
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show create table retail_alert_asda_bat_uk_im.loess_forecast_baseline_unit_upc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc retail_alert_asda_bat_uk_im.loess_forecast_baseline_unit_upc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from retail_alert_asda_bat_uk_im.loess_forecast_baseline_unit_upc
# MAGIC where SALES_DT >= '2024-01-01'
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select sales_dt, count(*) from retail_alert_asda_bat_uk_im.loess_forecast_baseline_unit_upc
# MAGIC where SALES_DT >= '2023-10-01'
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select sales_dt, count(*) from retail_alert_morrisons_bat_uk_im.loess_forecast_baseline_unit_upc
# MAGIC where SALES_DT >= '2023-10-01'
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select sales_dt, count(*) from retail_alert_tesco_bat_uk_im.loess_forecast_baseline_unit_upc
# MAGIC where SALES_DT >= '2023-10-01'
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_retail_analytics_im;
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- Acosta UK
# MAGIC AND
# MAGIC standard_response_text in ('1/2 Fixture Bay', '1/2 Fixture Shelf', '1/2 In-Aisle Promo Bay', '1/2 In-Aisle Promo Bay Shelf', '1/4 Fixture Shelf', '1/4 In-Aisle Promo Bay Shelf', 'Fixture Full Bay', 'In-Aisle Promo Bay Shelf')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT objective_typ, actionable_flg, nars_response_text, standard_response_text, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- Acosta UK
# MAGIC and
# MAGIC mdm_banner_id = 7746 -- Tesco
# MAGIC -- and 
# MAGIC -- objective_typ = 'Data Led Alerts'
# MAGIC and 
# MAGIC call_date >= "2024-01-01"
# MAGIC group by objective_typ, actionable_flg, nars_response_text, standard_response_text
# MAGIC order by objective_typ, actionable_flg, nars_response_text, standard_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT objective_typ, actionable_flg, nars_response_text, standard_response_text, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- Acosta UK
# MAGIC and
# MAGIC mdm_banner_id = 7746 -- Tesco
# MAGIC -- and 
# MAGIC -- objective_typ = 'Data Led Alerts'
# MAGIC and 
# MAGIC nars_response_text not like '%,%'
# MAGIC and 
# MAGIC call_date >= "2024-01-01"
# MAGIC group by objective_typ, actionable_flg, nars_response_text, standard_response_text
# MAGIC order by objective_typ, actionable_flg, nars_response_text, standard_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT objective_typ, min(call_date), max(call_date) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- Acosta UK
# MAGIC and
# MAGIC mdm_banner_id = 7746 -- Tesco
# MAGIC -- and 
# MAGIC -- objective_typ = 'Data Led Alerts'
# MAGIC and 
# MAGIC call_date >= "2023-01-01"
# MAGIC group by objective_typ

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- Acosta UK
# MAGIC AND
# MAGIC standard_response_text in ('1/2 Fixture Bay', '1/2 Fixture Shelf', '1/2 In-Aisle Promo Bay', '1/2 In-Aisle Promo Bay Shelf', '1/4 Fixture Shelf', '1/4 In-Aisle Promo Bay Shelf', 'Fixture Full Bay', 'In-Aisle Promo Bay Shelf')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT standard_response_cd, standard_response_text, intervention_group, intervention_start_day, intervention_end_day, actionable_flg FROM acosta_retail_analytics_im.interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- Acosta UK
# MAGIC and 
# MAGIC standard_response_cd <> "null"
# MAGIC order by
# MAGIC standard_response_cd,
# MAGIC standard_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.interventions_parameters

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT  standard_response_text, mdm_banner_nm, standard_response_cd, intervention_group, intervention_start_day, intervention_end_day, actionable_flg FROM acosta_retail_analytics_im.interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- Acosta UK
# MAGIC and 
# MAGIC standard_response_text NOT LIKE '%,%'
# MAGIC order by
# MAGIC standard_response_text,
# MAGIC mdm_banner_nm

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT standard_response_cd, standard_response_text, intervention_group, intervention_start_day, intervention_end_day, actionable_flg, max(call_date) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- Acosta UK
# MAGIC and 
# MAGIC mdm_banner_id = 7745 -- Sainsburys
# MAGIC and 
# MAGIC standard_response_cd <> "null"
# MAGIC and 
# MAGIC call_date >= '2023-01-01'
# MAGIC group by
# MAGIC standard_response_cd, standard_response_text, intervention_group, intervention_start_day, intervention_end_day, actionable_flg
# MAGIC order by
# MAGIC standard_response_cd,
# MAGIC standard_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT standard_response_cd, standard_response_text, intervention_group, intervention_start_day, intervention_end_day, actionable_flg, max(call_date) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- Acosta UK
# MAGIC and 
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and 
# MAGIC standard_response_cd <> "null"
# MAGIC and 
# MAGIC call_date >= '2023-01-01'
# MAGIC group by
# MAGIC standard_response_cd, standard_response_text, intervention_group, intervention_start_day, intervention_end_day, actionable_flg
# MAGIC order by
# MAGIC standard_response_cd,
# MAGIC standard_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17781 -- BAT
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- Acosta UK
# MAGIC and
# MAGIC mdm_banner_id = 7745 -- Sainsburys
# MAGIC -- and
# MAGIC -- coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and 
# MAGIC standard_response_cd <> "null"
# MAGIC order by
# MAGIC mdm_banner_id,
# MAGIC standard_response_cd,
# MAGIC standard_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17781 -- BAT
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- Acosta UK
# MAGIC and
# MAGIC mdm_banner_id = 7745 -- Sainsburys
# MAGIC -- and
# MAGIC -- coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT standard_response_text, actionable_flg, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC call_date >= '2024-01-01'
# MAGIC group by standard_response_text, actionable_flg
# MAGIC order by standard_response_text, actionable_flg

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- US
# MAGIC and
# MAGIC mdm_client_id = 17781 -- BAT
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- Acosta UK
# MAGIC -- and
# MAGIC -- coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC call_date >= '2023-10-01'
# MAGIC group by call_date
# MAGIC order by call_date

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
# MAGIC SELECT SALES_DT, count(*) FROM retaillink_walmart_rbhealth_ca_dv.vw_latest_sat_epos_summary
# MAGIC where SALES_DT >= '2023-06-01'
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) FROM retail_alert_walmart_rbhealth_ca_im.alert_on_shelf_availability
# MAGIC where SALES_DT >= '2023-06-01'
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) FROM team_retail_alert_rbhealth_walmart_drt_ca_im.alert_on_shelf_availability
# MAGIC where SALES_DT >= '2023-06-01'
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) FROM team_retail_alert_rbhealth_walmart_drt_ca_im.alert_inventory_cleanup
# MAGIC where SALES_DT >= '2023-06-01'
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM team_retail_alert_rbhealth_walmart_drt_ca_im.vw_top_actions

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) FROM team_retail_alert_walmart_syn_us_im.alert_on_shelf_availability
# MAGIC where Retail_Client = 'walmart_johnsonandjohnson_us'
# MAGIC and SALES_DT >= '2023-01-01'
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) FROM team_retail_alert_walmart_syn_us_im.alert_inventory_cleanup
# MAGIC where Retail_Client = 'walmart_johnsonandjohnson_us'
# MAGIC and SALES_DT >= '2023-01-01'
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bobv2.vw_bobv2_caps

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select CompanyId, Company, Manufacturer, ManufacturerId, CapType, CapValue from bobv2.vw_bobv2_caps
# MAGIC where ManufacturerId = 214

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE bobv2;
# MAGIC
# MAGIC UPDATE vw_bobv2_caps
# MAGIC    SET CapValue = 8.0 
# MAGIC    WHERE CompanyId = 567
# MAGIC    AND ManufacturerId = 214
# MAGIC    AND CapType = "Inv Cleanup Weeks Cover Max"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bobv2.company

# COMMAND ----------


