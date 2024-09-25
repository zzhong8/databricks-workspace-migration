# Databricks notebook source
import datetime
import pyspark.sql.functions as pyf

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) FROM rsi_boots_beiersdorf_uk_dv.vw_latest_sat_epos_summary
# MAGIC   where sales_dt >= '2023-01-01'
# MAGIC   AND sales_dt <= '2024-12-31'
# MAGIC   GROUP BY SALES_DT
# MAGIC   ORDER BY SALES_DT
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) FROM circana_boots_beiersdorf_uk_dv.vw_latest_sat_epos_summary
# MAGIC   where sales_dt >= '2023-01-01'
# MAGIC   AND sales_dt <= '2024-12-31'
# MAGIC   GROUP BY SALES_DT
# MAGIC   ORDER BY SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acosta_retail_analytics_im.interventions_retailer_client_config
# MAGIC where mdm_client_id = 17686

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acosta_retail_analytics_im.interventions_retailer_client_config_gen2_migration
# MAGIC where mdm_client_id = 17686

# COMMAND ----------

# MAGIC %sql
# MAGIC desc acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acosta_retail_analytics_im.interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and 
# MAGIC actionable_flg = 1
# MAGIC order by
# MAGIC intervention_rank, standard_response_text, standard_response_cd, objective_typ

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acosta_retail_analytics_im.interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7996 -- Boots
# MAGIC and 
# MAGIC actionable_flg = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7996 -- Boots

# COMMAND ----------

# MAGIC %sql
# MAGIC select call_date, count(*) from acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where mdm_client_id = 17686 -- Beiersdorf
# MAGIC and mdm_banner_id = 7996 -- Boots
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC select call_date, count(*) from acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where mdm_client_id = 17686 -- Beiersdorf
# MAGIC and mdm_banner_id <> 7996 -- All excl. Boots
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(distinct epos_retailer_item_id) from acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7996 -- Boots
# MAGIC and
# MAGIC call_date >= '2023-01-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(distinct epos_retailer_item_id) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7996 -- Boots
# MAGIC and
# MAGIC call_date >= '2023-01-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(distinct epos_retailer_item_id) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id <> 7996 -- All excl. Boots
# MAGIC and
# MAGIC call_date >= '2023-01-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) FROM retaillink_asda_beiersdorf_uk_dv.vw_latest_sat_epos_summary
# MAGIC   where sales_dt >= '2010-01-01'
# MAGIC   AND sales_dt <= '2024-12-31'
# MAGIC   GROUP BY SALES_DT
# MAGIC   ORDER BY SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(*) from acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC call_date >= '2020-01-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(*) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC call_date >= '2020-01-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from retail_alert_asda_beiersdorf_uk_im.LOESS_FORECAST_BASELINE_UNIT_UPC
# MAGIC limit 10

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC select sales_dt, count(*) from retail_alert_asda_beiersdorf_uk_im.LOESS_FORECAST_BASELINE_UNIT_UPC
# MAGIC group by sales_dt
# MAGIC order by sales_dt
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- objective_typ = 'DLA'
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC call_date >= '2023-01-01'
# MAGIC AND call_date <= '2024-12-31'
# MAGIC order by call_date, response_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(measurement_duration), sum(total_intervention_effect), sum(total_impact) from acosta_retail_analytics_im.ds_intervention_summary_20240802_asda_beiersdorf_drfe_ivm_test
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- objective_typ = 'DLA'
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC call_date >= '2023-01-01'
# MAGIC AND call_date <= '2024-12-31'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.ds_intervention_summary_20240806_asda_beiersdorf_loess_ivm_test
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- objective_typ = 'DLA'
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC call_date >= '2023-01-01'
# MAGIC AND call_date <= '2024-12-31'
# MAGIC order by call_date, response_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(measurement_duration), sum(total_intervention_effect), sum(total_impact) from acosta_retail_analytics_im.ds_intervention_summary_20240806_asda_beiersdorf_loess_ivm_test
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- objective_typ = 'DLA'
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC call_date >= '2023-01-01'
# MAGIC AND call_date <= '2024-12-31'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.ds_intervention_audit_raw
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC call_date >= '2024-04-12'
# MAGIC and
# MAGIC intervention_end_day > 0
# MAGIC and
# MAGIC (
# MAGIC status_code like '1%'
# MAGIC )
# MAGIC and
# MAGIC audit_ts >= '2024-04-13'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.ds_intervention_audit_raw
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC call_date >= '2024-04-12'
# MAGIC and
# MAGIC (
# MAGIC status_code like '1%'
# MAGIC or
# MAGIC status_code like '3%'
# MAGIC or
# MAGIC status_code like '4%'
# MAGIC or
# MAGIC status_code like '5%'
# MAGIC )
# MAGIC and
# MAGIC audit_ts >= '2024-04-13'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.ds_intervention_audit_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC audit_ts like '2024-04-27%'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.ds_intervention_audit_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC audit_ts >= '2024-05-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.ds_intervention_audit_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC audit_ts >= '2024-05-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.ds_intervention_audit_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7744 -- Morrisons
# MAGIC and
# MAGIC audit_ts >= '2024-05-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.ds_intervention_audit_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7746 -- Tesco
# MAGIC and
# MAGIC audit_ts >= '2024-05-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(distinct epos_retailer_item_id) from acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id < 7996 -- All excl Boots
# MAGIC and
# MAGIC objective_typ = 'DLA'
# MAGIC and
# MAGIC call_date >= '2023-01-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(distinct upc) from acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id < 7996 -- All excl Boots
# MAGIC and
# MAGIC objective_typ = 'DLA'
# MAGIC and
# MAGIC call_date >= '2023-01-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(*) from acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id < 7996 -- All excl Boots
# MAGIC and
# MAGIC objective_typ = 'DLA'
# MAGIC and
# MAGIC call_date >= '2023-01-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(*) from acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC actionable_flg = 1
# MAGIC and
# MAGIC intervention_end_day <= 14
# MAGIC -- and
# MAGIC -- objective_typ = 'Opportunity'
# MAGIC -- and
# MAGIC -- mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC call_date >= '2024-03-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(*) from acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC actionable_flg = 1
# MAGIC and
# MAGIC intervention_end_day < 21
# MAGIC -- and
# MAGIC -- objective_typ = 'Opportunity'
# MAGIC -- and
# MAGIC -- mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC call_date >= '2024-03-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(*) from acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC actionable_flg = 1
# MAGIC and
# MAGIC intervention_end_day >= 21
# MAGIC -- and
# MAGIC -- objective_typ = 'Opportunity'
# MAGIC -- and
# MAGIC -- mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC call_date >= '2024-03-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.ds_intervention_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct objective_typ from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, sum(total_intervention_effect), sum(total_impact) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'Data Led Alerts'
# MAGIC -- and
# MAGIC -- mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC call_date >= '2023-07-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, sum(total_intervention_effect), sum(total_impact) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'DLA'
# MAGIC -- and
# MAGIC -- mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC call_date >= '2023-07-01'
# MAGIC group by call_date
# MAGIC order by call_date
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, sum(total_intervention_effect), sum(total_impact) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'Opportunity'
# MAGIC -- and
# MAGIC -- mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC call_date >= '2023-07-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(distinct epos_retailer_item_id) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7996 -- Boots
# MAGIC and
# MAGIC objective_typ = 'DLA'
# MAGIC and
# MAGIC call_date >= '2023-01-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(distinct epos_retailer_item_id) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id < 7996 -- All excl Boots
# MAGIC and
# MAGIC objective_typ = 'DLA'
# MAGIC and
# MAGIC call_date >= '2023-01-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, sum(total_intervention_effect), sum(total_impact) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'DLA'
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC call_date >= '2023-07-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, sum(total_intervention_effect), sum(total_impact) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'DLA'
# MAGIC and
# MAGIC mdm_banner_id = 7744 -- Morrisons
# MAGIC and
# MAGIC call_date >= '2023-07-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, sum(total_intervention_effect), sum(total_impact) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'DLA'
# MAGIC and
# MAGIC mdm_banner_id = 7996 -- Boots
# MAGIC and
# MAGIC call_date >= '2023-07-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, sum(total_intervention_effect), sum(total_impact) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'DLA'
# MAGIC and
# MAGIC mdm_banner_id = 7746 -- Tesco
# MAGIC and
# MAGIC call_date >= '2023-07-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.interventions_parameters

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select distinct mdm_client_id, standard_response_cd, standard_response_text, intervention_rank, intervention_group, intervention_start_day, intervention_end_day, actionable_flg
# MAGIC from interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK
# MAGIC and
# MAGIC actionable_flg = 1
# MAGIC order BY
# MAGIC intervention_rank,
# MAGIC standard_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select distinct mdm_client_id, standard_response_cd, standard_response_text, intervention_rank, intervention_group, intervention_start_day, intervention_end_day, actionable_flg
# MAGIC from interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK
# MAGIC and
# MAGIC standard_response_cd <> 'null'
# MAGIC order BY
# MAGIC intervention_rank,
# MAGIC standard_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select distinct mdm_client_id, standard_response_cd, standard_response_text, intervention_rank, intervention_group, intervention_start_day, intervention_end_day, actionable_flg
# MAGIC from vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK
# MAGIC and
# MAGIC actionable_flg = 1
# MAGIC order BY
# MAGIC standard_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM team_retail_alert_beiersdorf_drt_uk_im.alert_on_shelf_availability
# MAGIC   WHERE Retail_Client = 'boots_beiersdorf_uk'
# MAGIC   AND sales_dt >= '2024-01-01'
# MAGIC   AND sales_dt <= '2024-12-31'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT sales_dt, count(*) FROM team_retail_alert_beiersdorf_drt_uk_im.alert_on_shelf_availability
# MAGIC   WHERE Retail_Client = 'morrisons_beiersdorf_uk'
# MAGIC   AND sales_dt >= '2024-01-01'
# MAGIC   AND sales_dt <= '2024-12-31'
# MAGIC   GROUP BY sales_dt
# MAGIC   ORDER BY sales_dt
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM team_retail_alert_campbells_meijer_drt_us_im.alert_on_shelf_availability
# MAGIC   WHERE Retail_Client = 'meijer_campbells_us'
# MAGIC   AND sales_dt >= '2024-01-01'
# MAGIC   AND sales_dt <= '2024-12-31'
# MAGIC   ORDER BY sales_dt, organization_unit_num, retailer_item_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM team_retail_alert_walmart_syn_us_im.alert_on_shelf_availability
# MAGIC   WHERE Retail_Client = 'walmart_everymanjack_us'
# MAGIC   AND sales_dt >= '2024-01-01'
# MAGIC   AND sales_dt <= '2024-12-31'
# MAGIC   ORDER BY sales_dt, organization_unit_num, retailer_item_id
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM team_retail_alert_bandgfoods_walmart_drt_ca_im.alert_on_shelf_availability
# MAGIC   WHERE Retail_Client = 'walmart_bandgfoods_ca'
# MAGIC   AND sales_dt >= '2024-01-01'
# MAGIC   AND sales_dt <= '2024-12-31'
# MAGIC   ORDER BY sales_dt, organization_unit_num, retailer_item_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
# MAGIC   WHERE Retail_Client = 'asda_nestlecore_uk'
# MAGIC   AND sales_dt >= '2024-06-01'
# MAGIC   AND sales_dt <= '2024-06-30'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(DISTINCT sales_dt, organization_unit_num, retailer_item_id) FROM team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
# MAGIC   WHERE Retail_Client = 'asda_nestlecore_uk'
# MAGIC   AND sales_dt >= '2024-06-01'
# MAGIC   AND sales_dt <= '2024-06-30'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(DISTINCT sales_dt, organization_unit_num, OutletId, retailer_item_id) FROM team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
# MAGIC   WHERE Retail_Client = 'asda_nestlecore_uk'
# MAGIC   AND sales_dt >= '2024-06-01'
# MAGIC   AND sales_dt <= '2024-06-30'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
# MAGIC   WHERE Retail_Client = 'asda_nestlecore_uk'
# MAGIC   AND sales_dt >= '2024-06-01'
# MAGIC   AND sales_dt <= '2024-06-30'
# MAGIC   ORDER BY sales_dt, organization_unit_num, retailer_item_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
# MAGIC   WHERE Retail_Client = 'sainsburys_nestlecore_uk'
# MAGIC   AND sales_dt >= '2024-06-01'
# MAGIC   AND sales_dt <= '2024-06-30'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(DISTINCT sales_dt, organization_unit_num, retailer_item_id) FROM team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
# MAGIC   WHERE Retail_Client = 'sainsburys_nestlecore_uk'
# MAGIC   AND sales_dt >= '2024-06-01'
# MAGIC   AND sales_dt <= '2024-06-30'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
# MAGIC   WHERE Retail_Client = 'sainsburys_nestlecore_uk'
# MAGIC   AND sales_dt >= '2024-06-01'
# MAGIC   AND sales_dt <= '2024-06-30'
# MAGIC   ORDER BY sales_dt, organization_unit_num, retailer_item_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
# MAGIC   WHERE Retail_Client = 'morrisons_nestlecore_uk'
# MAGIC   AND sales_dt >= '2024-06-01'
# MAGIC   AND sales_dt <= '2024-06-30'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(DISTINCT sales_dt, organization_unit_num, retailer_item_id) FROM team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
# MAGIC   WHERE Retail_Client = 'morrisons_nestlecore_uk'
# MAGIC   AND sales_dt >= '2024-06-01'
# MAGIC   AND sales_dt <= '2024-06-30'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(DISTINCT sales_dt, organization_unit_num, OutletId, retailer_item_id) FROM team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
# MAGIC   WHERE Retail_Client = 'morrisons_nestlecore_uk'
# MAGIC   AND sales_dt >= '2024-06-01'
# MAGIC   AND sales_dt <= '2024-06-30'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT sales_dt, organization_unit_num, OutletId, retailer_item_id FROM team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
# MAGIC   WHERE Retail_Client = 'morrisons_nestlecore_uk'
# MAGIC   AND sales_dt >= '2024-06-01'
# MAGIC   AND sales_dt <= '2024-06-30'
# MAGIC   ORDER BY sales_dt, organization_unit_num, OutletId, retailer_item_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
# MAGIC   WHERE Retail_Client = 'morrisons_nestlecore_uk'
# MAGIC   AND sales_dt >= '2024-06-01'
# MAGIC   AND sales_dt <= '2024-06-30'
# MAGIC   ORDER BY sales_dt, organization_unit_num, retailer_item_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
# MAGIC   WHERE Retail_Client = 'morrisons_nestlecore_uk'
# MAGIC   AND sales_dt >= '2024-06-01'
# MAGIC   AND sales_dt <= '2024-06-30'
# MAGIC   ORDER BY sales_dt, organization_unit_num, retailer_item_id

# COMMAND ----------

sql_query_morrisons_nestle_june_2024_alerts = """
    SELECT * FROM team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
    WHERE Retail_Client = 'morrisons_nestlecore_uk'
    AND sales_dt >= '2024-06-01'
    AND sales_dt <= '2024-06-30'
    ORDER BY sales_dt, organization_unit_num, retailer_item_id
"""

df_morrisons_nestle_june_2024_alerts = spark.sql(sql_query_morrisons_nestle_june_2024_alerts)

df_morrisons_nestle_june_2024_alerts = df_morrisons_nestle_june_2024_alerts.withColumn('sales_dt_1', pyf.expr("date_add(sales_dt, 1)"))
df_morrisons_nestle_june_2024_alerts = df_morrisons_nestle_june_2024_alerts.withColumn('sales_dt_2', pyf.expr("date_add(sales_dt, 2)"))
df_morrisons_nestle_june_2024_alerts = df_morrisons_nestle_june_2024_alerts.withColumn('sales_dt_3', pyf.expr("date_add(sales_dt, 3)"))

display(df_morrisons_nestle_june_2024_alerts)

# COMMAND ----------


