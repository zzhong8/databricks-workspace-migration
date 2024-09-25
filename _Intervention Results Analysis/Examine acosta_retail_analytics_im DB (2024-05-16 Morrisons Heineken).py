# Databricks notebook source
# MAGIC %sql
# MAGIC desc msd_morrisons_heineken_uk_dv.vw_latest_sat_epos_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from msd_morrisons_heineken_uk_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt <= '2024-02-29'
# MAGIC and sales_dt >= '2023-12-30'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from msd_morrisons_heineken_uk_dv.vw_latest_sat_epos_summary
# MAGIC limit 20

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from msd_morrisons_heineken_uk_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt <= '2024-02-29'
# MAGIC and sales_dt >= '2023-12-30'
# MAGIC limit 20

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from msd_morrisons_heineken_uk_dv.sat_epos_summary
# MAGIC limit 20

# COMMAND ----------

# MAGIC %sql
# MAGIC desc msd_morrisons_heineken_uk_dv.vw_latest_sat_epos_summary_ivm_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC select sales_dt, count(*) from msd_morrisons_heineken_uk_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt <= '2024-02-29'
# MAGIC and sales_dt >= '2023-12-30'
# MAGIC group by sales_dt
# MAGIC order by sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from msd_morrisons_heineken_uk_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt <= '2024-02-29'
# MAGIC and sales_dt >= '2023-12-30'

# COMMAND ----------

# MAGIC %sql
# MAGIC select sales_dt, count(*) from msd_morrisons_heineken_uk_dv.vw_latest_sat_epos_summary_ivm_temp
# MAGIC group by sales_dt
# MAGIC order by sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC select sales_dt, count(*) from msd_morrisons_heineken_uk_dv.vw_latest_sat_epos_summary_ivm_temp
# MAGIC where sales_dt <= '2024-02-29'
# MAGIC and sales_dt >= '2023-12-30'
# MAGIC group by sales_dt
# MAGIC order by sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from msd_morrisons_heineken_uk_dv.vw_latest_sat_epos_summary_ivm_temp
# MAGIC where sales_dt <= '2024-02-29'
# MAGIC and sales_dt >= '2023-12-30'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from msd_morrisons_heineken_uk_dv.vw_latest_sat_epos_summary_ivm_temp
# MAGIC limit 20

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from msd_morrisons_heineken_uk_dv.vw_latest_sat_epos_summary_ivm_temp
# MAGIC where sales_dt <= '2024-02-29'
# MAGIC and sales_dt >= '2023-12-30'
# MAGIC limit 20

# COMMAND ----------

# MAGIC %sql
# MAGIC desc acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.ds_intervention_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.ds_intervention_summary_temp_msdfix_drp_aftr_20240531

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17682 -- Heineken
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7744 -- Morrisons
# MAGIC and
# MAGIC call_date >= '2024-01-01'
# MAGIC and
# MAGIC call_date <= '2024-01-31'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.ds_intervention_summary_temp_msdfix_drp_aftr_20240531

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC UPDATE ds_intervention_summary_temp_msdfix_drp_aftr_20240531
# MAGIC    SET is_complete = 'FALSE' 
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17682   -- Heineken
# MAGIC    AND mdm_country_id = 30     -- UK
# MAGIC    AND mdm_banner_id IN ('7743','7744','7745','7746') -- Asda, Morrisons, Sainsburys, Tesco
# MAGIC    and
# MAGIC    call_date >= '2023-08-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC UPDATE ds_intervention_summary_temp_msdfix_drp_aftr_20240531
# MAGIC    SET total_intervention_effect = 0.0, total_impact = 0.0
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC    AND mdm_client_id = 17682   -- Heineken
# MAGIC    AND mdm_country_id = 30     -- UK 
# MAGIC    AND mdm_banner_id IN ('7743','7744','7745', '7746') -- Asda, Morrisons, Sainsburys
# MAGIC    AND is_complete = FALSE

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.ds_intervention_audit_raw_temp_msdfix_drp_aftr_20240531

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
# MAGIC select call_date, count(distinct u) from acosta_retail_analytics_im.vw_ds_intervention_input_nars
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
# MAGIC and
# MAGIC mdm_banner_id = 7744 -- Morrisons
# MAGIC and
# MAGIC call_date >= '2023-07-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.ds_intervention_audit_raw

# COMMAND ----------


