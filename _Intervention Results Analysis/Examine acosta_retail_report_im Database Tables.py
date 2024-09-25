# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_report_im.interventions_retailer_client_config

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct mdm_holding_nm, mdm_holding_id FROM acosta_retail_report_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_report_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct 
# MAGIC mdm_country_id,
# MAGIC mdm_country_nm,
# MAGIC mdm_holding_id,
# MAGIC mdm_holding_nm,
# MAGIC mdm_banner_id,
# MAGIC mdm_banner_nm,
# MAGIC mdm_client_id,
# MAGIC mdm_client_nm,
# MAGIC objective_typ,
# MAGIC standard_response_cd,
# MAGIC standard_response_text,
# MAGIC nars_response_text,
# MAGIC intervention_rank,
# MAGIC intervention_group,
# MAGIC intervention_start_day,
# MAGIC intervention_end_day,
# MAGIC actionable_flg
# MAGIC FROM acosta_retail_report_im.vw_ds_intervention_input_nars
# MAGIC where standard_response_cd is null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_retail_report_im;
# MAGIC
# MAGIC UPDATE vw_ds_intervention_input_nars
# MAGIC SET intervention_group = null, intervention_start_day = null, intervention_end_day = null, actionable_flg = null
# MAGIC WHERE standard_response_text = 'Warehouse Out'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct 
# MAGIC mdm_country_id,
# MAGIC mdm_country_nm,
# MAGIC mdm_holding_id,
# MAGIC mdm_holding_nm,
# MAGIC mdm_banner_id,
# MAGIC mdm_banner_nm,
# MAGIC mdm_client_id,
# MAGIC mdm_client_nm,
# MAGIC objective_typ,
# MAGIC standard_response_cd,
# MAGIC standard_response_text,
# MAGIC nars_response_text,
# MAGIC intervention_rank,
# MAGIC intervention_group,
# MAGIC intervention_start_day,
# MAGIC intervention_end_day,
# MAGIC actionable_flg
# MAGIC FROM acosta_retail_report_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_report_im.vw_ds_intervention_input_uk_opportunities

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
