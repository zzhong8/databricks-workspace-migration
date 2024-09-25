# Databricks notebook source
# MAGIC %sql
# MAGIC desc retaillink_walmart_trucoenterprisedbaotborder_us_dv.vw_latest_sat_epos_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select sales_dt, count (distinct retailer_item_id) from retaillink_walmart_trucoenterprisedbaotborder_us_dv.vw_latest_sat_epos_summary
# MAGIC group by sales_dt
# MAGIC order by sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc retaillink_walmart_trucoenterprisedbaotborder_us_dv.vw_latest_sat_epos_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count (distinct retailer_item_id) from retaillink_walmart_trucoenterprisedbaotborder_us_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt >= '2001-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count (distinct retailer_item_id) from retaillink_walmart_trucoenterprisedbaotborder_us_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt >= '2022-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC desc 8451stratum_kroger_bluetriton_us_dv.vw_latest_sat_epos_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select * from interventions_parameters 
# MAGIC    WHERE mdm_holding_id = 91 -- Kroger
# MAGIC    AND mdm_client_id = 17694 -- BlueTriton
# MAGIC    AND mdm_country_id = 1    -- US

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select count(*) from vw_ds_intervention_input_nars
# MAGIC    WHERE mdm_holding_id = 91 -- Kroger
# MAGIC    AND mdm_client_id = 17694 -- BlueTriton
# MAGIC    AND mdm_country_id = 1    -- US
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select distinct standard_response_text from vw_ds_intervention_input_nars
# MAGIC    WHERE mdm_holding_id = 91 -- Kroger
# MAGIC    AND mdm_client_id = 17694 -- BlueTriton
# MAGIC    AND mdm_country_id = 1    -- US
# MAGIC order by standard_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC desc interventions_parameters

# COMMAND ----------


