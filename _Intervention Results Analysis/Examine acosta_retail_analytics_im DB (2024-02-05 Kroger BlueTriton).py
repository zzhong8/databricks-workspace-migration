# Databricks notebook source
# MAGIC %sql
# MAGIC desc retaillink_walmart_bluetritonbrands_us_dv.vw_latest_sat_epos_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC desc 8451stratum_kroger_bluetriton_us_dv.vw_latest_sat_epos_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct organization_unit_num, retailer_item_id from 8451stratum_kroger_bluetriton_us_dv.vw_latest_sat_epos_summary
# MAGIC order by retailer_item_id, organization_unit_num

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct retailer_item_id from 8451stratum_kroger_bluetriton_us_dv.vw_latest_sat_epos_summary
# MAGIC order by retailer_item_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select SALES_DT, count(*) from 8451stratum_kroger_bluetriton_us_dv.vw_latest_sat_epos_summary
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC select SALES_DT, count(*) from retail_alert_kroger_bluetriton_us_im.alert_on_shelf_availability
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC select SALES_DT, count(*) from team_retail_alert_bluetriton_kroger_syn_us_im.alert_on_shelf_availability
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC select SALES_DT, count(*) from team_retail_alert_bluetriton_kroger_syn_us_im.alert_inventory_cleanup
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

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
# MAGIC select * from interventions_retailer_client_config_gen2_migration
# MAGIC    WHERE mdm_holding_id = 91 -- Kroger
# MAGIC    AND mdm_client_id = 17694 -- BlueTriton
# MAGIC    AND mdm_country_id = 1    -- US

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
# MAGIC select call_date, count(*) from vw_ds_intervention_input_nars
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
# MAGIC select distinct standard_response_cd, standard_response_text, intervention_rank, actionable_flg from vw_ds_intervention_input_nars
# MAGIC    WHERE mdm_holding_id = 91 -- Kroger
# MAGIC    AND mdm_client_id = 17694 -- BlueTriton
# MAGIC    AND mdm_country_id = 1    -- US
# MAGIC order by standard_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select * from vw_ds_intervention_input_nars 
# MAGIC    WHERE mdm_holding_id = 91 -- Kroger
# MAGIC    AND mdm_client_id = 17694 -- BlueTriton
# MAGIC    AND mdm_country_id = 1    -- US

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select DISTINCT epos_retailer_item_id from vw_ds_intervention_input_nars 
# MAGIC    WHERE mdm_holding_id = 91 -- Kroger
# MAGIC    AND mdm_client_id = 17694 -- BlueTriton
# MAGIC    AND mdm_country_id = 1    -- US
# MAGIC ORDER BY epos_retailer_item_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select * from vw_ds_intervention_summary
# MAGIC    WHERE holding_id = 91 -- Kroger
# MAGIC    AND client_id = 17694 -- BlueTriton
# MAGIC    AND country_id = 1    -- US

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select * from ds_intervention_summary
# MAGIC    WHERE mdm_holding_id = 91 -- Kroger
# MAGIC    AND mdm_client_id = 17694 -- BlueTriton
# MAGIC    AND mdm_country_id = 1    -- US

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
# MAGIC desc interventions_parameters

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select * from vw_ds_intervention_input_nars 
# MAGIC    WHERE mdm_holding_id = 91 -- Kroger
# MAGIC    AND mdm_client_id = 460 -- MinuteMaid (aka Coca-Cola)
# MAGIC    AND mdm_country_id = 1    -- US
# MAGIC    and call_date >= '2024-02-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from market6_kroger_cocacola_us_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt >= '2024-02-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select * from ds_intervention_summary
# MAGIC    WHERE mdm_holding_id = 91 -- Kroger
# MAGIC    AND mdm_client_id = 460 -- MinuteMaid (aka Coca-Cola)
# MAGIC    AND mdm_country_id = 1    -- US
# MAGIC    and call_date >= '2024-02-01'

# COMMAND ----------


