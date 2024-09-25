# Databricks notebook source
# MAGIC %sql
# MAGIC use retaillink_walmart_utzqualityfoods_us_dv;
# MAGIC
# MAGIC -- View daily ePOS data counts
# MAGIC select sales_dt, count(*) from vw_latest_sat_epos_summary a --join vw_latest_sat_retailer_item b on a.retailer_item_id = b.retailer_item_id
# MAGIC where a.sales_dt >= '2024-01-01'
# MAGIC group by sales_dt
# MAGIC order by sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC use retail_alert_walmart_utzqualityfoods_us_im;
# MAGIC
# MAGIC -- View daily candidate inventory cleanup alert counts
# MAGIC select sales_dt, count(*) from alert_inventory_cleanup
# MAGIC where sales_dt >= '2024-01-01'
# MAGIC group by sales_dt
# MAGIC order by sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC use retail_alert_walmart_utzqualityfoods_us_im;
# MAGIC
# MAGIC -- View daily candidate OSA alert counts
# MAGIC select sales_dt, count(*) from alert_on_shelf_availability
# MAGIC where sales_dt >= '2024-01-01'
# MAGIC group by sales_dt
# MAGIC order by sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC use team_retail_alert_walmart_syn_us_im;
# MAGIC
# MAGIC -- View daily team inventory cleanup alert counts
# MAGIC select sales_dt, count(*) from alert_inventory_cleanup
# MAGIC where sales_dt >= '2024-01-01'
# MAGIC and retail_client == 'walmart_utzqualityfoods_us'
# MAGIC group by sales_dt
# MAGIC order by sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC use team_retail_alert_walmart_syn_us_im;
# MAGIC
# MAGIC -- View daily team OSA alert counts
# MAGIC select sales_dt, count(*) from alert_on_shelf_availability
# MAGIC where sales_dt >= '2024-01-01'
# MAGIC and retail_client == 'walmart_utzqualityfoods_us'
# MAGIC group by sales_dt
# MAGIC order by sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.interventions_parameters

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select call_date, count(*) from vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 17688 -- Kenvue UK
# MAGIC GROUP by call_date
# MAGIC order BY call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select call_date, count(*) from vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 17688 -- Kenvue UK
# MAGIC and
# MAGIC actionable_flg = 1
# MAGIC GROUP by call_date
# MAGIC order BY call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select * from interventions_retailer_client_config
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 71 -- walmart
# MAGIC and
# MAGIC mdm_client_id in (17537, 14035) -- Lactalis and Utz

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select * from interventions_retailer_client_config_gen2_migration
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 71 -- walmart
# MAGIC and
# MAGIC mdm_client_id in (17537, 14035) -- Lactalis and Utz

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select distinct mdm_client_id, standard_response_cd, standard_response_text, intervention_rank, intervention_group, intervention_start_day, intervention_end_day, actionable_flg from interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 71 -- walmart
# MAGIC and
# MAGIC mdm_client_id = 17537 -- Lactalis
# MAGIC and
# MAGIC standard_response_cd <> 'null'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select * from interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 71 -- walmart
# MAGIC and
# MAGIC mdm_client_id = 17537 -- Lactalis
# MAGIC and
# MAGIC standard_response_cd <> 'null'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select * from vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 71 -- walmart
# MAGIC and
# MAGIC mdm_client_id = 17537 -- Lactalis

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select distinct mdm_client_id, standard_response_cd, standard_response_text, intervention_rank, intervention_group, intervention_start_day, intervention_end_day, actionable_flg from interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 71 -- walmart
# MAGIC and
# MAGIC mdm_client_id = 14035 -- Utz

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select * from interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 71 -- walmart
# MAGIC and
# MAGIC mdm_client_id = 14035 -- Utz

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select * from vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 71 -- walmart
# MAGIC and
# MAGIC mdm_client_id = 14035 -- Utz

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select * from vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 71 -- walmart
# MAGIC and
# MAGIC mdm_client_id = 14035 -- Utz
# MAGIC AND
# MAGIC call_date >= '2023-05-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select distinct mdm_client_id, standard_response_cd, standard_response_text, intervention_rank, intervention_group, intervention_start_day, intervention_end_day, actionable_flg from vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 71 -- walmart
# MAGIC and
# MAGIC mdm_client_id = 17537 -- Lactalis

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select distinct mdm_client_id, standard_response_cd, standard_response_text, intervention_rank, intervention_group, intervention_start_day, intervention_end_day, actionable_flg from vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 17687 -- Red Bull
# MAGIC and
# MAGIC actionable_flg = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select count(*) from vw_ds_intervention_input_nars
# MAGIC where
# MAGIC -- mdm_country_id = 30 -- UK
# MAGIC -- and
# MAGIC -- mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- mdm_client_id = 17685 -- Dr O UK
# MAGIC -- and
# MAGIC call_date >= '2023-01-01'
# MAGIC and
# MAGIC call_date <= '2023-12-31'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select count(*) from vw_ds_intervention_summary
# MAGIC where
# MAGIC -- mdm_country_id = 30 -- UK
# MAGIC -- and
# MAGIC -- mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- mdm_client_id = 17685 -- Dr O UK
# MAGIC -- and
# MAGIC call_date >= '2023-01-01'
# MAGIC and
# MAGIC call_date <= '2023-12-31'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.vw_ds_intervention_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select count(*) from vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC -- mdm_client_id = 17685 -- Dr O UK
# MAGIC -- and
# MAGIC call_date >= '2023-01-01'
# MAGIC and
# MAGIC call_date <= '2023-12-31'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select count(*) from vw_ds_intervention_summary
# MAGIC where
# MAGIC country_id = 30 -- UK
# MAGIC and
# MAGIC holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC -- mdm_client_id = 17685 -- Dr O UK
# MAGIC -- and
# MAGIC call_date >= '2023-01-01'
# MAGIC and
# MAGIC call_date <= '2023-12-31'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select count(*) from vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC call_date >= '2023-01-01'
# MAGIC and
# MAGIC call_date <= '2023-12-31'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select count(*) from vw_ds_intervention_summary
# MAGIC where
# MAGIC country_id = 30 -- UK
# MAGIC and
# MAGIC holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC call_date >= '2023-01-01'
# MAGIC and
# MAGIC call_date <= '2023-12-31'

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
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select distinct mdm_client_id, standard_response_cd, standard_response_text, intervention_rank, intervention_group, intervention_start_day, intervention_end_day, actionable_flg from vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 17685 -- Dr O UK
# MAGIC and
# MAGIC actionable_flg = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select distinct mdm_client_id, standard_response_cd, standard_response_text, intervention_rank, intervention_group, intervention_start_day, intervention_end_day, actionable_flg from interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 17685 -- Dr O UK
# MAGIC and
# MAGIC actionable_flg = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select distinct mdm_client_id, standard_response_cd, standard_response_text, intervention_rank, intervention_group, intervention_start_day, intervention_end_day, actionable_flg from interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 17685 -- Dr O UK
# MAGIC AND
# MAGIC standard_response_text = 'Facings Increased'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select distinct mdm_client_id, standard_response_cd, standard_response_text, intervention_rank, intervention_group, intervention_start_day, intervention_end_day, actionable_flg from vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 17685 -- Dr O UK
# MAGIC AND
# MAGIC standard_response_text = 'Facings Increased'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select year(call_date) as call_year, month(call_date) as call_month, count(*) from vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_client_id = 17688 -- Kenvue UK
# MAGIC and
# MAGIC actionable_flg = 1
# MAGIC GROUP by call_year, call_month
# MAGIC order BY call_year, call_month

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
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
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
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_summary
# MAGIC where
# MAGIC country_id = 30 -- UK
# MAGIC and
# MAGIC client_id = 17781 -- BAT
# MAGIC and
# MAGIC holding_id = 3257 -- Acosta UK
# MAGIC and
# MAGIC banner_id = 7745 -- Sainsburys
# MAGIC -- and
# MAGIC -- coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC limit 100

# COMMAND ----------


