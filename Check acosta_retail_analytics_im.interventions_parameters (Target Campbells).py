# Databricks notebook source
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
# MAGIC SELECT * FROM acosta_retail_analytics_im.interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 851 -- Campbells
# MAGIC and
# MAGIC mdm_holding_id = 2301 -- Target
# MAGIC order by
# MAGIC standard_response_text,
# MAGIC mdm_banner_id,
# MAGIC objective_typ
# MAGIC

# COMMAND ----------

# %sql

# SELECT distinct
# mdm_country_id,
# mdm_country_nm,
# mdm_holding_id,
# mdm_holding_nm,
# mdm_banner_id,
# mdm_banner_nm,
# mdm_client_id
# FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars

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
# MAGIC mdm_client_nm
# MAGIC FROM acosta_retail_analytics_im.interventions_parameters

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC call_date >= '2022-03-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC epos_retailer_item_id is not NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct epos_retailer_item_id FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC call_date >= '2022-03-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct epos_retailer_item_id FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC call_date < '2022-05-12'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct epos_retailer_item_id FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC call_date < '2022-05-12'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct epos_retailer_item_id FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC call_date >= '2022-05-12'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC -- coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC -- and
# MAGIC -- objective_typ='DLA'
# MAGIC standard_response_text in
# MAGIC ("Merchandising Display",
# MAGIC "Merchandising Fixture")
# MAGIC and
# MAGIC call_date >= '2022-03-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 851 -- Campbells
# MAGIC and
# MAGIC mdm_holding_id = 2301 -- Target
# MAGIC and
# MAGIC epos_retailer_item_id is not NULL
# MAGIC -- and
# MAGIC -- call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 851 -- Campbells
# MAGIC and
# MAGIC mdm_holding_id = 2301 -- Target
# MAGIC and
# MAGIC epos_retailer_item_id is NULL
# MAGIC -- and
# MAGIC -- call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC epos_retailer_item_id is not NULL
# MAGIC -- and
# MAGIC -- call_date >= '2022-03-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC epos_retailer_item_id is not NULL
# MAGIC and
# MAGIC actionable_flg is not NULL
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC epos_retailer_item_id is NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC call_date < '2022-03-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC nars_response_text = 'RCM Stocked Shelf'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC call_date = '2022-05-15'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC call_date < '2022-03-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC epos_retailer_item_id is NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC -- and
# MAGIC -- call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC is_complete = 'true'
# MAGIC -- and
# MAGIC -- call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC is_complete = 'false'
# MAGIC -- and
# MAGIC -- call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT MONTH(call_date), sum(total_intervention_effect), sum(total_impact) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC group by MONTH(call_date)
# MAGIC order by MONTH(call_date)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC epos_retailer_item_id = 585801594

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC epos_retailer_item_id = '585801594'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC epos_retailer_item_id = '585801594'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC epos_retailer_item_id = 585801594

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC standard_response_text = 'Dept Manager Reactivated'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 882 -- Danone
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC -- and
# MAGIC -- call_date >= '2022-03-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------


