# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.interventions_retailer_client_config
# MAGIC where mdm_holding_id = 91

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.interventions_retailer_client_config_gen2_migration
# MAGIC where mdm_holding_id = 91

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT epos_retailer_item_id, upc, min(call_date), max(call_date), count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC epos_retailer_item_id in (74576455, 50298452)
# MAGIC group by epos_retailer_item_id, upc
# MAGIC order by epos_retailer_item_id, upc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 851 -- Campbells
# MAGIC and
# MAGIC mdm_holding_id = 91 -- Kroger
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- N/A

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 851 -- Campbells
# MAGIC and
# MAGIC mdm_holding_id = 91 -- Kroger
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- N/A
# MAGIC AND
# MAGIC call_date >= '2024-08-19'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC epos_retailer_item_id in (74576455, 50298452)
# MAGIC order by
# MAGIC call_date, epos_retailer_item_id
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC epos_retailer_item_id in (50298452)
# MAGIC order by
# MAGIC epos_retailer_item_id, upc, call_date

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
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC epos_retailer_item_id in (74576455, 50298452)
# MAGIC order by
# MAGIC epos_retailer_item_id, call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT epos_retailer_item_id, min(call_date), max(call_date), count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC epos_retailer_item_id in (74576455, 50298452)
# MAGIC group by epos_retailer_item_id
# MAGIC order by epos_retailer_item_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT epos_retailer_item_id, call_date, count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC epos_retailer_item_id in (74576455, 50298452)
# MAGIC group by epos_retailer_item_id, call_date
# MAGIC order by epos_retailer_item_id, call_date

# COMMAND ----------


