# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.interventions_retailer_client_config

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.interventions_response_mapping
# MAGIC order by mdm_country_id, objective_typ, standard_response_text

# COMMAND ----------

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
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- standard_response_text in
# MAGIC -- ("Increased Facings",
# MAGIC -- "Merchandising Fixture",
# MAGIC -- "Pallet Corner",
# MAGIC -- "Promotion Re-Charge")
# MAGIC order by
# MAGIC standard_response_text,
# MAGIC mdm_banner_id,
# MAGIC objective_typ
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



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
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
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT mdm_banner_id, call_date, count(*), sum(total_intervention_effect), sum(total_impact) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC -- and
# MAGIC -- objective_typ='DLA'
# MAGIC -- standard_response_text in
# MAGIC -- ("Merchandising Display",
# MAGIC -- "Merchandising Fixture")
# MAGIC and
# MAGIC call_date >= '2022-03-01'
# MAGIC group by mdm_banner_id, call_date
# MAGIC order by mdm_banner_id, call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT mdm_banner_id, call_date, count(*), sum(total_intervention_effect), sum(total_impact) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC call_date >= '2022-03-01'
# MAGIC group by mdm_banner_id, call_date
# MAGIC order by mdm_banner_id, call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT mdm_banner_id, call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC epos_retailer_item_id is not NULL
# MAGIC and
# MAGIC actionable_flg is not NULL
# MAGIC and
# MAGIC call_date >= '2022-03-01'
# MAGIC group by mdm_banner_id, call_date
# MAGIC order by mdm_banner_id, call_date

# COMMAND ----------


