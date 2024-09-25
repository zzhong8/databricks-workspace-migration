# Databricks notebook source
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


