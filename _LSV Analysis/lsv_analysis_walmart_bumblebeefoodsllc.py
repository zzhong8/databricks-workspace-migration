# Databricks notebook source
# MAGIC %sql
# MAGIC select * from acosta_retail_analytics_im.interventions_retailer_client_config
# MAGIC where mdm_country_id = 1 -- US
# MAGIC and mdm_holding_id = 71 -- Walmart
# MAGIC -- and mdm_client_id = 16279 -- Bumble Bee Foods LLC

# COMMAND ----------

# MAGIC %sql
# MAGIC desc acosta_retail_analytics_im.interventions_response_mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acosta_retail_analytics_im.interventions_response_mapping
# MAGIC where mdm_country_id = 1 -- US

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acosta_retail_analytics_im.interventions_parameters
# MAGIC where mdm_country_id = 1 -- US
# MAGIC and mdm_holding_id = 71 -- Walmart
# MAGIC and (mdm_client_id = 16279 -- Bumble Bee Foods LLC
# MAGIC or mdm_client_id = 16540)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc bobv2.company

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use bobv2;
# MAGIC select * from company

# COMMAND ----------

# MAGIC %sql
# MAGIC --parent_chain_id
# MAGIC
# MAGIC use bobv2;
# MAGIC select * from chain

# COMMAND ----------


