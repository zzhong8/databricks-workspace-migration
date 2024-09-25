# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC use acosta_retail_analytics_im;
# MAGIC
# MAGIC desc vw_dimension_store

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_retail_analytics_im;
# MAGIC
# MAGIC select distinct 
# MAGIC
# MAGIC holding_id, holding_description, 
# MAGIC channel_id, channel_description, 
# MAGIC division_id, division_description, 
# MAGIC banner_id, banner_description, 
# MAGIC subbanner_id, subbanner_description
# MAGIC ----market_id, market_description, 
# MAGIC
# MAGIC from vw_dimension_store
# MAGIC     
# MAGIC where holding_id = 71 -- Walmart US
# MAGIC
# MAGIC order by
# MAGIC holding_id,
# MAGIC channel_id,
# MAGIC division_id,
# MAGIC banner_id,
# MAGIC subbanner_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_retail_analytics_im;
# MAGIC
# MAGIC select distinct 
# MAGIC
# MAGIC holding_id, holding_description, 
# MAGIC channel_id, channel_description, 
# MAGIC division_id, division_description, 
# MAGIC banner_id, banner_description, 
# MAGIC subbanner_id, subbanner_description
# MAGIC ----market_id, market_description, 
# MAGIC
# MAGIC from vw_dimension_store
# MAGIC     
# MAGIC where holding_id = 71 -- Walmart US
# MAGIC
# MAGIC order by
# MAGIC holding_id,
# MAGIC channel_id,
# MAGIC division_id,
# MAGIC banner_id,
# MAGIC subbanner_id
