# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC SELECT sales_dt, count(*) FROM msd_morrisons_nestlecore_uk_dv.vw_latest_sat_epos_summary
# MAGIC group by sales_dt
# MAGIC order by sales_dt desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM msd_morrisons_nestlecore_uk_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt = '2023-09-03'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM msd_morrisons_nestlecore_uk_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt = '2023-09-02'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM msd_morrisons_nestlecore_uk_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt = '2023-09-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_audit_summary

# COMMAND ----------


