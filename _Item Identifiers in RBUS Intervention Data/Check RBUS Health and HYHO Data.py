# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC use retaillink_walmart_rbusahealth_us_dv;
# MAGIC
# MAGIC select distinct sales_dt from vw_latest_sat_epos_summary order by sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use retaillink_walmart_rbusahygiene_us_dv;
# MAGIC
# MAGIC select distinct sales_dt from vw_latest_sat_epos_summary order by sales_dt

# COMMAND ----------


