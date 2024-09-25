# Databricks notebook source
from pprint import pprint

import pyspark.sql.functions as pyf

from acosta.measurement import required_columns, process_notebook_inputs

import acosta
from acosta.alerting.preprocessing import read_pos_data

print(acosta.__version__)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Kroger Campbells DRT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables in team_retail_alert_campbells_kroger_drt_us_im

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select SALES_DT, count(*) from team_retail_alert_campbells_kroger_drt_us_im.alert_inventory_cleanup
# MAGIC where Retail_Client = 'kroger_campbells_us'
# MAGIC group by sales_dt
# MAGIC order by sales_dt desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select SALES_DT, count(*) from team_retail_alert_campbells_kroger_drt_us_im.alert_on_shelf_availability
# MAGIC where Retail_Client = 'kroger_campbells_us'
# MAGIC group by sales_dt
# MAGIC order by sales_dt desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select SALES_DT, count(*) from team_retail_alert_campbells_kroger_drt_us_im.vw_retail_alerts_inventory_cleanup
# MAGIC where Retail_Client = 'kroger_campbells_us'
# MAGIC group by sales_dt
# MAGIC order by sales_dt desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select SALES_DT, count(*) from team_retail_alert_campbells_kroger_drt_us_im.vw_retail_alerts_on_shelf_availability
# MAGIC where Retail_Client = 'kroger_campbells_us'
# MAGIC group by sales_dt
# MAGIC order by sales_dt desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from team_retail_alert_campbells_kroger_drt_us_im.vw_retailer_alert_inventory_cleanup_sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from team_retail_alert_campbells_kroger_drt_us_im.vw_retailer_alert_on_shelf_availability_sales_dt

# COMMAND ----------

# MAGIC %md
# MAGIC #### Kroger Campbells Syndicated

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables in team_retail_alert_campbells_kroger_syn_us_im

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select SALES_DT, count(*) from team_retail_alert_campbells_kroger_syn_us_im.alert_inventory_cleanup
# MAGIC where Retail_Client = 'kroger_campbells_us'
# MAGIC group by sales_dt
# MAGIC order by sales_dt desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select SALES_DT, count(*) from team_retail_alert_campbells_kroger_syn_us_im.alert_on_shelf_availability
# MAGIC where Retail_Client = 'kroger_campbells_us'
# MAGIC group by sales_dt
# MAGIC order by sales_dt desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select SALES_DT, count(*) from team_retail_alert_campbells_kroger_syn_us_im.vw_retail_alerts_inventory_cleanup
# MAGIC where Retail_Client = 'kroger_campbells_us'
# MAGIC group by sales_dt
# MAGIC order by sales_dt desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select SALES_DT, count(*) from team_retail_alert_campbells_kroger_syn_us_im.vw_retail_alerts_on_shelf_availability
# MAGIC where Retail_Client = 'kroger_campbells_us'
# MAGIC group by sales_dt
# MAGIC order by sales_dt desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from team_retail_alert_campbells_kroger_syn_us_im.vw_retailer_alert_inventory_cleanup_sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from team_retail_alert_campbells_kroger_drt_us_im.vw_retailer_alert_on_shelf_availability_sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from team_retail_alert_walmart_syn_us_im.vw_retailer_alert_inventory_cleanup_sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from team_retail_alert_walmart_syn_us_im.vw_retailer_alert_on_shelf_availability_sales_dt
