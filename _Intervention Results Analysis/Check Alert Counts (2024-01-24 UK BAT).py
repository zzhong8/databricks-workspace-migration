# Databricks notebook source
from pprint import pprint

import pyspark.sql.functions as pyf

from acosta.measurement import required_columns, process_notebook_inputs

import acosta
from acosta.alerting.preprocessing import read_pos_data

print(acosta.__version__)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables in retaillink_asda_bat_uk_dv

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct UPC from retail_alert_asda_bat_uk_im.loess_forecast_baseline_unit_upc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct UPC from retail_alert_morrisons_bat_uk_im.loess_forecast_baseline_unit_upc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct UPC from retail_alert_tesco_bat_uk_im.loess_forecast_baseline_unit_upc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct *
# MAGIC from (select UPC from retail_alert_asda_bat_uk_im.loess_forecast_baseline_unit_upc union
# MAGIC select UPC from retail_alert_morrisons_bat_uk_im.loess_forecast_baseline_unit_upc union
# MAGIC select UPC from retail_alert_tesco_bat_uk_im.loess_forecast_baseline_unit_upc)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc retaillink_asda_bat_uk_dv.vw_latest_sat_epos_summary

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct retailer_item_id from retaillink_asda_bat_uk_dv.vw_latest_sat_epos_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct retailer_item_id from msd_morrisons_bat_uk_dv.vw_latest_sat_epos_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct retailer_item_id from tescopartnertoolkit_tesco_bat_uk_dv.vw_latest_sat_epos_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct retailer_item_id from retaillink_walmart_danoneusllc_us_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt >= '2023-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct retailer_item_id from retaillink_walmart_harrysinc_us_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt >= '2023-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct retailer_item_id from retaillink_walmart_rbusa_us_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt >= '2023-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct retailer_item_id from retaillink_walmart_sanofi_us_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt >= '2024-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) from retaillink_asda_bat_uk_dv.vw_latest_sat_epos_summary
# MAGIC where SALES_DT >= '2001-01-01'
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) from msd_morrisons_bat_uk_dv.vw_latest_sat_epos_summary
# MAGIC where SALES_DT >= '2001-01-01'
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) from tescopartnertoolkit_tesco_beiersdorf_uk_dv.vw_latest_sat_epos_summary
# MAGIC where SALES_DT >= '2001-01-01'
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, RETAIL_CLIENT, count(*) as Final_Num_OSA_Alerts FROM team_retail_alert_beiersdorf_drt_uk_im.alert_on_shelf_availability
# MAGIC where
# MAGIC SALES_DT >= '2024-01-01'
# MAGIC group by retail_client, SALES_DT
# MAGIC order by retail_client, SALES_DT desc

# COMMAND ----------



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
# MAGIC SELECT SALES_DT, RETAIL_CLIENT, count(*) as Final_Num_OSA_Alerts FROM team_retail_alert_bat_drt_uk_im.alert_on_shelf_availability
# MAGIC where
# MAGIC SALES_DT >= '2024-01-01'
# MAGIC group by retail_client, SALES_DT
# MAGIC order by retail_client, SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, RETAIL_CLIENT, count(*) as Final_Num_Inv_Cleanup_Alerts FROM team_retail_alert_bat_drt_uk_im.alert_inventory_cleanup
# MAGIC where
# MAGIC SALES_DT >= '2024-01-01'
# MAGIC group by retail_client, SALES_DT
# MAGIC order by retail_client, SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) as Candidate_Num_OSA_Alerts FROM retail_alert_asda_beiersdorf_uk_im.alert_on_shelf_availability
# MAGIC where SALES_DT >= '2023-10-01'
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) as Candidate_Num_OSA_Alerts FROM retail_alert_morrisons_beiersdorf_uk_im.alert_on_shelf_availability
# MAGIC where SALES_DT >= '2023-10-01'
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) as Candidate_Num_OSA_Alerts FROM retail_alert_tesco_beiersdorf_uk_im.alert_on_shelf_availability
# MAGIC where SALES_DT >= '2023-10-01'
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) as Candidate_Num_OSA_Alerts FROM retail_alert_asda_bat_uk_im.alert_on_shelf_availability
# MAGIC where SALES_DT >= '2023-10-01'
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) as Candidate_Num_OSA_Alerts FROM retail_alert_morrisons_bat_uk_im.alert_on_shelf_availability
# MAGIC where SALES_DT >= '2023-10-01'
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) as Candidate_Num_OSA_Alerts FROM retail_alert_tesco_bat_uk_im.alert_on_shelf_availability
# MAGIC where SALES_DT >= '2023-10-01'
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bobv2.vw_bobv2_caps
# MAGIC where CompanyId = 252

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bobv2.vw_bobv2_product
# MAGIC where ParentChainId = 16 and CompanyId = 252

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC select * from interventions_parameters 
# MAGIC    WHERE mdm_holding_id = 3257 -- AcostaUK
# MAGIC    AND mdm_client_id = 17781 -- BAT
# MAGIC    AND mdm_country_id = 30    -- UK

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bobv2.vw_bobv2_caps

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bobv2.vw_bobv2_caps
# MAGIC where Companyid = 437
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct productid from bobv2.product
# MAGIC where Companyid = 437
# MAGIC and IsActive = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(distinct productid) from bobv2.vw_bobv2_product
# MAGIC where Companyid = 437

# COMMAND ----------


