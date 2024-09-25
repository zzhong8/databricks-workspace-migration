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
# MAGIC DESC acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC WEEKOFYEAR('2021-04-03'),
# MAGIC WEEKOFYEAR('2021-04-04'),
# MAGIC WEEKOFYEAR(DATE_ADD(CAST('2021-04-04' AS DATE), 1)),
# MAGIC WEEKOFYEAR('2021-04-05')

# COMMAND ----------

country_id = 1  # US
client_id = 851 # Campbells
holding_id = 91 # Kroger
banner_id = -1  # default

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * --,
# MAGIC -- MONTH(call_date, 1) AS call_month 
# MAGIC FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 91 -- Kroger
# MAGIC and
# MAGIC mdm_client_id = 851 -- Campbells
# MAGIC -- and
# MAGIC -- call_date >= '2021-11-01'
# MAGIC -- and
# MAGIC -- call_date <= '2021-11-30'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT nars_response_text, count(nars_response_text) as number_of_responses 
# MAGIC FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 91 -- Kroger
# MAGIC and
# MAGIC mdm_client_id = 851 -- Campbells
# MAGIC and
# MAGIC call_date >= '2024-01-01'
# MAGIC and
# MAGIC call_date <= '2024-01-31'
# MAGIC -- and
# MAGIC -- nars_response_text != 'Resolved on Entry, No Action Taken'
# MAGIC group by
# MAGIC nars_response_text
# MAGIC order by
# MAGIC number_of_responses
# MAGIC desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct mdm_holding_nm, mdm_holding_id FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct mdm_holding_nm, mdm_holding_id FROM acosta_retail_analytics_im.ds_intervention_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct mdm_client_nm, mdm_client_id FROM acosta_retail_analytics_im.ds_intervention_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT mdm_holding_id, mdm_holding_nm, epos_retailer_item_id, upc, count(*), min(call_date), max(call_date) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 851 -- Campbells
# MAGIC and
# MAGIC epos_retailer_item_id = 9236848
# MAGIC group by mdm_holding_id, mdm_holding_nm, epos_retailer_item_id, upc
# MAGIC order by mdm_holding_id, mdm_holding_nm, epos_retailer_item_id, upc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retaillink_walmart_campbells_us_dv.sat_retailer_item
# MAGIC where retailer_item_id = 9236848

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vendornet_meijer_campbells_us_dv.sat_retailer_item_unit_price
# MAGIC where
# MAGIC organization_unit_num = 268
# MAGIC and retailer_item_id = 477474

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vendornet_meijer_campbells_us_dv.sat_retailer_item
# MAGIC where retailer_item_desc like "%CHDR%"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vendornet_meijer_campbells_us_dv.sat_retailer_item
# MAGIC where retailer_item_desc like "%CHUNKY%POTATO%"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vendornet_meijer_campbells_us_dv.sat_retailer_item
# MAGIC where retailer_item_desc like "%CHUNKY%SOUP%"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vendornet_meijer_campbells_us_dv.sat_retailer_item
# MAGIC where retailer_item_desc like "%NOODLE%SOUP%"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 8451stratum_kroger_campbells_us_dv.sat_retailer_item
# MAGIC where retailer_item_desc like "%NOODLE%SOUP%"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 8451stratum_kroger_campbells_us_dv.sat_retailer_item
# MAGIC where retailer_item_desc like "%POTATO%SOUP%"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 8451stratum_kroger_campbells_us_dv.sat_retailer_item
# MAGIC where retailer_item_desc like "%CHUNKY%"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 851 -- Campbells
# MAGIC and
# MAGIC epos_retailer_item_id = 477474

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 851 -- Campbells
# MAGIC and
# MAGIC epos_retailer_item_id in (
# MAGIC   477474,
# MAGIC 471230,
# MAGIC 5063477,
# MAGIC 481753,
# MAGIC 4981049,
# MAGIC 5225273,
# MAGIC 485419,
# MAGIC 489434,
# MAGIC 485706
# MAGIC )
# MAGIC and total_intervention_effect > 0
# MAGIC order by epos_retailer_item_id, call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM retail_alert_meijer_campbells_us_im.alert_on_shelf_availability
# MAGIC where
# MAGIC organization_unit_num = 268
# MAGIC and
# MAGIC retailer_item_id = 477474
# MAGIC and 
# MAGIC sales_dt >= '2024-06-27'
# MAGIC and
# MAGIC sales_dt <= '2024-07-11'
# MAGIC ORDER BY sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT avg(pos_item_qty) FROM vendornet_meijer_campbells_us_dv.vw_latest_sat_epos_summary
# MAGIC where
# MAGIC organization_unit_num = 268
# MAGIC and
# MAGIC retailer_item_id = 477474
# MAGIC and 
# MAGIC sales_dt >= '2024-06-26'
# MAGIC and
# MAGIC sales_dt <= '2024-07-10'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(pos_item_qty), avg(pos_item_qty) FROM vendornet_meijer_campbells_us_dv.vw_latest_sat_epos_summary
# MAGIC where
# MAGIC organization_unit_num = 268
# MAGIC and
# MAGIC retailer_item_id = 477474

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT sales_dt, pos_item_qty, pos_amt, on_hand_inventory_qty FROM vendornet_meijer_campbells_us_dv.vw_latest_sat_epos_summary
# MAGIC where
# MAGIC organization_unit_num = 268
# MAGIC and
# MAGIC retailer_item_id = 477474
# MAGIC and 
# MAGIC sales_dt >= '2024-06-26'
# MAGIC and
# MAGIC sales_dt <= '2024-07-10'
# MAGIC ORDER BY sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM vendornet_meijer_campbells_us_dv.vw_latest_sat_epos_summary
# MAGIC where
# MAGIC organization_unit_num = 68
# MAGIC and
# MAGIC retailer_item_id = 	5225273
# MAGIC and 
# MAGIC sales_dt >= '2024-07-01'
# MAGIC and
# MAGIC sales_dt <= '2024-08-11'
# MAGIC ORDER BY sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM vendornet_meijer_campbells_us_dv.vw_latest_sat_epos_summary
# MAGIC where
# MAGIC organization_unit_num = 271
# MAGIC and
# MAGIC retailer_item_id = 	471230
# MAGIC and 
# MAGIC sales_dt >= '2024-07-28'
# MAGIC and
# MAGIC sales_dt <= '2024-08-31'
# MAGIC ORDER BY sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct retailer_item_id, retailer_item_desc from market6_kroger_campbells_us_dv.sat_retailer_item
# MAGIC where retailer_item_id in (
# MAGIC 5100001621,
# MAGIC 5100016680,
# MAGIC 5100021230,
# MAGIC 5100023262
# MAGIC )
# MAGIC order by retailer_item_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct retailer_item_id, retailer_item_desc from 8451stratum_kroger_campbells_us_dv.sat_retailer_item
# MAGIC where retailer_item_id in (
# MAGIC 5100001621,
# MAGIC 5100016680,
# MAGIC 5100021230,
# MAGIC 5100023262
# MAGIC )
# MAGIC order by retailer_item_id

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 851 -- Campbells
# MAGIC and
# MAGIC epos_retailer_item_id in (
# MAGIC 5100001621,
# MAGIC 5100016680,
# MAGIC 5100021230,
# MAGIC 5100023262
# MAGIC )
# MAGIC and total_intervention_effect > 0
# MAGIC order by epos_retailer_item_id, call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM 8451stratum_kroger_campbells_us_dv.vw_latest_sat_epos_summary
# MAGIC where
# MAGIC organization_unit_num = "528-531"	
# MAGIC and
# MAGIC retailer_item_id = 	5100001621
# MAGIC and 
# MAGIC sales_dt >= '2024-08-06'
# MAGIC and
# MAGIC sales_dt <= '2024-08-20'
# MAGIC ORDER BY sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM 8451stratum_kroger_campbells_us_dv.vw_latest_sat_epos_summary
# MAGIC where
# MAGIC organization_unit_num = "607-660"	
# MAGIC and
# MAGIC retailer_item_id = 	5100001621
# MAGIC and 
# MAGIC sales_dt >= '2024-07-22'
# MAGIC and
# MAGIC sales_dt <= '2024-08-22'
# MAGIC ORDER BY sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM market6_kroger_campbells_us_dv.vw_latest_sat_epos_summary
# MAGIC where
# MAGIC organization_unit_num = "607-660"	
# MAGIC and
# MAGIC retailer_item_id = 	5100001621
# MAGIC and 
# MAGIC sales_dt >= '2024-07-22'
# MAGIC and
# MAGIC sales_dt <= '2024-08-22'
# MAGIC ORDER BY sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM 8451stratum_kroger_campbells_us_dv.vw_latest_sat_epos_summary
# MAGIC where
# MAGIC organization_unit_num = "681-660"	
# MAGIC and
# MAGIC retailer_item_id = 	5100001621
# MAGIC and 
# MAGIC sales_dt >= '2024-07-24'
# MAGIC and
# MAGIC sales_dt <= '2024-08-21'
# MAGIC ORDER BY sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM market6_kroger_campbells_us_dv.vw_latest_sat_epos_summary
# MAGIC where
# MAGIC organization_unit_num = "681-660"	
# MAGIC and
# MAGIC retailer_item_id = 	5100001621
# MAGIC and 
# MAGIC sales_dt >= '2024-07-24'
# MAGIC and
# MAGIC sales_dt <= '2024-08-21'
# MAGIC ORDER BY sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC mdm_client_id = 851 -- Campbells
# MAGIC and
# MAGIC epos_retailer_item_id = 9236848

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 851 -- Campbells
# MAGIC and
# MAGIC epos_retailer_item_id = 9236848

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 91 -- Kroger
# MAGIC and
# MAGIC mdm_client_id = 851 -- Campbells
# MAGIC and
# MAGIC epos_retailer_item_id = 9236848

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 91 -- Kroger
# MAGIC and
# MAGIC mdm_client_id = 851 -- Campbells
# MAGIC -- and
# MAGIC -- call_date >= '2021-11-01'
# MAGIC -- and
# MAGIC -- call_date <= '2021-11-30'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT nars_response_text, count(nars_response_text) as number_of_responses FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 91 -- Kroger
# MAGIC and
# MAGIC mdm_client_id = 851 -- Campbells
# MAGIC and
# MAGIC call_date >= '2024-01-01'
# MAGIC and
# MAGIC call_date <= '2024-01-31'
# MAGIC and 
# MAGIC is_complete = 'true'
# MAGIC group by
# MAGIC nars_response_text
# MAGIC order by
# MAGIC number_of_responses
# MAGIC desc

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im1 = """
  SELECT
  mdm_country_nm,
  mdm_holding_nm,
  --mdm_banner_nm,
  mdm_client_nm,
  --mdm_country_id,
  --mdm_client_id,
  --mdm_holding_id,
  --mdm_banner_id,
  COUNT(total_intervention_effect),
  SUM(total_intervention_effect),
  --SUM(total_qintervention_effect),
  SUM(total_impact),
  --SUM(total_qimpact),
  WEEKOFYEAR(DATE_ADD(call_date, 1)) AS call_week
  FROM 
  acosta_retail_analytics_im.ds_intervention_summary
  WHERE
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  coalesce(mdm_banner_id, -1) = {banner_id} AND -- default
  (
      call_date >= '2021-01-01'
  ) AND -- calls happened between 2021-01-24 and 2021-12-31
  load_ts like '2021%' -- loaded in 2021
  GROUP BY
  mdm_country_nm,
  mdm_holding_nm,
  mdm_banner_nm,
  mdm_client_nm,
  call_week
  ORDER BY
  call_week
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)

# COMMAND ----------

df_acosta_retail_analytics_im1 = spark.sql(df_sql_query_acosta_retail_analytics_im1)
display(df_acosta_retail_analytics_im1)

# Kroger Campbells
# Results from 2021-01-31 to 2021-12-31

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im2 = """
  SELECT
*
  FROM 
  acosta_retail_analytics_im.ds_intervention_summary
  WHERE
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  coalesce(mdm_banner_id, -1) = {banner_id} AND -- default
  (
      call_date >= '2023-12-01'
  ) AND -- calls happened between 2021-01-24 and 2021-12-31
  (
      call_date <= '2024-01-31'
  )
  and
  nars_response_text like '%Display Build%'
  
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)

# COMMAND ----------

df_acosta_retail_analytics_im2 = spark.sql(df_sql_query_acosta_retail_analytics_im2)
display(df_acosta_retail_analytics_im2)

# Kroger Campbells
# Results from 2021-01-31 to 2021-12-31

# COMMAND ----------


