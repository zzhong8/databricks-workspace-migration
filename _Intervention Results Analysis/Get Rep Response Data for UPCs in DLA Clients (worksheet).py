# Databricks notebook source
# TODOS

# Add a requirement for an extract to obtain the daily number of alerts deployed for any retailer/client
# Put this into the Smart Retail requirements, ideally for Wave 2 if possible

# COMMAND ----------

import pyspark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from pyspark.sql.window import Window
from pyspark.sql.functions import count, ltrim, rtrim, col, rank, regexp_replace, desc, when

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables in acosta_retail_analytics_im

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.vw_dimension_itemlevel

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.vw_dimension_itemlevel
# MAGIC where 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.vw_fact_question_response

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc bobv2.product

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc bobv2.vw_bobv2_product

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select min(ProductId), max(ProductId) from bobv2.vw_bobv2_product

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc retaillink_walmart_sanofi_us_dv.vw_latest_sat_epos_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.interventions_retailer_client_config

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC iin.mdm_country_id,
# MAGIC iin.mdm_country_nm,
# MAGIC iin.mdm_holding_id,
# MAGIC iin.mdm_holding_nm,
# MAGIC iin.mdm_client_id,
# MAGIC iin.mdm_client_nm,
# MAGIC iin.mdm_banner_id,
# MAGIC iin.mdm_banner_nm,
# MAGIC case
# MAGIC   when iin.epos_retailer_item_id is NULL
# MAGIC   then 'No'
# MAGIC   else 'Yes'
# MAGIC end as measurable,
# MAGIC count(distinct il.upc) as num_upcs
# MAGIC from 
# MAGIC acosta_retail_analytics_im.vw_ds_intervention_input_nars iin
# MAGIC join 
# MAGIC acosta_retail_analytics_im.vw_fact_question_response fqr
# MAGIC on 
# MAGIC iin.response_id = fqr.response_id
# MAGIC join
# MAGIC acosta_retail_analytics_im.vw_dimension_itemlevel il
# MAGIC on 
# MAGIC fqr.item_level_id = il.item_level_id
# MAGIC where
# MAGIC -- iin.mdm_country_id = 1 -- US
# MAGIC -- and
# MAGIC -- iin.mdm_client_id = 16540 -- Harrys Inc
# MAGIC -- and
# MAGIC -- iin.mdm_holding_id = 71 -- Walmart
# MAGIC -- and
# MAGIC -- coalesce(iin.mdm_banner_id, -1) = -1 -- default
# MAGIC -- and
# MAGIC iin.objective_typ='Data Led Alerts'
# MAGIC and
# MAGIC iin.actionable_flg is not NULL
# MAGIC and
# MAGIC iin.call_date >= '2022-01-01'
# MAGIC group by
# MAGIC iin.mdm_country_id,
# MAGIC iin.mdm_country_nm,
# MAGIC iin.mdm_holding_id,
# MAGIC iin.mdm_holding_nm,
# MAGIC iin.mdm_client_id,
# MAGIC iin.mdm_client_nm,
# MAGIC iin.mdm_banner_id,
# MAGIC iin.mdm_banner_nm,
# MAGIC measurable
# MAGIC order by 
# MAGIC iin.mdm_country_id,
# MAGIC iin.mdm_country_nm,
# MAGIC iin.mdm_holding_id,
# MAGIC iin.mdm_holding_nm,
# MAGIC iin.mdm_client_id,
# MAGIC iin.mdm_client_nm,
# MAGIC iin.mdm_banner_id,
# MAGIC iin.mdm_banner_nm,
# MAGIC measurable

# COMMAND ----------

sql_all_360_DLA_clients = """
select distinct
  iin.mdm_country_id as mdm_country_id,
  iin.mdm_country_nm as mdm_country_nm,
  iin.mdm_holding_id as mdm_holding_id,
  iin.mdm_holding_nm as mdm_holding_nm,
  iin.mdm_client_id as mdm_client_id,
  iin.mdm_client_nm as mdm_client_nm,
  max(iin.call_date) as max_response_date
  
  from 
  acosta_retail_analytics_im.vw_ds_intervention_input_nars iin
  where
  iin.objective_typ = 'Data Led Alerts'
  
group by
  mdm_country_id,
  mdm_country_nm,
  mdm_holding_id,
  mdm_holding_nm,
  mdm_client_id,
  mdm_client_nm
  
order by 
  mdm_country_id,
  mdm_country_nm,
  mdm_holding_id,
  mdm_holding_nm,
  mdm_client_id,
  mdm_client_nm
"""

# COMMAND ----------

df_all_360_DLA_clients = spark.sql(sql_all_360_DLA_clients);

display(df_all_360_DLA_clients)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Get the count of measurable and non-measurable rep-responded UPCs for each NARS DLA client based on year-to-date call data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC iin.mdm_country_id,
# MAGIC iin.mdm_country_nm,
# MAGIC iin.mdm_holding_id,
# MAGIC iin.mdm_holding_nm,
# MAGIC iin.mdm_client_id,
# MAGIC iin.mdm_client_nm,
# MAGIC iin.mdm_banner_id,
# MAGIC iin.mdm_banner_nm,
# MAGIC case
# MAGIC   when iin.epos_retailer_item_id is NULL
# MAGIC   then 'No'
# MAGIC   else 'Yes'
# MAGIC end as measurable,
# MAGIC count(distinct il.upc) as num_upcs
# MAGIC from 
# MAGIC acosta_retail_analytics_im.vw_ds_intervention_input_nars iin
# MAGIC join 
# MAGIC acosta_retail_analytics_im.vw_fact_question_response fqr
# MAGIC on 
# MAGIC iin.response_id = fqr.response_id
# MAGIC join
# MAGIC acosta_retail_analytics_im.vw_dimension_itemlevel il
# MAGIC on 
# MAGIC fqr.item_level_id = il.item_level_id
# MAGIC where
# MAGIC -- iin.mdm_country_id = 1 -- US
# MAGIC -- and
# MAGIC -- iin.mdm_client_id = 16540 -- Harrys Inc
# MAGIC -- and
# MAGIC -- iin.mdm_holding_id = 71 -- Walmart
# MAGIC -- and
# MAGIC -- coalesce(iin.mdm_banner_id, -1) = -1 -- default
# MAGIC -- and
# MAGIC iin.objective_typ='DLA'
# MAGIC and
# MAGIC iin.actionable_flg is not NULL
# MAGIC and
# MAGIC iin.call_date >= '2022-01-01'
# MAGIC group by
# MAGIC iin.mdm_country_id,
# MAGIC iin.mdm_country_nm,
# MAGIC iin.mdm_holding_id,
# MAGIC iin.mdm_holding_nm,
# MAGIC iin.mdm_client_id,
# MAGIC iin.mdm_client_nm,
# MAGIC iin.mdm_banner_id,
# MAGIC iin.mdm_banner_nm,
# MAGIC measurable
# MAGIC order by 
# MAGIC iin.mdm_country_id,
# MAGIC iin.mdm_country_nm,
# MAGIC iin.mdm_holding_id,
# MAGIC iin.mdm_holding_nm,
# MAGIC iin.mdm_client_id,
# MAGIC iin.mdm_client_nm,
# MAGIC iin.mdm_banner_id,
# MAGIC iin.mdm_banner_nm,
# MAGIC measurable

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Get a list of all the non-measurable rep-responded products (UPCs) for all the NARS DLA clients based on year-to-date call data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct
# MAGIC iin.mdm_country_id,
# MAGIC iin.mdm_country_nm,
# MAGIC iin.mdm_holding_id,
# MAGIC iin.mdm_holding_nm,
# MAGIC iin.mdm_client_id,
# MAGIC iin.mdm_client_nm,
# MAGIC iin.mdm_banner_id,
# MAGIC iin.mdm_banner_nm,
# MAGIC il.upc
# MAGIC from 
# MAGIC acosta_retail_analytics_im.vw_ds_intervention_input_nars iin
# MAGIC join 
# MAGIC acosta_retail_analytics_im.vw_fact_question_response fqr
# MAGIC on 
# MAGIC iin.response_id = fqr.response_id
# MAGIC join
# MAGIC acosta_retail_analytics_im.vw_dimension_itemlevel il
# MAGIC on 
# MAGIC fqr.item_level_id = il.item_level_id
# MAGIC where
# MAGIC -- iin.mdm_country_id = 1 -- US
# MAGIC -- and
# MAGIC -- iin.mdm_client_id = 16540 -- Harrys Inc
# MAGIC -- and
# MAGIC -- iin.mdm_holding_id = 71 -- Walmart
# MAGIC -- and
# MAGIC -- coalesce(iin.mdm_banner_id, -1) = -1 -- default
# MAGIC -- and
# MAGIC iin.objective_typ='DLA'
# MAGIC and
# MAGIC iin.actionable_flg is not NULL
# MAGIC and
# MAGIC iin.epos_retailer_item_id is NULL
# MAGIC and
# MAGIC iin.call_date >= '2022-01-01'
# MAGIC order by 
# MAGIC iin.mdm_country_id,
# MAGIC iin.mdm_country_nm,
# MAGIC iin.mdm_holding_id,
# MAGIC iin.mdm_holding_nm,
# MAGIC iin.mdm_client_id,
# MAGIC iin.mdm_client_nm,
# MAGIC iin.mdm_banner_id,
# MAGIC iin.mdm_banner_nm,
# MAGIC il.upc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC     bp.ParentChainId,
# MAGIC     bp.CompanyId,
# MAGIC     bp.ManufacturerId,
# MAGIC     bp.ProductId,
# MAGIC
# MAGIC     hri.retailer_item_id as retailer_item_id,
# MAGIC     
# MAGIC     count(distinct vsles.sales_dt) as num_sales_days,
# MAGIC     count(distinct hou.organization_unit_num) as num_sales_stores,
# MAGIC     min(vsles.sales_dt) as min_sales_date,
# MAGIC     max(vsles.sales_dt) as max_sales_date,
# MAGIC     
# MAGIC     sum(vsles.pos_item_qty) as total_sales_units,
# MAGIC     sum(vsles.pos_amt) as total_sales_dollars,
# MAGIC     
# MAGIC     avg(vsles.pos_item_qty) as avg_sales_units,
# MAGIC     avg(vsles.pos_amt) as avg_sales_dollars
# MAGIC
# MAGIC   from
# MAGIC     tesco_nestlecore_uk_dv.vw_sat_link_epos_summary vsles
# MAGIC     
# MAGIC     join
# MAGIC     tesco_nestlecore_uk_dv.hub_retailer_item hri
# MAGIC     ON
# MAGIC     hri.hub_retailer_item_hk = vsles.hub_retailer_item_hk
# MAGIC     
# MAGIC     join
# MAGIC     tesco_nestlecore_uk_dv.hub_organization_unit hou
# MAGIC     ON 
# MAGIC     hou.hub_organization_unit_hk = vsles.hub_organization_unit_hk
# MAGIC     
# MAGIC     join 
# MAGIC     bobv2.vw_bobv2_product bp
# MAGIC     on
# MAGIC     ltrim('0', ltrim(rtrim(hri.retailer_item_id))) = ltrim('0', ltrim(rtrim(bp.RefExternal)))
# MAGIC
# MAGIC   group by
# MAGIC     bp.ParentChainId,
# MAGIC     bp.CompanyId,
# MAGIC     bp.ManufacturerId,
# MAGIC     bp.ProductId,
# MAGIC     retailer_item_id
# MAGIC
# MAGIC   order by
# MAGIC     total_sales_dollars desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 2 -- CA
# MAGIC -- and
# MAGIC -- mdm_client_id = 16540 -- Harrys Inc
# MAGIC -- and
# MAGIC -- mdm_holding_id = 2596 -- Walmart Canada
# MAGIC -- and
# MAGIC -- coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC -- and
# MAGIC -- objective_typ='DLA'
# MAGIC -- and
# MAGIC -- call_date >= '2022-01-01'
# MAGIC group by call_date
# MAGIC order by call_date
