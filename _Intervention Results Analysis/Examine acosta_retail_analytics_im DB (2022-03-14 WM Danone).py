# Databricks notebook source
from pprint import pprint

import pyspark.sql.functions as pyf

from acosta.measurement import required_columns, process_notebook_inputs

import acosta
from acosta.alerting.preprocessing import read_pos_data

print(acosta.__version__)

# COMMAND ----------

country_id = 1  # US
client_id = 882 # Danone
holding_id = 71 # Walmart
banner_id = -1  # default

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc nars_raw.dpau

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct retailer_id FROM nars_raw.dpau 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM nars_raw.dpau where retailer_id = 71

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM nars_raw.dpau where retailer_id = 2596

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_retail_analytics_im;
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 882 -- Danone
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC objective_typ='DLA'
# MAGIC and
# MAGIC call_date >= '2022-02-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# %sql

# SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# where
# mdm_country_id = 1 -- US
# and
# mdm_client_id = 882 -- Danone
# and
# mdm_holding_id = 71 -- Walmart
# and
# coalesce(mdm_banner_id, -1) = -1 -- default
# and
# objective_typ = 'DLA'
# and
# call_date >= '2022-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 882 -- Danone
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC objective_typ='DLA'
# MAGIC and
# MAGIC call_date >= '2022-02-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 882 -- Danone
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC objective_typ='DLA'
# MAGIC and
# MAGIC call_date >= '2022-02-01'
# MAGIC and
# MAGIC is_complete = 'true'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# %sql

# SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# where
# mdm_country_id = 1 -- US
# and
# mdm_client_id = 882 -- Danone
# and
# mdm_holding_id = 71 -- Walmart
# and
# coalesce(mdm_banner_id, -1) = -1 -- default
# and
# objective_typ = 'DLA'
# and
# call_date >= '2022-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM 
# MAGIC acosta_retail_analytics_im.vw_ds_intervention_input_nars vdiin 
# MAGIC left join
# MAGIC acosta_retail_analytics_im.ds_intervention_summary dis 
# MAGIC on
# MAGIC vdiin.response_id = dis.response_id
# MAGIC where
# MAGIC vdiin.mdm_country_id = 1 -- US
# MAGIC and
# MAGIC vdiin.mdm_client_id = 882 -- Danone
# MAGIC and
# MAGIC vdiin.mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(vdiin.mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC vdiin.objective_typ = 'DLA'
# MAGIC and
# MAGIC vdiin.call_date >= '2022-02-01'
# MAGIC order by vdiin.call_date, vdiin.nars_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC select Companyid, company, lkp_productgroupid, productgroupname, captype, capvalue from bobv2.vw_bobv2_caps where companyid = 577

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where mdm_country_id = 2

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct
# MAGIC mdm_country_id,
# MAGIC mdm_country_nm,
# MAGIC mdm_holding_id,
# MAGIC mdm_holding_nm,
# MAGIC mdm_banner_id,
# MAGIC mdm_banner_nm,
# MAGIC mdm_client_id,
# MAGIC mdm_client_nm,
# MAGIC epos_retailer_item_id,
# MAGIC objective_typ
# MAGIC FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where mdm_country_id = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where mdm_holding_id = 2596 -- Walmart Canada
# MAGIC and epos_retailer_item_id is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc nars_raw.dpau

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC # SELECT count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where standard_response_text = 'Initiate ISA Scan Process'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where standard_response_text = 'Initiate ISA Scan Process'

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
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct mdm_client_nm, mdm_client_id FROM acosta_retail_analytics_im.ds_intervention_summary
