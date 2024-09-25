# Databricks notebook source
from pprint import pprint

import pyspark.sql.functions as pyf

from acosta.measurement import required_columns, process_notebook_inputs

import acosta
from acosta.alerting.preprocessing import read_pos_data

print(acosta.__version__)

# COMMAND ----------

country_id = 1  # US
client_id = 869 # Clorox
holding_id = 71 # Walmart
banner_id = -1  # default

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_alert_walmart_clorox_us_im.alert_on_shelf_availability

# COMMAND ----------

# MAGIC %sql
# MAGIC select sales_dt, count(*) from retail_alert_walmart_clorox_us_im.alert_on_shelf_availability
# MAGIC group by sales_dt
# MAGIC order by sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct client_id FROM nars_raw.dpau 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM nars_raw.dpau 
# MAGIC where retailer_id = 71
# MAGIC and client_id = 869

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM nars_raw.dpau 
# MAGIC where retailer_id = 71
# MAGIC and client_id = 869
# MAGIC and call_date >= '2022-07-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_retail_analytics_im;
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC   FROM acosta_retail_analytics_im.interventions_retailer_client_config 
# MAGIC order by
# MAGIC active_flg desc,
# MAGIC mdm_country_id,
# MAGIC mdm_holding_id,
# MAGIC mdm_client_id,
# MAGIC mdm_banner_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC   FROM acosta_retail_analytics_im.interventions_response_mapping
# MAGIC order by
# MAGIC mdm_country_id,
# MAGIC objective_typ,
# MAGIC lower(standard_response_text)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC   FROM acosta_retail_analytics_im.interventions_response_mapping
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC order by
# MAGIC objective_typ,
# MAGIC lower(standard_response_text)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 869 -- Clorox
# MAGIC -- and
# MAGIC -- mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC order by
# MAGIC lower(standard_response_text)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_dla
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 869 -- Clorox
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC -- call_date >= '2022-07-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 869 -- Clorox
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC -- call_date >= '2022-07-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 882 -- Danone
# MAGIC and
# MAGIC mdm_holding_id = 91 -- Kroger
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC standard_response_text = "Opportunity found, unable to resolve"
# MAGIC and
# MAGIC call_date >= '2022-07-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 882 -- Danone
# MAGIC and
# MAGIC mdm_holding_id = 91 -- Kroger
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC call_date >= '2022-07-01'
# MAGIC group by call_date
# MAGIC order by call_date

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
# MAGIC call_date >= '2022-02-01'
# MAGIC group by call_date
# MAGIC order by call_date

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
# MAGIC call_date >= '2022-02-01'
# MAGIC group by call_date
# MAGIC order by call_date

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
# MAGIC epos_retailer_item_id is not NULL
# MAGIC and
# MAGIC actionable_flg is not NULL
# MAGIC and
# MAGIC call_date >= '2022-02-01'
# MAGIC group by call_date
# MAGIC order by call_date

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
# MAGIC objective_typ='Data Led Alerts'
# MAGIC and
# MAGIC epos_retailer_item_id is NULL
# MAGIC and
# MAGIC actionable_flg is not NULL
# MAGIC and
# MAGIC call_date >= '2022-02-01'
# MAGIC group by call_date
# MAGIC order by call_date

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
# MAGIC objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC epos_retailer_item_id is not NULL
# MAGIC and
# MAGIC actionable_flg is not NULL
# MAGIC and
# MAGIC call_date >= '2022-02-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 882 -- Danone
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC epos_retailer_item_id is not NULL
# MAGIC and
# MAGIC actionable_flg is not NULL
# MAGIC and
# MAGIC call_date >= '2022-06-11'

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
# MAGIC objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC call_date >= '2022-02-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 882 -- Danone
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC call_date >= '2022-07-10'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 882 -- Danone
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC objective_typ = 'DLA'
# MAGIC and
# MAGIC call_date >= '2022-01-01'

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
# MAGIC SELECT count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
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

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 882 -- Danone
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC call_date >= '2022-07-10'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct
# MAGIC   dis.mdm_country_id as mdm_country_id,
# MAGIC   dis.mdm_country_nm as mdm_country_nm,
# MAGIC   dis.mdm_holding_id as mdm_holding_id,
# MAGIC   dis.mdm_holding_nm as mdm_holding_nm,
# MAGIC   dis.mdm_client_id as mdm_client_id,
# MAGIC   dis.mdm_client_nm as mdm_client_nm,
# MAGIC   dis.mdm_banner_id as mdm_banner_id,
# MAGIC   dis.mdm_banner_nm as mdm_banner_nm
# MAGIC from
# MAGIC   acosta_retail_analytics_im.ds_intervention_summary dis
# MAGIC where
# MAGIC   call_date >= '2022-07-16'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *
# MAGIC from
# MAGIC   acosta_retail_analytics_im.ds_intervention_summary dis
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 9663 -- Barilla
# MAGIC and
# MAGIC mdm_holding_id = 91 -- Kroger
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC call_date >= '2022-07-17'

# COMMAND ----------


