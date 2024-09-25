# Databricks notebook source
import pyspark.sql.functions as pyf
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime

from itertools import chain

# from sklearn.neighbors import KernelDensity

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_retail_analytics_im;
# MAGIC
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct c.calendar_key from 
# MAGIC acosta_retail_analytics_im.fact_questionresponse qr
# MAGIC join acosta_retail_analytics_im.fact_calls c on c.call_id = qr.call_id
# MAGIC join acosta_retail_analytics_im.dim_question q ON qr.question_id = q.question_id
# MAGIC where
# MAGIC q.client_id = 1992 -- Reckitt?
# MAGIC order by c.calendar_key

# COMMAND ----------

# MAGIC %sql
# MAGIC select c.calendar_key, count(c.calendar_key) from 
# MAGIC acosta_retail_analytics_im.fact_questionresponse qr
# MAGIC join acosta_retail_analytics_im.fact_calls c on c.call_id = qr.call_id
# MAGIC join acosta_retail_analytics_im.dim_question q ON qr.question_id = q.question_id
# MAGIC where
# MAGIC q.client_id = 1992 -- Reckitt?
# MAGIC group by c.calendar_key
# MAGIC order by c.calendar_key

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.vw_dimension_client

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from acosta_retail_analytics_im.vw_dimension_client
# MAGIC where country_id = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.vw_dimension_client
# MAGIC where country_id = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.vw_dimension_client
# MAGIC where country_id = 2

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(a.call_id),
# MAGIC b.channel_id,
# MAGIC b.channel_description,
# MAGIC b.holding_id,
# MAGIC b.holding_description,
# MAGIC b.country_id,
# MAGIC b.country_description,
# MAGIC -- a.call_type_id,
# MAGIC a.call_type_code,
# MAGIC a.call_type_description
# MAGIC -- a.call_type_abbreviation
# MAGIC from acosta_retail_analytics_im.fact_calls a join acosta_retail_analytics_im.vw_dimension_store b
# MAGIC on a.store_id = b.store_id
# MAGIC where a.country_id = 2
# MAGIC -- and a.date_call_executed >= '2021-01-01'
# MAGIC and a.call_completed_date >= '2021-01-01'
# MAGIC group by 
# MAGIC b.channel_id,
# MAGIC b.channel_description,
# MAGIC b.holding_id,
# MAGIC b.holding_description,
# MAGIC b.country_id,
# MAGIC b.country_description,
# MAGIC -- a.call_type_id,
# MAGIC a.call_type_code,
# MAGIC a.call_type_description
# MAGIC order by b.channel_id, b.holding_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(a.question_id, a.response_id),
# MAGIC b.channel_id,
# MAGIC b.channel_description,
# MAGIC b.holding_id,
# MAGIC b.holding_description,
# MAGIC c.client_id,
# MAGIC c.description,
# MAGIC b.country_id,
# MAGIC b.country_description
# MAGIC from acosta_retail_analytics_im.fact_questionresponse a 
# MAGIC join acosta_retail_analytics_im.vw_dimension_store b on a.store_id = b.store_id
# MAGIC join acosta_retail_analytics_im.vw_dimension_client c on a.client_id = c.client_id
# MAGIC where a.country_id = 2
# MAGIC and a.call_completed_date >= '2021-01-01'
# MAGIC group by 
# MAGIC b.channel_id,
# MAGIC b.channel_description,
# MAGIC b.holding_id,
# MAGIC b.holding_description,
# MAGIC c.client_id,
# MAGIC c.description,
# MAGIC b.country_id,
# MAGIC b.country_description
# MAGIC order by b.channel_id, b.holding_id, c.client_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(a.question_id, a.response_id),
# MAGIC b.channel_id,
# MAGIC b.channel_description,
# MAGIC b.holding_id,
# MAGIC b.holding_description,
# MAGIC c.client_id,
# MAGIC c.description,
# MAGIC b.country_id,
# MAGIC b.country_description
# MAGIC from acosta_retail_analytics_im.fact_questionresponse a 
# MAGIC join acosta_retail_analytics_im.vw_dimension_store b on a.store_id = b.store_id
# MAGIC join acosta_retail_analytics_im.vw_dimension_client c on a.client_id = c.client_id
# MAGIC join acosta_retail_analytics_im.dim_question q on a.question_id = q.question_id
# MAGIC where a.country_id = 2
# MAGIC and a.call_completed_date >= '2021-01-01'
# MAGIC and q.question_type = 'Data Led Alerts'
# MAGIC group by 
# MAGIC b.channel_id,
# MAGIC b.channel_description,
# MAGIC b.holding_id,
# MAGIC b.holding_description,
# MAGIC c.client_id,
# MAGIC c.description,
# MAGIC b.country_id,
# MAGIC b.country_description
# MAGIC order by b.channel_id, b.holding_id, c.client_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(a.question_id, a.response_id),
# MAGIC b.channel_id,
# MAGIC b.channel_description,
# MAGIC b.holding_id,
# MAGIC b.holding_description,
# MAGIC c.client_id,
# MAGIC c.description,
# MAGIC b.country_id,
# MAGIC b.country_description
# MAGIC from acosta_retail_analytics_im.fact_questionresponse a 
# MAGIC join acosta_retail_analytics_im.vw_dimension_store b on a.store_id = b.store_id
# MAGIC join acosta_retail_analytics_im.vw_dimension_client c on a.client_id = c.client_id
# MAGIC join acosta_retail_analytics_im.dim_question q on a.question_id = q.question_id
# MAGIC where a.country_id = 2
# MAGIC and a.call_completed_date >= '2021-09-01'
# MAGIC and q.question_type = 'Data Led Alerts'
# MAGIC group by 
# MAGIC b.channel_id,
# MAGIC b.channel_description,
# MAGIC b.holding_id,
# MAGIC b.holding_description,
# MAGIC c.client_id,
# MAGIC c.description,
# MAGIC b.country_id,
# MAGIC b.country_description
# MAGIC order by b.channel_id, b.holding_id, c.client_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.interventions_retailer_client_config

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Count of all responses for July 2021

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*)
# MAGIC FROM acosta_retail_analytics_im.fact_questionresponse AS qr
# MAGIC LEFT JOIN acosta_retail_analytics_im.dim_question AS q ON qr.question_id = q.question_id
# MAGIC WHERE 
# MAGIC   --item_cap_type = 3000 
# MAGIC   --item_entity_id = "PRSK"
# MAGIC   --and 
# MAGIC   qr.calendar_key BETWEEN 20210701 AND 20210731
# MAGIC   --AND call_type_code NOT IN ("PGCSCAN","PGRAM","PGRAMC","PROJECT","PGRIALERTS","Nielsen")
# MAGIC   --and response != "No Response"
# MAGIC   --and action_completed != ""
# MAGIC   --group by category_name

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Count of all responses for July 2021 that have a response

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*)
# MAGIC FROM acosta_retail_analytics_im.fact_questionresponse AS qr
# MAGIC LEFT JOIN acosta_retail_analytics_im.dim_question AS q ON qr.question_id = q.question_id
# MAGIC WHERE 
# MAGIC   --item_cap_type = 3000 
# MAGIC   --item_entity_id = "PRSK"
# MAGIC   ---and 
# MAGIC   qr.calendar_key BETWEEN 20210701 AND 20210731
# MAGIC   --AND call_type_code NOT IN ("PGCSCAN","PGRAM","PGRAMC","PROJECT","PGRIALERTS","Nielsen")
# MAGIC   and response != "No Response"
# MAGIC   --and action_completed != ""
# MAGIC   --group by category_name

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Count of all responses for July 2021 that are scoped to one item

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*)
# MAGIC FROM acosta_retail_analytics_im.fact_questionresponse AS qr
# MAGIC LEFT JOIN acosta_retail_analytics_im.dim_question AS q ON qr.question_id = q.question_id
# MAGIC WHERE 
# MAGIC   --item_cap_type = 3000 
# MAGIC   item_entity_id = "PRSK" -- CODE THAT MEANS RESPONSE IS SCOPED TO ONE ITEM 
# MAGIC   and qr.calendar_key BETWEEN 20210701 AND 20210731
# MAGIC   --AND call_type_code NOT IN ("PGCSCAN","PGRAM","PGRAMC","PROJECT","PGRIALERTS","Nielsen")
# MAGIC   --and response != "No Response"
# MAGIC   --and action_completed != ""
# MAGIC   --group by category_name

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Count of all responses for July 2021 that are scoped to one item and have a response

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*)
# MAGIC FROM acosta_retail_analytics_im.fact_questionresponse AS qr
# MAGIC LEFT JOIN acosta_retail_analytics_im.dim_question AS q ON qr.question_id = q.question_id
# MAGIC WHERE 
# MAGIC   --item_cap_type = 3000 
# MAGIC   item_entity_id = "PRSK"
# MAGIC   and qr.calendar_key BETWEEN 20210701 AND 20210731
# MAGIC   --AND call_type_code NOT IN ("PGCSCAN","PGRAM","PGRAMC","PROJECT","PGRIALERTS","Nielsen")
# MAGIC   and response != "No Response"
# MAGIC   --and action_completed != ""
# MAGIC   --group by category_name

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Count of all responses for July 2021 that are scoped to one item and have a response and are not scans, etc.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*)
# MAGIC FROM acosta_retail_analytics_im.fact_questionresponse AS qr
# MAGIC LEFT JOIN acosta_retail_analytics_im.dim_question AS q ON qr.question_id = q.question_id
# MAGIC WHERE 
# MAGIC   --item_cap_type = 3000 
# MAGIC   item_entity_id = "PRSK"
# MAGIC   and qr.calendar_key BETWEEN 20210701 AND 20210731
# MAGIC   AND call_type_code NOT IN ("PGCSCAN","PGRAM","PGRAMC","PROJECT","PGRIALERTS","Nielsen")
# MAGIC   and response != "No Response"
# MAGIC   --and action_completed != ""
# MAGIC   --group by category_name

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select q.category_name, count(*) as count
# MAGIC FROM acosta_retail_analytics_im.fact_questionresponse AS qr
# MAGIC LEFT JOIN acosta_retail_analytics_im.dim_question AS q ON qr.question_id = q.question_id
# MAGIC WHERE 
# MAGIC   --item_cap_type = 3000 
# MAGIC   item_entity_id = "PRSK"
# MAGIC   and qr.calendar_key BETWEEN 20210701 AND 20210731
# MAGIC   AND call_type_code NOT IN ("PGCSCAN","PGRAM","PGRAMC","PROJECT","PGRIALERTS","Nielsen")
# MAGIC   and response != "No Response"
# MAGIC   --and action_completed != ""
# MAGIC   group by category_name
# MAGIC   order by count desc

# COMMAND ----------

# Get infomart calls for Aug 17
query = f"""
    SELECT d
    c.call_id,
    c.call_type_description,
    c.calendar_key,
    c.date_call_executed AS call_start,
    c.call_completed_date AS call_end,
    c.call_planned_duration AS planned_duration,
    c.exec_duration,
    c.call_status,
    c.holding_id,
    
    qr.store_id,
    qr.client_id,
    qr.call_complete_status_id,
--    qr.response_type,
--    qr.last_edit_date,
--    qr.num_attachments,

    qr.item_level_id,
    qr.item_dimension_id,
    qr.item_entity_id,
    
    il.upc,
    
    qr.scan_code,
    qr.scan_datetime,
    qr.scan_code_upc,

    q.category_name,
    q.question_category,
    q.question_type,

    s.store_name,
    s.country_description,
--    s.store_address_1,
--    s.store_sqft,
    
    cl.client_id,
    cl.description,
    cl.description_short,
    
    q.question,
    qr.response,
    qr.response_value,
    qr.action_completed,
    qr.system_response,
    qr.action_required,
    s.channel_description,
    s.store_city,
    s.store_state_or_province
    
    FROM acosta_retail_analytics_im.fact_calls AS c
    
    JOIN acosta_retail_analytics_im.fact_questionresponse AS qr ON c.call_id = qr.call_id
    JOIN acosta_retail_analytics_im.dim_question AS q ON qr.question_id = q.question_id
    
    JOIN acosta_retail_analytics_im.vw_dimension_itemlevel il on qr.item_level_id = il.item_level_id
    
    JOIN acosta_retail_analytics_im.vw_dimension_store AS s on c.store_id = s.store_id
    LEFT JOIN acosta_retail_analytics_im.vw_dimension_client AS cl on qr.client_id = cl.client_id
    
--  ONE DAY ONLY
    WHERE c.calendar_key = 20210817
    AND qr.calendar_key = 20210817

--  ONE MONTH ONLY
--    WHERE c.calendar_key BETWEEN 20210701 AND 20210731
--    AND qr.calendar_key BETWEEN 20210701 AND 20210731
 
    ORDER BY call_id
    """
df = spark.sql(query)
print(f'df count = {df.cache().count():,}')

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct
# MAGIC qr.scan_code,
# MAGIC qr.scan_datetime,
# MAGIC qr.scan_code_upc
# MAGIC
# MAGIC FROM acosta_retail_analytics_im.fact_calls AS c
# MAGIC
# MAGIC JOIN acosta_retail_analytics_im.fact_questionresponse AS qr ON c.call_id = qr.call_id
# MAGIC JOIN acosta_retail_analytics_im.dim_question AS q ON qr.question_id = q.question_id
# MAGIC
# MAGIC JOIN acosta_retail_analytics_im.vw_dimension_store AS s on c.store_id = s.store_id
# MAGIC LEFT JOIN acosta_retail_analytics_im.vw_dimension_client AS cl on qr.client_id = cl.client_id
# MAGIC
# MAGIC --  ONE DAY ONLY
# MAGIC -- WHERE c.calendar_key = 20210817
# MAGIC -- AND qr.calendar_key = 20210817
# MAGIC
# MAGIC --  ONE MONTH ONLY
# MAGIC    WHERE c.calendar_key BETWEEN 20210101 AND 20211231
# MAGIC    AND qr.calendar_key BETWEEN 20210101 AND 20211231

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct
# MAGIC qr.scan_code,
# MAGIC qr.scan_datetime,
# MAGIC qr.scan_code_upc
# MAGIC
# MAGIC FROM acosta_retail_analytics_im.fact_calls AS c
# MAGIC
# MAGIC JOIN acosta_retail_analytics_im.fact_questionresponse AS qr ON c.call_id = qr.call_id
# MAGIC JOIN acosta_retail_analytics_im.dim_question AS q ON qr.question_id = q.question_id
# MAGIC
# MAGIC JOIN acosta_retail_analytics_im.vw_dimension_store AS s on c.store_id = s.store_id
# MAGIC LEFT JOIN acosta_retail_analytics_im.vw_dimension_client AS cl on qr.client_id = cl.client_id
# MAGIC
# MAGIC --  ONE DAY ONLY
# MAGIC -- WHERE c.calendar_key = 20210817
# MAGIC -- AND qr.calendar_key = 20210817
# MAGIC
# MAGIC --  ONE MONTH ONLY
# MAGIC    WHERE c.calendar_key BETWEEN 20210101 AND 20211231
# MAGIC    AND qr.calendar_key BETWEEN 20210101 AND 20211231

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(distinct
# MAGIC qr.scan_code,
# MAGIC qr.scan_datetime,
# MAGIC qr.scan_code_upc)
# MAGIC
# MAGIC FROM acosta_retail_analytics_im.fact_calls AS c
# MAGIC
# MAGIC JOIN acosta_retail_analytics_im.fact_questionresponse AS qr ON c.call_id = qr.call_id
# MAGIC JOIN acosta_retail_analytics_im.dim_question AS q ON qr.question_id = q.question_id
# MAGIC
# MAGIC JOIN acosta_retail_analytics_im.vw_dimension_store AS s on c.store_id = s.store_id
# MAGIC LEFT JOIN acosta_retail_analytics_im.vw_dimension_client AS cl on qr.client_id = cl.client_id
# MAGIC
# MAGIC --  ONE DAY ONLY
# MAGIC -- WHERE c.calendar_key = 20210817
# MAGIC -- AND qr.calendar_key = 20210817
# MAGIC
# MAGIC --  2021
# MAGIC    WHERE c.calendar_key BETWEEN 20210101 AND 20211231
# MAGIC    AND qr.calendar_key BETWEEN 20210101 AND 20211231

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(distinct
# MAGIC qr.scan_code,
# MAGIC qr.scan_datetime,
# MAGIC qr.scan_code_upc)
# MAGIC
# MAGIC FROM acosta_retail_analytics_im.fact_calls AS c
# MAGIC
# MAGIC JOIN acosta_retail_analytics_im.fact_questionresponse AS qr ON c.call_id = qr.call_id
# MAGIC JOIN acosta_retail_analytics_im.dim_question AS q ON qr.question_id = q.question_id
# MAGIC
# MAGIC JOIN acosta_retail_analytics_im.vw_dimension_store AS s on c.store_id = s.store_id
# MAGIC LEFT JOIN acosta_retail_analytics_im.vw_dimension_client AS cl on qr.client_id = cl.client_id
# MAGIC
# MAGIC --  ONE DAY ONLY
# MAGIC -- WHERE c.calendar_key = 20210817
# MAGIC -- AND qr.calendar_key = 20210817
# MAGIC
# MAGIC --  ONE MONTH ONLY
# MAGIC    WHERE c.calendar_key BETWEEN 20210801 AND 20210831
# MAGIC    AND qr.calendar_key BETWEEN 20210801 AND 20210831

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(distinct
# MAGIC qr.scan_code,
# MAGIC qr.scan_datetime,
# MAGIC qr.scan_code_upc)
# MAGIC
# MAGIC FROM acosta_retail_analytics_im.fact_calls AS c
# MAGIC
# MAGIC JOIN acosta_retail_analytics_im.fact_questionresponse AS qr ON c.call_id = qr.call_id
# MAGIC JOIN acosta_retail_analytics_im.dim_question AS q ON qr.question_id = q.question_id
# MAGIC
# MAGIC JOIN acosta_retail_analytics_im.vw_dimension_store AS s on c.store_id = s.store_id
# MAGIC LEFT JOIN acosta_retail_analytics_im.vw_dimension_client AS cl on qr.client_id = cl.client_id
# MAGIC
# MAGIC --  ONE DAY ONLY
# MAGIC WHERE c.calendar_key = 20210817
# MAGIC AND qr.calendar_key = 20210817
# MAGIC
# MAGIC --  ONE MONTH ONLY
# MAGIC --    WHERE c.calendar_key BETWEEN 20210801 AND 20210831
# MAGIC --    AND qr.calendar_key BETWEEN 20210801 AND 20210831

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(distinct
# MAGIC     cl.client_id,
# MAGIC     cl.description,
# MAGIC     cl.description_short,
# MAGIC     cl.principal_id,
# MAGIC     cl.national_client_indicator,
# MAGIC     cl.master_client_id,
# MAGIC     cl.master_client_description,
# MAGIC     cl.ad_group_name,
# MAGIC     cl.rsl_filter_percent,
# MAGIC     cl.tdlinx_flag,
# MAGIC     cl.nielsen_sla_flag,
# MAGIC     cl.active,
# MAGIC     cl.insert_datetime,
# MAGIC     cl.update_datetime,
# MAGIC     cl.status,
# MAGIC     cl.country_id
# MAGIC )
# MAGIC
# MAGIC FROM acosta_retail_analytics_im.fact_calls AS c
# MAGIC
# MAGIC JOIN acosta_retail_analytics_im.fact_questionresponse AS qr ON c.call_id = qr.call_id
# MAGIC JOIN acosta_retail_analytics_im.dim_question AS q ON qr.question_id = q.question_id
# MAGIC
# MAGIC JOIN acosta_retail_analytics_im.vw_dimension_store AS s on c.store_id = s.store_id
# MAGIC JOIN acosta_retail_analytics_im.vw_dimension_client AS cl on qr.client_id = cl.client_id
# MAGIC
# MAGIC --  ONE DAY ONLY
# MAGIC -- WHERE c.calendar_key = 20210817
# MAGIC -- AND qr.calendar_key = 20210817
# MAGIC
# MAGIC --  2021
# MAGIC WHERE c.calendar_key BETWEEN 20210101 AND 20211231
# MAGIC AND qr.calendar_key BETWEEN 20210101 AND 20211231
# MAGIC AND cl.client_id > 0

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct
# MAGIC     cl.client_id,
# MAGIC     cl.description,
# MAGIC     cl.description_short,
# MAGIC     cl.principal_id,
# MAGIC     cl.national_client_indicator,
# MAGIC     cl.master_client_id,
# MAGIC     cl.master_client_description,
# MAGIC     cl.ad_group_name,
# MAGIC     cl.rsl_filter_percent,
# MAGIC     cl.tdlinx_flag,
# MAGIC     cl.nielsen_sla_flag,
# MAGIC     cl.active,
# MAGIC     cl.insert_datetime,
# MAGIC     cl.update_datetime,
# MAGIC     cl.status,
# MAGIC     cl.country_id
# MAGIC
# MAGIC FROM acosta_retail_analytics_im.fact_calls AS c
# MAGIC
# MAGIC JOIN acosta_retail_analytics_im.fact_questionresponse AS qr ON c.call_id = qr.call_id
# MAGIC JOIN acosta_retail_analytics_im.dim_question AS q ON qr.question_id = q.question_id
# MAGIC
# MAGIC JOIN acosta_retail_analytics_im.vw_dimension_store AS s on c.store_id = s.store_id
# MAGIC JOIN acosta_retail_analytics_im.vw_dimension_client AS cl on qr.client_id = cl.client_id
# MAGIC
# MAGIC --  ONE DAY ONLY
# MAGIC -- WHERE c.calendar_key = 20210817
# MAGIC -- AND qr.calendar_key = 20210817
# MAGIC
# MAGIC --  2021
# MAGIC WHERE c.calendar_key BETWEEN 20210101 AND 20211231
# MAGIC AND qr.calendar_key BETWEEN 20210101 AND 20211231
# MAGIC AND cl.client_id > 0

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(distinct
# MAGIC     cl.client_id,
# MAGIC     cl.description,
# MAGIC     cl.description_short)
# MAGIC
# MAGIC FROM acosta_retail_analytics_im.fact_calls AS c
# MAGIC
# MAGIC JOIN acosta_retail_analytics_im.fact_questionresponse AS qr ON c.call_id = qr.call_id
# MAGIC JOIN acosta_retail_analytics_im.dim_question AS q ON qr.question_id = q.question_id
# MAGIC
# MAGIC JOIN acosta_retail_analytics_im.vw_dimension_store AS s on c.store_id = s.store_id
# MAGIC LEFT JOIN acosta_retail_analytics_im.vw_dimension_client AS cl on qr.client_id = cl.client_id
# MAGIC
# MAGIC --  ONE DAY ONLY
# MAGIC -- WHERE c.calendar_key = 20210817
# MAGIC -- AND qr.calendar_key = 20210817
# MAGIC
# MAGIC --  2021
# MAGIC    WHERE c.calendar_key BETWEEN 20210101 AND 20211231
# MAGIC    AND qr.calendar_key BETWEEN 20210101 AND 20211231
# MAGIC    AND cl.client_id > 0

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct
# MAGIC     cl.client_id,
# MAGIC     cl.description,
# MAGIC     cl.description_short
# MAGIC
# MAGIC FROM acosta_retail_analytics_im.fact_calls AS c
# MAGIC
# MAGIC JOIN acosta_retail_analytics_im.fact_questionresponse AS qr ON c.call_id = qr.call_id
# MAGIC JOIN acosta_retail_analytics_im.dim_question AS q ON qr.question_id = q.question_id
# MAGIC
# MAGIC JOIN acosta_retail_analytics_im.vw_dimension_store AS s on c.store_id = s.store_id
# MAGIC LEFT JOIN acosta_retail_analytics_im.vw_dimension_client AS cl on qr.client_id = cl.client_id
# MAGIC
# MAGIC --  ONE DAY ONLY
# MAGIC -- WHERE c.calendar_key = 20210817
# MAGIC -- AND qr.calendar_key = 20210817
# MAGIC
# MAGIC --  2021
# MAGIC    WHERE c.calendar_key BETWEEN 20210101 AND 20211231
# MAGIC    AND qr.calendar_key BETWEEN 20210101 AND 20211231
# MAGIC    AND cl.client_id > 0

# COMMAND ----------

# MAGIC %sql
# MAGIC desc acosta_retail_analytics_im.fact_calls

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acosta_retail_analytics_im.fact_calls c
# MAGIC where
# MAGIC c.country_id = 30 -- UK
# MAGIC and
# MAGIC c.holding_id = 3257 -- AcostaRetailUK

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct country_id from acosta_retail_analytics_im.fact_questionresponse

# COMMAND ----------

# MAGIC %sql
# MAGIC desc acosta_retail_analytics_im.fact_questionresponse

# COMMAND ----------

# MAGIC %sql
# MAGIC desc acosta_retail_analytics_im.dim_question

# COMMAND ----------

# MAGIC %sql
# MAGIC desc acosta_retail_analytics_im.vw_dimension_store

# COMMAND ----------

# MAGIC %sql
# MAGIC desc acosta_retail_analytics_im.vw_dimension_client

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acosta_retail_analytics_im.vw_dimension_client
# MAGIC where country_id = 30

# COMMAND ----------

mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
coalesce(mdm_banner_id, -1) = 7746 -- Tesco

# COMMAND ----------

# MAGIC %sql
# MAGIC desc acosta_retail_analytics_im.vw_dimension_itemlevel

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acosta_retail_analytics_im.vw_dimension_itemlevel

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct
# MAGIC item_entity_id,
# MAGIC item_entity_description
# MAGIC from acosta_retail_analytics_im.vw_dimension_itemlevel

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acosta_retail_analytics_im.vw_dimension_itemlevel
# MAGIC where item_entity_id = 'PRSK'
# MAGIC and client_id = 13429

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT distinct
# MAGIC VSLES.RETAILER_ITEM_ID
# MAGIC FROM retaillink_walmart_nestlewaters_us_dv.vw_latest_sat_epos_summary VSLES

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select * from vw_BOBv2_Product  
# MAGIC where CompanyId = 567 -- WM Nestlewaters

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC desc acosta_retail_analytics_im.vw_dimension_product

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acosta_retail_analytics_im.vw_dimension_product
# MAGIC where f.item_entity_id = 'PRSK'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acosta_retail_analytics_im.dim_question AS q

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acosta_retail_analytics_im.dim_question
# MAGIC where question_type = 'Data Led Alerts'
# MAGIC and client_id = 882
# MAGIC and question_start_date >= '2021-07-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC q.question_id,
# MAGIC q.question,
# MAGIC q.category_name,
# MAGIC q.question_group,
# MAGIC q.question_category,
# MAGIC q.question_start_date,
# MAGIC q.question_end_date,
# MAGIC q.question_status,
# MAGIC q.question_type,
# MAGIC q.response_type,
# MAGIC q.priority,
# MAGIC q.client_id,
# MAGIC q.active,
# MAGIC q.status,
# MAGIC q.nars_source,
# MAGIC
# MAGIC qr.store_id,
# MAGIC qr.store_tdlinx_id,
# MAGIC qr.client_id,
# MAGIC qr.call_complete_status_id,
# MAGIC qr.call_completed_date,
# MAGIC
# MAGIC qr.item_level_id,
# MAGIC qr.item_dimension_id,
# MAGIC qr.item_entity_id,
# MAGIC
# MAGIC il.upc,
# MAGIC
# MAGIC qr.response,
# MAGIC qr.response_value,
# MAGIC qr.action_completed,
# MAGIC qr.system_response,
# MAGIC qr.action_required
# MAGIC
# MAGIC from acosta_retail_analytics_im.dim_question q
# MAGIC JOIN acosta_retail_analytics_im.fact_questionresponse qr ON qr.question_id = q.question_id
# MAGIC JOIN acosta_retail_analytics_im.vw_dimension_itemlevel il on qr.item_level_id = il.item_level_id
# MAGIC where q.question_type = 'Data Led Alerts'
# MAGIC and q.client_id = 882
# MAGIC and q.question_start_date >= '2021-07-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC select p.department_id, p.department_description, count(p.item_id) from acosta_retail_analytics_im.vw_dimension_product as p
# MAGIC JOIN acosta_retail_analytics_im.fact_questionresponse as qr on p.department_id = qr.item_dimension_id
# MAGIC join acosta_retail_analytics_im.fact_calls AS c on c.call_id = qr.call_id
# MAGIC where qr.item_entity_id = 'PRDE'
# MAGIC and p.client_id = 869
# MAGIC and p.holding_id = 71
# MAGIC and c.call_type_code like '%DRT%'
# MAGIC group by p.department_id, p.department_description
# MAGIC order by p.department_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select p.department_id, p.department_description, count(distinct p.item_id) from acosta_retail_analytics_im.vw_dimension_product as p
# MAGIC JOIN acosta_retail_analytics_im.fact_questionresponse as qr on p.department_id = qr.item_dimension_id
# MAGIC join acosta_retail_analytics_im.fact_call s AS c on c.call_id = qr.call_id
# MAGIC where qr.item_entity_id = 'PRDE'
# MAGIC and p.client_id = 869
# MAGIC and p.holding_id = 71
# MAGIC and c.call_type_code like '%DRT%'
# MAGIC group by p.department_id, p.department_description
# MAGIC order by p.department_id

# COMMAND ----------

# MAGIC %md
# MAGIC #### J&J DRT

# COMMAND ----------

query = "SELECT client_id, description FROM acosta_retail_analytics_im.vw_dimension_client"
df_companies = spark.sql(query)

df_jj = df_companies[df_companies['description'].contains('J&J')]
display(df_jj)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT c.call_type_code, qr.client_id, c.holding_id, dc.description --, count(c.call_type_code)
# MAGIC FROM acosta_retail_analytics_im.fact_calls AS c
# MAGIC JOIN acosta_retail_analytics_im.fact_questionresponse AS qr ON c.call_id = qr.call_id
# MAGIC JOIN acosta_retail_analytics_im.vw_dimension_client AS dc ON qr.client_id = dc.client_id
# MAGIC --WHERE c.holding_id = 71
# MAGIC WHERE dc.description LIKE "%J&J%"
# MAGIC -- AND qr.client_id = 869
# MAGIC --AND c.call_type_code LIKE '%DRT%'
# MAGIC -- GROUP BY c.call_type_code, qr.client_id, c.holding_id
# MAGIC --display(df.groupBy('item_entity_id').count().orderBy('count', ascending=False).collect())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT c.call_type_code, qr.client_id, c.holding_id, dc.description --, count(c.call_type_code)
# MAGIC FROM acosta_retail_analytics_im.fact_calls AS c
# MAGIC JOIN acosta_retail_analytics_im.fact_questionresponse AS qr ON c.call_id = qr.call_id
# MAGIC JOIN acosta_retail_analytics_im.vw_dimension_client AS dc ON qr.client_id = dc.client_id
# MAGIC WHERE c.holding_id = 71
# MAGIC and dc.description LIKE "%ohnson%"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT c.call_type_code, qr.client_id, c.holding_id, dc.description, count(c.call_type_code)
# MAGIC FROM acosta_retail_analytics_im.fact_calls AS c
# MAGIC JOIN acosta_retail_analytics_im.fact_questionresponse AS qr ON c.call_id = qr.call_id
# MAGIC JOIN acosta_retail_analytics_im.vw_dimension_client AS dc ON qr.client_id = dc.client_id
# MAGIC WHERE c.holding_id = 71
# MAGIC and c.call_type_code LIKE "%DRT%"

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select p.department_id, p.department_description, count(p.item_id) from acosta_retail_analytics_im.vw_dimension_product as p
# MAGIC JOIN acosta_retail_analytics_im.fact_questionresponse as qr on p.department_id = qr.item_dimension_id
# MAGIC join acosta_retail_analytics_im.fact_calls AS c on c.call_id = qr.call_id
# MAGIC where qr.item_entity_id = 'PRDE'
# MAGIC and p.client_id = 869
# MAGIC and p.holding_id = 71
# MAGIC and c.call_type_code like '%DRT%'
# MAGIC group by p.department_id, p.department_description
# MAGIC order by p.department_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select p.department_id, p.department_description, count(distinct p.item_id) from acosta_retail_analytics_im.vw_dimension_product as p
# MAGIC JOIN acosta_retail_analytics_im.fact_questionresponse as qr on p.department_id = qr.item_dimension_id
# MAGIC join acosta_retail_analytics_im.fact_calls as c on c.call_id = qr.call_id
# MAGIC where qr.item_entity_id = 'PRDE'
# MAGIC and p.client_id = 869
# MAGIC and p.holding_id = 71
# MAGIC and c.call_type_code like '%DRT%'
# MAGIC group by p.department_id, p.department_description
# MAGIC order by p.department_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select p.department_id, p.department_description, count(p.item_id) from acosta_retail_analytics_im.vw_dimension_product as p
# MAGIC JOIN acosta_retail_analytics_im.fact_questionresponse as qr on p.department_id = qr.item_dimension_id
# MAGIC join acosta_retail_analytics_im.fact_calls as c on c.call_id = qr.call_id
# MAGIC where qr.item_entity_id = 'PRDE'
# MAGIC and p.client_id = 12407
# MAGIC and p.holding_id = 71
# MAGIC and c.call_type_code like '%DRT%'
# MAGIC group by p.department_id, p.department_description
# MAGIC order by p.department_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select p.department_id, p.department_description, count(distinct p.item_id) from acosta_retail_analytics_im.vw_dimension_product as p
# MAGIC JOIN acosta_retail_analytics_im.fact_questionresponse as qr on p.department_id = qr.item_dimension_id
# MAGIC join acosta_retail_analytics_im.fact_calls as c on c.call_id = qr.call_id
# MAGIC where qr.item_entity_id = 'PRDE'
# MAGIC and p.client_id = 12407
# MAGIC and p.holding_id = 71
# MAGIC and c.call_type_code like '%DRT%'
# MAGIC group by p.department_id, p.department_description
# MAGIC order by p.department_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct c.*, q.question, qr.response from 
# MAGIC acosta_retail_analytics_im.fact_questionresponse qr
# MAGIC join acosta_retail_analytics_im.fact_calls c on c.call_id = qr.call_id
# MAGIC join acosta_retail_analytics_im.dim_question q ON qr.question_id = q.question_id
# MAGIC where
# MAGIC c.holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC qr.response = 'Gap Fill'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct c.call_type_id, c.call_type_code, q.question, qr.response from 
# MAGIC acosta_retail_analytics_im.fact_questionresponse qr
# MAGIC join acosta_retail_analytics_im.fact_calls c on c.call_id = qr.call_id
# MAGIC join acosta_retail_analytics_im.dim_question q ON qr.question_id = q.question_id
# MAGIC where
# MAGIC c.holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC qr.response = 'Gap Fill'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct c.call_type_id, c.call_type_code, q.question, qr.response from 
# MAGIC acosta_retail_analytics_im.fact_questionresponse qr
# MAGIC join acosta_retail_analytics_im.fact_calls c on c.call_id = qr.call_id
# MAGIC join acosta_retail_analytics_im.dim_question q ON qr.question_id = q.question_id
# MAGIC where
# MAGIC c.holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC qr.response = 'Gap Fill'
# MAGIC and
# MAGIC q.question = 'Intervention Made'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct c.calendar_key, c.call_type_id, c.call_type_code, q.question, qr.response from 
# MAGIC acosta_retail_analytics_im.fact_questionresponse qr
# MAGIC join acosta_retail_analytics_im.fact_calls c on c.call_id = qr.call_id
# MAGIC join acosta_retail_analytics_im.dim_question q ON qr.question_id = q.question_id
# MAGIC where
# MAGIC c.holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC qr.response = 'Gap Fill'
# MAGIC and
# MAGIC q.question = 'Intervention Made'
# MAGIC order by
# MAGIC c.calendar_key

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct c.call_type_id, c.call_type_code from 
# MAGIC acosta_retail_analytics_im.fact_questionresponse qr
# MAGIC join acosta_retail_analytics_im.fact_calls c on c.call_id = qr.call_id
# MAGIC join acosta_retail_analytics_im.dim_question q ON qr.question_id = q.question_id
# MAGIC where
# MAGIC c.holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC qr.response = 'Gap Fill'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct c.*, q.question, qr.response from 
# MAGIC acosta_retail_analytics_im.fact_questionresponse qr
# MAGIC join acosta_retail_analytics_im.fact_calls c on c.call_id = qr.call_id
# MAGIC join acosta_retail_analytics_im.dim_question q ON qr.question_id = q.question_id
# MAGIC where
# MAGIC c.holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC q.question = 'Intervention Made' -- UK Opportunities?

# COMMAND ----------

# %sql
# select distinct c.calendar_key from 
# acosta_retail_analytics_im.fact_questionresponse qr
# join acosta_retail_analytics_im.fact_calls c on c.call_id = qr.call_id
# join acosta_retail_analytics_im.dim_question q ON qr.question_id = q.question_id
# where
# c.client_id = 1992 -- Reckitt??
# order by c.calendar_key

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct c.calendar_key from 
# MAGIC acosta_retail_analytics_im.fact_questionresponse qr
# MAGIC join acosta_retail_analytics_im.fact_calls c on c.call_id = qr.call_id
# MAGIC join acosta_retail_analytics_im.dim_question q ON qr.question_id = q.question_id
# MAGIC where
# MAGIC c.holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC q.question = 'Intervention Made' -- UK Opportunities?
# MAGIC order by c.calendar_key

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct c.*, q.question, qr.response from 
# MAGIC acosta_retail_analytics_im.fact_questionresponse qr
# MAGIC join acosta_retail_analytics_im.fact_calls c on c.call_id = qr.call_id
# MAGIC join acosta_retail_analytics_im.dim_question q ON qr.question_id = q.question_id
# MAGIC where
# MAGIC c.holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC c.call_type_code = 'STND'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct c.calendar_key from 
# MAGIC acosta_retail_analytics_im.fact_questionresponse qr
# MAGIC join acosta_retail_analytics_im.fact_calls c on c.call_id = qr.call_id
# MAGIC join acosta_retail_analytics_im.dim_question q ON qr.question_id = q.question_id
# MAGIC where
# MAGIC q.client_id = 1992 -- Reckitt?
# MAGIC order by c.calendar_key

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct c.calendar_key from 
# MAGIC acosta_retail_analytics_im.fact_questionresponse qr
# MAGIC join acosta_retail_analytics_im.fact_calls c on c.call_id = qr.call_id
# MAGIC join acosta_retail_analytics_im.dim_question q ON qr.question_id = q.question_id
# MAGIC where
# MAGIC c.holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC c.call_type_code = 'STND'
# MAGIC order by c.calendar_key

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC q.question_id,
# MAGIC q.question,
# MAGIC q.category_name,
# MAGIC q.question_group,
# MAGIC q.question_category,
# MAGIC q.question_start_date,
# MAGIC q.question_end_date,
# MAGIC q.question_status,
# MAGIC q.question_type,
# MAGIC q.response_type,
# MAGIC q.priority,
# MAGIC q.client_id,
# MAGIC q.active,
# MAGIC q.status,
# MAGIC q.nars_source,
# MAGIC
# MAGIC qr.store_id,
# MAGIC qr.store_tdlinx_id,
# MAGIC qr.client_id,
# MAGIC qr.call_complete_status_id,
# MAGIC qr.call_completed_date,
# MAGIC
# MAGIC qr.item_level_id,
# MAGIC qr.item_dimension_id,
# MAGIC qr.item_entity_id,
# MAGIC
# MAGIC il.upc,
# MAGIC
# MAGIC qr.response,
# MAGIC qr.response_value,
# MAGIC qr.action_completed,
# MAGIC qr.system_response,
# MAGIC qr.action_required
# MAGIC
# MAGIC from acosta_retail_analytics_im.dim_question q
# MAGIC JOIN acosta_retail_analytics_im.fact_questionresponse qr ON qr.question_id = q.question_id
# MAGIC JOIN acosta_retail_analytics_im.vw_dimension_itemlevel il on qr.item_level_id = il.item_level_id
# MAGIC where q.question_type = 'Data Led Alerts'
# MAGIC and q.question_start_date >= '2021-07-01'
# MAGIC and qr.client_id = 882 -- Danone
# MAGIC and qr.holding_id = 91 -- Kroger

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acosta_retail_analytics_im.interventions_retailer_client_config

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acosta_retail_analytics_im.interventions_retailer_client_config
# MAGIC where
# MAGIC mdm_country_id = 30

# COMMAND ----------

# MAGIC %sql
# MAGIC desc acosta_retail_analytics_im.dim_question

# COMMAND ----------

# MAGIC %sql
# MAGIC desc acosta_retail_analytics_im.fact_questionresponse

# COMMAND ----------

# Get infomart calls for Aug 17
query = f"""
    SELECT
    c.call_id,
    c.call_type_description,
    c.calendar_key,
    c.date_call_executed AS call_start,
    c.call_completed_date AS call_end,
    c.call_planned_duration AS planned_duration,
    c.exec_duration,
    c.call_status,
    c.holding_id,
    
    qr.store_id,
    qr.client_id,
    qr.call_complete_status_id,
    qr.response_type,
    qr.last_edit_date,
    qr.num_attachments,
    qr.priority,

    qr.item_level_id,
    qr.item_dimension_id,
    qr.item_entity_id,
    
    il.upc,
    
    qr.scan_code,
    qr.scan_datetime,
    qr.scan_code_upc,

    q.category_name,
    q.question_category,
    q.question_type,

    s.store_name,
    s.country_description,
--    s.store_address_1,
--    s.store_sqft,
    
    cl.client_id,
    cl.description,
    cl.description_short,
    
    q.question,
    qr.response,
    qr.response_value,
    qr.action_completed,
    qr.system_response,
    qr.action_required,
    s.channel_description,
    s.store_city,
    s.store_state_or_province
    
    FROM acosta_retail_analytics_im.fact_calls AS c
    
    JOIN acosta_retail_analytics_im.fact_questionresponse AS qr ON c.call_id = qr.call_id
    JOIN acosta_retail_analytics_im.dim_question AS q ON qr.question_id = q.question_id
    
    JOIN acosta_retail_analytics_im.vw_dimension_itemlevel il on qr.item_level_id = il.item_level_id
    
    JOIN acosta_retail_analytics_im.vw_dimension_store AS s on c.store_id = s.store_id
    LEFT JOIN acosta_retail_analytics_im.vw_dimension_client AS cl on qr.client_id = cl.client_id
    
--  ONE DAY ONLY
    WHERE c.calendar_key = 20210817
    AND qr.calendar_key = 20210817

--  ONE MONTH ONLY
--    WHERE c.calendar_key BETWEEN 20210701 AND 20210731
--    AND qr.calendar_key BETWEEN 20210701 AND 20210731
 
    ORDER BY call_id
    """
df = spark.sql(query)
print(f'df count = {df.cache().count():,}')

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select a.priority, count(a.priority)
# MAGIC from acosta_retail_analytics_im.fact_questionresponse a
# MAGIC where a.calendar_key = 20210817
# MAGIC group by a.priority

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select a.priority, count(a.priority)
# MAGIC from acosta_retail_analytics_im.fact_questionresponse a
# MAGIC where a.calendar_key >= 20210101
# MAGIC group by a.priority

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select q.question_category, qr.priority, count(qr.priority)
# MAGIC from acosta_retail_analytics_im.dim_question q
# MAGIC JOIN acosta_retail_analytics_im.fact_questionresponse qr ON qr.question_id = q.question_id
# MAGIC where qr.calendar_key >= '20210101'
# MAGIC group by q.question_category, qr.priority

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select q.question_category, q.priority, count(q.priority)
# MAGIC from acosta_retail_analytics_im.dim_question q
# MAGIC where q.question_start_date >= '2021-01-01'
# MAGIC group by q.question_category, q.priority

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select q.question_category, q.question_type, qr.priority, count(qr.priority)
# MAGIC from acosta_retail_analytics_im.dim_question q
# MAGIC JOIN acosta_retail_analytics_im.fact_questionresponse qr ON qr.question_id = q.question_id
# MAGIC where qr.calendar_key >= '20210101'
# MAGIC group by q.question_category, q.question_type, qr.priority

# COMMAND ----------

# MAGIC %md
# MAGIC #### Walmart Syndicated

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct c.channel_id, c.call_type_code, c.call_type_description, c.call_type_abbreviation, count(call_type_code) from acosta_retail_analytics_im.fact_calls c
# MAGIC where c.holding_id = 71 -- Walmart
# MAGIC and c.country_id = 1 -- US
# MAGIC group by c.channel_id, c.call_type_code, c.call_type_description, c.call_type_abbreviation

# COMMAND ----------

# MAGIC %sql
# MAGIC desc acosta_retail_analytics_im.dim_retail_team

# COMMAND ----------

# MAGIC %sql
# MAGIC desc acosta_retail_analytics_im.dim_team

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acosta_retail_analytics_im.dim_team

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 
# MAGIC (
# MAGIC     select distinct 
# MAGIC     c.team_executed_code,
# MAGIC     c.team_planned_code,
# MAGIC     count(c.team_executed_code) as num_calls_in_2021
# MAGIC     from acosta_retail_analytics_im.fact_calls c
# MAGIC     where c.holding_id = 71 -- Walmart
# MAGIC     and c.country_id = 1 -- US
# MAGIC     and c.call_completed_date >= '2021-01-01'
# MAGIC     group by
# MAGIC     c.team_executed_code,
# MAGIC     c.team_planned_code
# MAGIC )
# MAGIC where num_calls_in_2021 > 0
# MAGIC order by num_calls_in_2021 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.vw_dimension_profile

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.vw_dimension_profile

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.vw_fact_question_response

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.vw_fact_calls

# COMMAND ----------

# MAGIC %sql
# MAGIC desc acosta_retail_analytics_im.fact_questionresponse

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.vw_dimension_client

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.vw_dimension_client where description like '%Reckitt%'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables from acosta_retail_analytics_im

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.interventions_retailer_client_config
# MAGIC where mdm_holding_id = 71

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from BOBV2.vw_bobv2_caps
# MAGIC -- where Company_id = '627'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from BOBV2.vw_bobv2_caps
# MAGIC where CompanyId = '590'

# COMMAND ----------


