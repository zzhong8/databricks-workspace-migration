# Databricks notebook source
from pprint import pprint

import numpy as np
import pandas as pd

import matplotlib.pyplot as graph
import seaborn as sns

import pyspark.sql.types as pyt
import pyspark.sql.functions as pyf

from acosta.measurement import required_columns, process_notebook_inputs

import acosta
from acosta.alerting.preprocessing import read_pos_data

print(acosta.__version__)

# COMMAND ----------

country_id = 30  # UK
client_id = 16320 # Nestle UK
holding_id = 3257 # AcostaRetailUK

# COMMAND ----------

country_id = 30  # UK
client_id = 16320 # Nestle UK
holding_id = 3257 # AcostaRetailUK
banner_id = 7743  # Asda

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC UPDATE ds_intervention_summary
# MAGIC    SET is_complete = 'FALSE' 
# MAGIC    WHERE mdm_holding_id = 3257
# MAGIC    AND mdm_client_id = 16320
# MAGIC    AND mdm_country_id = 30

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE acosta_retail_analytics_im;
# MAGIC
# MAGIC UPDATE ds_intervention_summary
# MAGIC    SET is_complete = 'TRUE' 
# MAGIC    WHERE mdm_holding_id = 3257
# MAGIC    AND mdm_client_id = 16320
# MAGIC    AND mdm_country_id = 30
# MAGIC    AND call_date >= '2022-09-21'
# MAGIC    
# MAGIC   and not
# MAGIC   is_complete
# MAGIC   and
# MAGIC   total_impact > 0

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count (*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-09-21'
# MAGIC )
# MAGIC and
# MAGIC is_complete
# MAGIC and
# MAGIC total_impact < 0

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count (*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-09-21'
# MAGIC )
# MAGIC and not
# MAGIC is_complete
# MAGIC and
# MAGIC total_impact > 0

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count (*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-09-21'
# MAGIC )
# MAGIC and not
# MAGIC is_complete
# MAGIC and not
# MAGIC total_impact >= 0

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count (*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-09-21'
# MAGIC )
# MAGIC and
# MAGIC is_complete = 'TRUE'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count (*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-09-21'
# MAGIC )
# MAGIC and
# MAGIC is_complete = 'FALSE'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count (*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-09-21'
# MAGIC )
# MAGIC and
# MAGIC is_complete <> 'TRUE'
# MAGIC and
# MAGIC is_complete <> 'FALSE'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count (*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7746 -- Tesco
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-09-21'
# MAGIC )
# MAGIC and
# MAGIC is_complete

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count (*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7746 -- Tesco
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-09-21'
# MAGIC )
# MAGIC and not
# MAGIC is_complete

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count (*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-01-01' AND
# MAGIC   call_date <= '2022-12-31'
# MAGIC )
# MAGIC and
# MAGIC load_ts < '2023-02-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count (*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-01-01' AND
# MAGIC   call_date <= '2022-12-31'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda  
# MAGIC and
# MAGIC standard_response_cd is not null
# MAGIC and
# MAGIC epos_retailer_item_id is not null
# MAGIC and
# MAGIC (
# MAGIC   call_date = '2022-10-12'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda  
# MAGIC and
# MAGIC standard_response_cd is not null
# MAGIC and
# MAGIC epos_retailer_item_id is not null
# MAGIC and
# MAGIC (
# MAGIC   call_date = '2022-10-12'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.ds_intervention_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc asda_nestlecore_uk_dv.vw_sat_link_epos_summary

# COMMAND ----------

sql_all_measurable_asda_nestle_uk_rep_responses_and_results_20221012 = """
SELECT distinct
a.*,
c.RETAILER_ITEM_ID,
e.SALES_DT,
b.response_id as response_id_2,
b.is_complete,
b.total_intervention_effect,
b.total_qintervention_effect,
b.total_impact,
b.total_qimpact,
b.load_ts
FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars a
full join acosta_retail_analytics_im.ds_intervention_summary b on 
ltrim('0', ltrim(rtrim(a.response_id))) = ltrim('0', ltrim(rtrim(b.response_id)))
left join asda_nestlecore_uk_dv.hub_retailer_item c on
ltrim('0', ltrim(rtrim(a.epos_retailer_item_id))) = ltrim('0', ltrim(rtrim(c.RETAILER_ITEM_ID)))
left join asda_nestlecore_uk_dv.hub_organization_unit d on
ltrim('0', ltrim(rtrim(a.epos_organization_unit_num))) = ltrim('0', ltrim(rtrim(d.ORGANIZATION_UNIT_NUM)))
left join (select * from asda_nestlecore_uk_dv.vw_sat_link_epos_summary where sales_dt = '2022-10-12') e on
(c.HUB_RETAILER_ITEM_HK = e.HUB_RETAILER_ITEM_HK and d.HUB_ORGANIZATION_UNIT_HK = e.HUB_ORGANIZATION_UNIT_HK )
where
a.mdm_country_id = 30 -- UK
and
a.mdm_client_id = 16320 -- Nestle UK
and
a.mdm_holding_id = 3257 -- AcostaRetailUK
and
a.mdm_banner_id = 7743 -- Asda  
and
a.standard_response_cd is not null
-- and
-- epos_retailer_item_id is not null
and
(
  a.call_date = '2022-10-12'
)
ORDER BY
a.response_id
"""

df_all_measurable_asda_nestle_uk_rep_responses_and_results_20221012 = spark.sql(sql_all_measurable_asda_nestle_uk_rep_responses_and_results_20221012).cache()

df_all_measurable_asda_nestle_uk_rep_responses_and_results_20221012.coalesce(1).write.format('csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save(f'/mnt/artifacts/hugh/all_measurable_asda_nestle_uk_rep_responses_and_results_20221012_new')

# COMMAND ----------

sql_all_measurable_asda_nestle_uk_rep_responses_and_results_202210 = """
SELECT distinct
a.*,
c.RETAILER_ITEM_ID,
e.SALES_DT,
b.response_id as response_id_2,
b.is_complete,
b.total_intervention_effect,
b.total_qintervention_effect,
b.total_impact,
b.total_qimpact,
b.load_ts
FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars a
full join acosta_retail_analytics_im.ds_intervention_summary b on 
ltrim('0', ltrim(rtrim(a.response_id))) = ltrim('0', ltrim(rtrim(b.response_id)))
left join asda_nestlecore_uk_dv.hub_retailer_item c on
ltrim('0', ltrim(rtrim(a.epos_retailer_item_id))) = ltrim('0', ltrim(rtrim(c.RETAILER_ITEM_ID)))
left join asda_nestlecore_uk_dv.hub_organization_unit d on
ltrim('0', ltrim(rtrim(a.epos_organization_unit_num))) = ltrim('0', ltrim(rtrim(d.ORGANIZATION_UNIT_NUM)))
left join (select * from asda_nestlecore_uk_dv.vw_sat_link_epos_summary) e on
(c.HUB_RETAILER_ITEM_HK = e.HUB_RETAILER_ITEM_HK and d.HUB_ORGANIZATION_UNIT_HK = e.HUB_ORGANIZATION_UNIT_HK 
and a.call_date = e.sales_dt)
where
a.mdm_country_id = 30 -- UK
and
a.mdm_client_id = 16320 -- Nestle UK
and
a.mdm_holding_id = 3257 -- AcostaRetailUK
and
a.mdm_banner_id = 7743 -- Asda  
and
a.standard_response_cd is not null
-- and
-- epos_retailer_item_id is not null
and
(
  a.call_date >= '2022-10-01'
  and
  a.call_date <= '2022-10-31'
)
ORDER BY
a.response_id
"""

df_all_measurable_asda_nestle_uk_rep_responses_and_results_202210 = spark.sql(sql_all_measurable_asda_nestle_uk_rep_responses_and_results_202210).cache()

df_all_measurable_asda_nestle_uk_rep_responses_and_results_202210.coalesce(1).write.format('csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save(f'/mnt/artifacts/hugh/all_measurable_asda_nestle_uk_rep_responses_and_results_202210_take2')

# COMMAND ----------

sql_all_measurable_asda_nestle_uk_rep_responses_and_results_202210b = """
SELECT distinct
a.*,
c.RETAILER_ITEM_ID,
substr(e.sales_dt, 1, 7) as call_month,
b.response_id as response_id_2,
b.is_complete,
b.total_intervention_effect,
b.total_qintervention_effect,
b.total_impact,
b.total_qimpact,
b.load_ts
FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars a
full join acosta_retail_analytics_im.ds_intervention_summary b on 
ltrim('0', ltrim(rtrim(a.response_id))) = ltrim('0', ltrim(rtrim(b.response_id)))
left join asda_nestlecore_uk_dv.hub_retailer_item c on
ltrim('0', ltrim(rtrim(a.epos_retailer_item_id))) = ltrim('0', ltrim(rtrim(c.RETAILER_ITEM_ID)))
left join asda_nestlecore_uk_dv.hub_organization_unit d on
ltrim('0', ltrim(rtrim(a.epos_organization_unit_num))) = ltrim('0', ltrim(rtrim(d.ORGANIZATION_UNIT_NUM)))
left join (select * from asda_nestlecore_uk_dv.vw_sat_link_epos_summary) e on
(c.HUB_RETAILER_ITEM_HK = e.HUB_RETAILER_ITEM_HK and d.HUB_ORGANIZATION_UNIT_HK = e.HUB_ORGANIZATION_UNIT_HK 
and (substr(a.call_date, 1, 7) = substr(e.sales_dt, 1, 7)))
where
a.mdm_country_id = 30 -- UK
and
a.mdm_client_id = 16320 -- Nestle UK
and
a.mdm_holding_id = 3257 -- AcostaRetailUK
and
a.mdm_banner_id = 7743 -- Asda  
and
a.standard_response_cd is not null
-- and
-- epos_retailer_item_id is not null
and
(
  a.call_date >= '2022-10-01'
  and
  a.call_date <= '2022-10-31'
)
ORDER BY
a.response_id
"""

df_all_measurable_asda_nestle_uk_rep_responses_and_results_202210b = spark.sql(sql_all_measurable_asda_nestle_uk_rep_responses_and_results_202210b).cache()

df_all_measurable_asda_nestle_uk_rep_responses_and_results_202210b.coalesce(1).write.format('csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save(f'/mnt/artifacts/hugh/all_measurable_asda_nestle_uk_rep_responses_and_results_202210_take2b')

# COMMAND ----------

sql_all_measurable_morrisons_nestle_uk_rep_responses_and_results_202210 = """
SELECT distinct
a.*,
c.RETAILER_ITEM_ID,
e.SALES_DT,
b.response_id as response_id_2,
b.is_complete,
b.total_intervention_effect,
b.total_qintervention_effect,
b.total_impact,
b.total_qimpact,
b.load_ts
FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars a
full join acosta_retail_analytics_im.ds_intervention_summary b on 
ltrim('0', ltrim(rtrim(a.response_id))) = ltrim('0', ltrim(rtrim(b.response_id)))
left join morrisons_nestlecore_uk_dv.hub_retailer_item c on
ltrim('0', ltrim(rtrim(a.epos_retailer_item_id))) = ltrim('0', ltrim(rtrim(c.RETAILER_ITEM_ID)))
left join morrisons_nestlecore_uk_dv.hub_organization_unit d on
ltrim('0', ltrim(rtrim(a.epos_organization_unit_num))) = ltrim('0', ltrim(rtrim(d.ORGANIZATION_UNIT_NUM)))
left join (select * from morrisons_nestlecore_uk_dv.vw_sat_link_epos_summary) e on
(c.HUB_RETAILER_ITEM_HK = e.HUB_RETAILER_ITEM_HK and d.HUB_ORGANIZATION_UNIT_HK = e.HUB_ORGANIZATION_UNIT_HK 
and a.call_date = e.sales_dt)
where
a.mdm_country_id = 30 -- UK
and
a.mdm_client_id = 16320 -- Nestle UK
and
a.mdm_holding_id = 3257 -- AcostaRetailUK
and
a.mdm_banner_id = 7744 -- Morrisons  
and
a.standard_response_cd is not null
-- and
-- epos_retailer_item_id is not null
and
(
  a.call_date >= '2022-10-01'
  and
  a.call_date <= '2022-10-31'
)
ORDER BY
a.response_id
"""

df_all_measurable_morrisons_nestle_uk_rep_responses_and_results_202210 = spark.sql(sql_all_measurable_morrisons_nestle_uk_rep_responses_and_results_202210).cache()

df_all_measurable_morrisons_nestle_uk_rep_responses_and_results_202210.coalesce(1).write.format('csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save(f'/mnt/artifacts/hugh/all_measurable_morrisons_nestle_uk_rep_responses_and_results_202210')

# COMMAND ----------

display(df_all_measurable_asda_nestle_uk_rep_responses_and_results_20221012)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct * from acosta_retail_analytics_im.interventions_retailer_client_config
# MAGIC
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC -- and
# MAGIC -- mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC order by 
# MAGIC mdm_client_id,
# MAGIC mdm_banner_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct standard_response_cd, standard_response_text from acosta_retail_analytics_im.interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC -- and
# MAGIC -- mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC order by 
# MAGIC standard_response_text

# COMMAND ----------

country_id = 30  # UK
client_id = 16320 # Nestle UK
holding_id = 3257 # AcostaRetailUK
banner_id = 7743  # Asda

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT objective_typ, call_date, count(response_id) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-10-01' AND
# MAGIC   call_date <= '2022-11-30'
# MAGIC )
# MAGIC group by 
# MAGIC objective_typ, call_date
# MAGIC ORDER BY
# MAGIC objective_typ, call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'Opportunity'
# MAGIC and
# MAGIC standard_response_cd is not null
# MAGIC and
# MAGIC (
# MAGIC   call_date = '2022-10-31'
# MAGIC )
# MAGIC ORDER BY
# MAGIC standard_response_cd

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where response_id = '54615784'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where standard_response_text = 'Till Location'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'Opportunity'
# MAGIC and
# MAGIC standard_response_cd is not null
# MAGIC and
# MAGIC (
# MAGIC   call_date <= '2022-10-28'
# MAGIC   and
# MAGIC   call_date >= '2022-10-25'
# MAGIC )
# MAGIC ORDER BY
# MAGIC standard_response_cd
# MAGIC limit
# MAGIC 10000

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'Opportunity'
# MAGIC and
# MAGIC standard_response_cd is not null
# MAGIC and
# MAGIC (
# MAGIC   call_date <= '2022-10-28'
# MAGIC   and
# MAGIC   call_date >= '2022-10-25'
# MAGIC )
# MAGIC ORDER BY
# MAGIC standard_response_cd
# MAGIC limit
# MAGIC 10000

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC epos_retailer_item_id is null

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im_null_ = """
  SELECT *
  FROM 
  acosta_retail_analytics_im.vw_ds_intervention_input_nars
  WHERE
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  mdm_banner_id = {banner_id} AND
  (
      call_date >= '2022-01-01' AND
      call_date <= '2023-02-28'
  ) -- calls happened between 2021-01-01 and 2023-02-28
  ORDER BY
  mdm_banner_id,
  objective_typ,
  call_date,
  response_id
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)

df_acosta_retail_analytics_im_asda = spark.sql(df_sql_query_acosta_retail_analytics_im_asda).cache()

df_acosta_retail_analytics_im_asda.coalesce(1).write.format('csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save(f'/mnt/artifacts/hugh/nestle-uk-measurable-interventions-20220101-to-20230228')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT objective_typ, call_date, count(response_id) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-08-01' AND
# MAGIC   call_date <= '2023-02-28'
# MAGIC )
# MAGIC group by 
# MAGIC objective_typ, call_date
# MAGIC ORDER BY
# MAGIC objective_typ, call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT objective_typ, call_date, count(response_id) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-08-01' AND
# MAGIC   call_date <= '2023-02-28'
# MAGIC )
# MAGIC group by 
# MAGIC objective_typ, call_date
# MAGIC ORDER BY
# MAGIC objective_typ, call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC desc acosta_retail_analytics_im.ds_intervention_summary

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im2 = """
  SELECT 
  substr(call_date, 1, 7) AS call_month,
  avg(total_impact/measurement_duration) as average_daily_uplift
  FROM acosta_retail_analytics_im.ds_intervention_summary
  where
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  (
    call_date >= '2023-05-01' AND
    call_date <= '2023-12-31'
  ) AND
  is_complete = 'TRUE' 
  group by 
  call_month
  ORDER BY
  call_month
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id)

# COMMAND ----------

df_acosta_retail_analytics_im2 = spark.sql(df_sql_query_acosta_retail_analytics_im2)

display(df_acosta_retail_analytics_im2)

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im3 = """
  SELECT 
  call_date,
  avg(total_impact/measurement_duration) as average_daily_uplift
  FROM acosta_retail_analytics_im.ds_intervention_summary
  where
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  (
    call_date >= '2023-05-01' AND
    call_date <= '2023-12-31'
  ) AND
  is_complete = 'TRUE' 
  group by 
  call_date
  ORDER BY
  call_date
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id)

# COMMAND ----------

df_acosta_retail_analytics_im3 = spark.sql(df_sql_query_acosta_retail_analytics_im3)

display(df_acosta_retail_analytics_im3)

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im_by_day = """
  SELECT 
  call_date,
  avg(total_impact) as average_uplift
  FROM acosta_retail_analytics_im.ds_intervention_summary
  where
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  (
    call_date >= '2023-05-01' AND
    call_date <= '2023-12-31'
  ) AND
  is_complete = 'TRUE' 
  group by 
  call_date
  ORDER BY
  call_date
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id)

# COMMAND ----------

df_acosta_retail_analytics_im_by_day = spark.sql(df_sql_query_acosta_retail_analytics_im_by_day).cache()

display(df_acosta_retail_analytics_im_by_day)

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im_count = """
  SELECT 
  call_date, count(is_complete)
  FROM acosta_retail_analytics_im.ds_intervention_summary
  where
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  (
    call_date >= '2023-01-01' AND
    call_date <= '2023-12-31'
  )
  group by 
  call_date, is_complete
  ORDER BY
  call_date, is_complete
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id)

# COMMAND ----------

df_acosta_retail_analytics_im_count = spark.sql(df_sql_query_acosta_retail_analytics_im_count).cache()

display(df_acosta_retail_analytics_im_count)

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im_count_total = """
  SELECT 
  call_date, count(response_id)
  FROM acosta_retail_analytics_im.ds_intervention_summary
  where
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  (
    call_date >= '2023-05-01' AND
    call_date <= '2023-12-31'
  )
  group by 
  call_date
  ORDER BY
  call_date
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id)

# COMMAND ----------

df_acosta_retail_analytics_im_count_total = spark.sql(df_sql_query_acosta_retail_analytics_im_count_total).cache()

display(df_acosta_retail_analytics_im_count_total)

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im_asda = """
  SELECT 
  substr(call_date, 1, 7) AS call_month,
  standard_response_cd,
  count(response_id) as num_uk_opportunity_responses
  FROM acosta_retail_analytics_im.ds_intervention_summary
  where
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  mdm_banner_id = {banner_id} AND
  (
    call_date >= '2022-01-01' AND
    call_date <= '2023-12-31'
  )
  group by 
  call_month,
  standard_response_cd
  ORDER BY
  call_month,
  standard_response_cd
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)

# COMMAND ----------

df_acosta_retail_analytics_im_asda = spark.sql(df_sql_query_acosta_retail_analytics_im_asda).cache()

pivot_df_asda = df_acosta_retail_analytics_im_asda.groupBy("call_month").pivot("standard_response_cd").sum("num_uk_opportunity_responses")

display(pivot_df_asda)

# COMMAND ----------

banner_id = 7743  # Asda

df_sql_query_acosta_retail_analytics_im_asda_uk_opportunities = """
  SELECT 
  substr(call_date, 1, 7) AS call_month,
  standard_response_cd,
  count(response_id) as num_uk_opportunity_responses
  FROM acosta_retail_analytics_im.ds_intervention_summary
  where
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  mdm_banner_id = {banner_id} AND
  (
    call_date >= '2022-01-01' AND
    call_date <= '2023-12-31' 
  )
  AND
  objective_typ = 'Opportunity'
  group by 
  call_month,
  standard_response_cd
  ORDER BY
  call_month,
  standard_response_cd
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)

# COMMAND ----------

df_acosta_retail_analytics_im_asda_uk_opportunities = spark.sql(df_sql_query_acosta_retail_analytics_im_asda_uk_opportunities).cache()

pivot_df_asda_uk_opportunities = df_acosta_retail_analytics_im_asda_uk_opportunities.groupBy("call_month").pivot("standard_response_cd").sum("num_uk_opportunity_responses")

display(pivot_df_asda_uk_opportunities)

# COMMAND ----------

banner_id = 7744  # Morrisons

df_sql_query_acosta_retail_analytics_im_morrisons = """
  SELECT 
  substr(call_date, 1, 7) AS call_month,
  standard_response_cd,
  count(response_id) as num_uk_opportunity_responses
  FROM acosta_retail_analytics_im.ds_intervention_summary
  where
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  mdm_banner_id = {banner_id} AND
  (
    call_date >= '2022-01-01' AND
    call_date <= '2023-12-31'
  )
  group by 
  call_month,
  standard_response_cd
  ORDER BY
  call_month,
  standard_response_cd
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)

# COMMAND ----------

df_acosta_retail_analytics_im_morrisons = spark.sql(df_sql_query_acosta_retail_analytics_im_morrisons).cache()

pivot_df_morrisons = df_acosta_retail_analytics_im_morrisons.groupBy("call_month").pivot("standard_response_cd").sum("num_uk_opportunity_responses")

display(pivot_df_morrisons)

# COMMAND ----------

banner_id = 7745  # Sainsburys

df_sql_query_acosta_retail_analytics_im_sainsburys = """
  SELECT 
  substr(call_date, 1, 7) AS call_month,
  standard_response_cd,
  count(response_id) as num_uk_opportunity_responses
  FROM acosta_retail_analytics_im.ds_intervention_summary
  where
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  mdm_banner_id = {banner_id} AND
  (
    call_date >= '2022-01-01' AND
    call_date <= '2023-12-31'
  )
  group by 
  call_month,
  standard_response_cd
  ORDER BY
  call_month,
  standard_response_cd
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)

# COMMAND ----------

df_acosta_retail_analytics_im_sainsburys = spark.sql(df_sql_query_acosta_retail_analytics_im_sainsburys).cache()

pivot_df_sainsburys = df_acosta_retail_analytics_im_sainsburys.groupBy("call_month").pivot("standard_response_cd").sum("num_uk_opportunity_responses")

display(pivot_df_sainsburys)

# COMMAND ----------

banner_id = 7746  # Tesco

df_sql_query_acosta_retail_analytics_im_tesco = """
  SELECT 
  substr(call_date, 1, 7) AS call_month,
  standard_response_cd,
  count(response_id) as num_uk_opportunity_responses
  FROM acosta_retail_analytics_im.ds_intervention_summary
  where
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  mdm_banner_id = {banner_id} AND
  (
    call_date >= '2022-01-01' AND
    call_date <= '2023-12-31'
  )
  group by 
  call_month,
  standard_response_cd
  ORDER BY
  call_month,
  standard_response_cd
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)

# COMMAND ----------

df_acosta_retail_analytics_im_tesco = spark.sql(df_sql_query_acosta_retail_analytics_im_tesco).cache()

pivot_df_tesco = df_acosta_retail_analytics_im_tesco.groupBy("call_month").pivot("standard_response_cd").sum("num_uk_opportunity_responses")

display(pivot_df_tesco)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT objective_typ, call_date, avg()(response_id) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-08-01' AND
# MAGIC   call_date <= '2022-12-31'
# MAGIC )
# MAGIC group by 
# MAGIC objective_typ, call_date
# MAGIC ORDER BY
# MAGIC objective_typ, call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT objective_typ, call_date, count(response_id) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-08-01' AND
# MAGIC   call_date <= '2022-12-31'
# MAGIC )
# MAGIC group by 
# MAGIC objective_typ, call_date
# MAGIC ORDER BY
# MAGIC objective_typ, call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables in acosta_retail_analytics_im

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im = """
  SELECT *
  FROM 
  acosta_retail_analytics_im.vw_ds_intervention_input_nars
  WHERE
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  -- mdm_banner_id = {banner_id} AND
  (
      call_date >= '2022-01-01' AND
      call_date <= '2023-02-28'
  ) -- calls happened between 2022-01-01 and 2023-02-28
  ORDER BY
  mdm_banner_id,
  objective_typ,
  call_date,
  response_id
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)

# COMMAND ----------

df_acosta_retail_analytics_im = spark.sql(df_sql_query_acosta_retail_analytics_im).cache()

# COMMAND ----------

df_acosta_retail_analytics_im.coalesce(1).write.format('csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save(f'/mnt/artifacts/hugh/nestle-uk-measurable-interventions-20220101-to-20230228')

# COMMAND ----------

banner_id = 7743

df_sql_query_acosta_retail_analytics_im_asda = """
  SELECT *
  FROM 
  acosta_retail_analytics_im.vw_ds_intervention_input_nars
  WHERE
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  mdm_banner_id = {banner_id} AND
  (
      call_date >= '2022-01-01' AND
      call_date <= '2023-02-28'
  ) -- calls happened between 2022-01-01 and 2023-02-28
  ORDER BY
  mdm_banner_id,
  objective_typ,
  call_date,
  response_id
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)

df_acosta_retail_analytics_im_asda = spark.sql(df_sql_query_acosta_retail_analytics_im_asda).cache()

df_acosta_retail_analytics_im_asda.coalesce(1).write.format('csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save(f'/mnt/artifacts/hugh/asda-nestle-uk-measurable-interventions-20220101-to-20230228')

# COMMAND ----------

banner_id = 7744

df_sql_query_acosta_retail_analytics_im_asda = """
  SELECT *
  FROM 
  acosta_retail_analytics_im.vw_ds_intervention_input_nars
  WHERE
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  mdm_banner_id = {banner_id} AND
  (
      call_date >= '2022-01-01' AND
      call_date <= '2023-02-28'
  ) -- calls happened between 2021-01-01 and 2023-02-28
  ORDER BY
  mdm_banner_id,
  objective_typ,
  call_date,
  response_id
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)

df_acosta_retail_analytics_im_asda = spark.sql(df_sql_query_acosta_retail_analytics_im_asda).cache()

df_acosta_retail_analytics_im_asda.coalesce(1).write.format('csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save(f'/mnt/artifacts/hugh/nestle-uk-measurable-interventions-20220101-to-20230228')

# COMMAND ----------

banner_id = 7745

df_sql_query_acosta_retail_analytics_im_asda = """
  SELECT *
  FROM 
  acosta_retail_analytics_im.vw_ds_intervention_input_nars
  WHERE
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  mdm_banner_id = {banner_id} AND
  (
      call_date >= '2022-01-01' AND
      call_date <= '2023-02-28'
  ) -- calls happened between 2021-01-01 and 2023-02-28
  ORDER BY
  mdm_banner_id,
  objective_typ,
  call_date,
  response_id
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)

df_acosta_retail_analytics_im_asda = spark.sql(df_sql_query_acosta_retail_analytics_im_asda).cache()

df_acosta_retail_analytics_im_asda.coalesce(1).write.format('csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save(f'/mnt/artifacts/hugh/nestle-uk-measurable-interventions-20220101-to-20230228')

# COMMAND ----------

banner_id = 7746

df_sql_query_acosta_retail_analytics_im_asda = """
  SELECT *
  FROM 
  acosta_retail_analytics_im.vw_ds_intervention_input_nars
  WHERE
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  mdm_banner_id = {banner_id} AND
  (
      call_date >= '2022-01-01' AND
      call_date <= '2023-02-28'
  ) -- calls happened between 2021-01-01 and 2023-02-28
  ORDER BY
  mdm_banner_id,
  objective_typ,
  call_date,
  response_id
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)

df_acosta_retail_analytics_im_asda = spark.sql(df_sql_query_acosta_retail_analytics_im_asda).cache()

df_acosta_retail_analytics_im_asda.coalesce(1).write.format('csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save(f'/mnt/artifacts/hugh/nestle-uk-measurable-interventions-20220101-to-20230228')
