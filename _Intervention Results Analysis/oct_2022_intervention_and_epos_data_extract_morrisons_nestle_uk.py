# Databricks notebook source
from pprint import pprint

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
banner_id = 7744  # Morrisons

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
# MAGIC mdm_banner_id = 7744 -- Morrisons
# MAGIC and
# MAGIC (
# MAGIC   call_date >= '2022-10-01' AND
# MAGIC   call_date <= '2022-10-31'
# MAGIC )

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
    .save(f'/mnt/artifacts/hugh/all_measurable_morrisons_nestle_uk_rep_responses_and_results_oct_2022_20230302')

# COMMAND ----------


