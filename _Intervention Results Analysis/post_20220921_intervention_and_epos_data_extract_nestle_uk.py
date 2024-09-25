# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

from pprint import pprint
import datetime

import matplotlib.pyplot as graph
import seaborn as sns

import pyspark.sql.types as pyt
import pyspark.sql.functions as pyf

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.vw_ds_intervention_input_nars

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
# MAGIC   call_date >= '2022-10-01' 
# MAGIC )

# COMMAND ----------

sql_all_measurable_nestle_uk_rep_responses_and_results_202210b = """
WITH input_nars as
(
select
mdm_country_id,
mdm_country_nm,
mdm_holding_id,
mdm_holding_nm,
mdm_banner_id,
mdm_banner_nm,
store_acosta_number,
epos_organization_unit_num,
mdm_client_id,
mdm_client_nm,
epos_retailer_item_id,
objective_typ,
call_date,
call_id,
response_id,
standard_response_cd,
standard_response_text,
nars_response_text,
actionable_flg

from acosta_retail_analytics_im.vw_ds_intervention_input_nars where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
standard_response_cd is not null
and
call_date >= '2022-10-01'
)
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
FROM
(
select * from input_nars where mdm_banner_id = 7743
) a
left join
(
select * from acosta_retail_analytics_im.ds_intervention_summary where call_date >= '2022-10-01'
) b on a.response_id = b.response_id
left join asda_nestlecore_uk_dv.hub_retailer_item c on
a.epos_retailer_item_id = c.RETAILER_ITEM_ID
left join asda_nestlecore_uk_dv.hub_organization_unit d on
a.epos_organization_unit_num = d.ORGANIZATION_UNIT_NUM
left join (select * from asda_nestlecore_uk_dv.vw_sat_link_epos_summary where sales_dt >='2022-10-01') e on
(c.HUB_RETAILER_ITEM_HK = e.HUB_RETAILER_ITEM_HK and d.HUB_ORGANIZATION_UNIT_HK = e.HUB_ORGANIZATION_UNIT_HK
and a.call_date = e.sales_dt)
union
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
FROM
(
select * from input_nars where mdm_banner_id = 7744
) a
left join
(
select * from acosta_retail_analytics_im.ds_intervention_summary where call_date >= '2022-10-01' 
) b on a.response_id = b.response_id
left join morrisons_nestlecore_uk_dv.hub_retailer_item c on
a.epos_retailer_item_id = c.RETAILER_ITEM_ID
left join morrisons_nestlecore_uk_dv.hub_organization_unit d on
a.epos_organization_unit_num = d.ORGANIZATION_UNIT_NUM
left join (select * from morrisons_nestlecore_uk_dv.vw_sat_link_epos_summary where sales_dt >='2022-09-21') e on
(c.HUB_RETAILER_ITEM_HK = e.HUB_RETAILER_ITEM_HK and d.HUB_ORGANIZATION_UNIT_HK = e.HUB_ORGANIZATION_UNIT_HK
and a.call_date = e.sales_dt)
union
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
FROM
(
select * from input_nars where mdm_banner_id = 7745
) a
left join
(
select * from acosta_retail_analytics_im.ds_intervention_summary where call_date >= '2022-10-01'
) b on a.response_id = b.response_id
left join sainsburys_nestlecore_uk_dv.hub_retailer_item c on
a.epos_retailer_item_id = c.RETAILER_ITEM_ID
left join sainsburys_nestlecore_uk_dv.hub_organization_unit d on
a.epos_organization_unit_num = d.ORGANIZATION_UNIT_NUM
left join (select * from sainsburys_nestlecore_uk_dv.vw_sat_link_epos_summary where sales_dt >='2022-10-01') e on
(c.HUB_RETAILER_ITEM_HK = e.HUB_RETAILER_ITEM_HK and d.HUB_ORGANIZATION_UNIT_HK = e.HUB_ORGANIZATION_UNIT_HK
and a.call_date = e.sales_dt)
union
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
FROM
(
select * from input_nars where mdm_banner_id = 7746
) a
left join
(
select * from acosta_retail_analytics_im.ds_intervention_summary where call_date >= '2022-10-01'
) b on a.response_id = b.response_id
left join tesco_nestlecore_uk_dv.hub_retailer_item c on
a.epos_retailer_item_id = c.RETAILER_ITEM_ID
left join tesco_nestlecore_uk_dv.hub_organization_unit d on
a.epos_organization_unit_num = d.ORGANIZATION_UNIT_NUM
left join (select * from tesco_nestlecore_uk_dv.vw_sat_link_epos_summary where sales_dt >='2022-10-01') e on
(c.HUB_RETAILER_ITEM_HK = e.HUB_RETAILER_ITEM_HK and d.HUB_ORGANIZATION_UNIT_HK = e.HUB_ORGANIZATION_UNIT_HK
and a.call_date = e.sales_dt)
ORDER BY
mdm_banner_id, response_id
"""

df_all_measurable_nestle_uk_rep_responses_and_results_202210 = spark.sql(sql_all_measurable_nestle_uk_rep_responses_and_results_202210b).cache()

# COMMAND ----------

current_date = datetime.date.today()

df_all_measurable_nestle_uk_rep_responses_and_results_202210.coalesce(1).write.format('csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save(f'/mnt/artifacts/hugh/all_measurable_nestle_uk_responses_and_results_after_20221001_{current_date}'.format(current_date=current_date))

# COMMAND ----------


