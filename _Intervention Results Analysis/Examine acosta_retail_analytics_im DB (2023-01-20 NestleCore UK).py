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

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.interventions_retailer_client_config
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17713

# COMMAND ----------

client_id = 16320
country_id = 30
holding_id = 3257
banner_id = 7743 # asda

# COMMAND ----------

# Load required data
df_asda_nestle_uk_summary_table = spark.read.format('delta').load(
    f'/mnt/processed/measurement/results/{client_id}-{country_id}-{holding_id}-{banner_id}-summary-apltest'
)

print(df_asda_nestle_uk_summary_table.cache().count())

# COMMAND ----------

banner_id = 7744 # morrisons

# Load required data
df_morrisons_nestle_uk_summary_table = spark.read.format('delta').load(
    f'/mnt/processed/measurement/results/{client_id}-{country_id}-{holding_id}-{banner_id}-summary-apltest'
)

print(df_morrisons_nestle_uk_summary_table.cache().count())

# COMMAND ----------

banner_id = 7743 # asda

df_asda_nestle_uk_summary_table.coalesce(1).write.format('csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save(f'/mnt/artifacts/hugh/{client_id}-{country_id}-{holding_id}-{banner_id}-summary-apltest')

# COMMAND ----------

banner_id = 7744 # morrisons

df_morrisons_nestle_uk_summary_table.coalesce(1).write.format('csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save(f'/mnt/artifacts/hugh/{client_id}-{country_id}-{holding_id}-{banner_id}-summary-apltest')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc default.ds_intervention_summary_apltest

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from default.ds_intervention_summary_apltest

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.vw_ds_intervention_input_nars_apltest

# COMMAND ----------

client_id = 16320
country_id = 30
holding_id = 3257
banner_id = 7743 # asda

sql_all_measurable_asda_nestle_uk_rep_responses_input_nars_apltest = """
select * from acosta_retail_analytics_im.vw_ds_intervention_input_nars_apltest
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_banner_id = 7743 -- Asda
and
standard_response_cd is not null
and
epos_retailer_item_id is not null
and
(
  (call_date >= '2022-10-01'AND
  call_date <= '2022-12-31')   
)
"""
df_all_measurable_asda_nestle_uk_rep_responses_input_nars_apltest = spark.sql(sql_all_measurable_asda_nestle_uk_rep_responses_input_nars_apltest).cache()

print(df_all_measurable_asda_nestle_uk_rep_responses_input_nars_apltest.count())

df_all_measurable_asda_nestle_uk_rep_responses_input_nars_apltest.coalesce(1).write.format('csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save(f'/mnt/artifacts/hugh/{client_id}-{country_id}-{holding_id}-{banner_id}-input-nars-apltest')

# COMMAND ----------

client_id = 16320
country_id = 30
holding_id = 3257
banner_id = 7744 # morrisons

sql_all_measurable_morrisons_nestle_uk_rep_responses_input_nars_apltest = """
select * from acosta_retail_analytics_im.vw_ds_intervention_input_nars_apltest
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_banner_id = 7744 -- Morrisons
and
standard_response_cd is not null
and
epos_retailer_item_id is not null
and
(
  (call_date >= '2022-10-01'AND
  call_date <= '2022-12-31')   
)
"""
df_all_measurable_morrisons_nestle_uk_rep_responses_input_nars_apltest = spark.sql(sql_all_measurable_morrisons_nestle_uk_rep_responses_input_nars_apltest).cache()

print(df_all_measurable_morrisons_nestle_uk_rep_responses_input_nars_apltest.count())

df_all_measurable_morrisons_nestle_uk_rep_responses_input_nars_apltest.coalesce(1).write.format('csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save(f'/mnt/artifacts/hugh/{client_id}-{country_id}-{holding_id}-{banner_id}-input-nars-apltest')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc default.vw_sat_link_epos_summary_apltest

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc morrisons_nestlecore_uk_dv.vw_sat_link_epos_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from default.vw_sat_link_epos_summary_apltest
# MAGIC where sales_dt >= '2022-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from acosta_retail_analytics_im.vw_ds_intervention_input_nars_apltest
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
# MAGIC   call_date >= '2022-10-01'
# MAGIC   and
# MAGIC   call_date <= '2023-12-31'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from acosta_retail_analytics_im.vw_ds_intervention_input_nars_apltest
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
# MAGIC   call_date >= '2022-10-01'
# MAGIC   and
# MAGIC   call_date <= '2023-12-31'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.vw_ds_intervention_input_nars_apltest
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7744 -- Morrisons  

# COMMAND ----------

country_id = 30  # UK
client_id = 16320 # Nestle UK
holding_id = 3257 # AcostaRetailUK
banner_id = 7743  # Asda

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
    .save(f'/mnt/artifacts/hugh/all_measurable_morrisons_nestle_uk_rep_responses_and_results_202210b')

# COMMAND ----------

df_interventions = spark.read.format('delta').load(
    f'/mnt/processed/measurement/cache/{client_id}-{country_id}-{holding_id}-{banner_id}-intervention'
)
df_unique_interventions = df_interventions.select(
    'mdm_country_nm', 'mdm_holding_nm', 'mdm_banner_nm', 'mdm_client_nm',
    'store_acosta_number', 'epos_organization_unit_num', 'epos_retailer_item_id',
    'objective_typ', 'call_id', 'response_id',
    'nars_response_text', 'standard_response_text',
    'duration', 'call_date'
).distinct()

intervention_duration_map = df_interventions.select('response_id', 'duration').distinct().toPandas()
intervention_duration_map = dict(zip(
    intervention_duration_map['response_id'],
    intervention_duration_map['duration']
))
print(f'{len(intervention_duration_map):,} Interventions')

# COMMAND ----------

df_results = spark.read.format('delta').load(
    f'/mnt/processed/measurement/results/{client_id}-{country_id}-{holding_id}-{banner_id}')

print(df_results.count())

# COMMAND ----------

summary_table_schema = pyt.StructType([
    pyt.StructField('epos_organization_unit_num', pyt.StringType()),
    pyt.StructField('epos_retailer_item_id', pyt.StringType()),
    pyt.StructField('response_id', pyt.StringType()),
    pyt.StructField('standard_response_cd', pyt.StringType()),
    pyt.StructField('is_complete', pyt.BooleanType()),
    pyt.StructField('total_intervention_effect', pyt.FloatType()),
    pyt.StructField('total_qintervention_effect', pyt.FloatType()),
    pyt.StructField('total_impact', pyt.FloatType()),
    pyt.StructField('total_qimpact', pyt.FloatType()),
])
summary_table_cols = [col.name for col in summary_table_schema]


def compile_summary_table_udf(df: pd.DataFrame):
    # Required outputs
    response_id = df['response_id'].unique()[0]
    retailer_item_id = df['RETAILER_ITEM_ID'].unique()[0]
    organization_unit_num = df['ORGANIZATION_UNIT_NUM'].unique()[0]
    response_code = df['standard_response_cd'].unique()[0]
    total_intervention_effect = None
    total_qintervention_effect = None
    total_impact = None
    total_qimpact = None

    # Logic
    observed_diff_days = df['diff_day'].unique()
    is_complete = observed_diff_days.max() == intervention_duration_map[response_id]

    if is_complete:
        total_intervention_effect = df['INTERVENTION_EFFECT'].fillna(0).sum()
        total_qintervention_effect = df['QINTERVENTION_EFFECT'].fillna(0).sum()
        total_impact = df['INTERVENTION_VALUE'].fillna(0).sum()
        total_qimpact = (df['PRICE'].fillna(0) * df['QINTERVENTION_EFFECT'].fillna(0)).sum()

    # Results
    df_output = pd.DataFrame({
        'response_id': [response_id],
        'epos_retailer_item_id': [retailer_item_id],
        'epos_organization_unit_num': [organization_unit_num],
        'standard_response_cd': [response_code],
        'is_complete': [is_complete],
        'total_intervention_effect': [total_intervention_effect],
        'total_qintervention_effect': [total_qintervention_effect],
        'total_impact': [total_impact],
        'total_qimpact': [total_qimpact],
    })
    return df_output


# Join identifying information
df_summary_table = df_results \
    .groupby('response_id') \
    .applyInPandas(compile_summary_table_udf, schema=summary_table_schema)
print(f'{df_summary_table.cache().count():,} Interventions')

df_summary_table = df_summary_table.join(
    df_unique_interventions,
    on=['response_id', 'epos_organization_unit_num', 'epos_retailer_item_id'],
    how='left'
)

# Add essential columns
df_summary_table = df_summary_table.withColumn('mdm_country_id', pyf.lit(country_id))
df_summary_table = df_summary_table.withColumn('mdm_client_id', pyf.lit(client_id))
df_summary_table = df_summary_table.withColumn('mdm_holding_id', pyf.lit(holding_id))
df_summary_table = df_summary_table.withColumn('mdm_banner_id', pyf.lit(banner_id))
df_summary_table = df_summary_table.withColumn('load_ts', pyf.current_timestamp())

df_summary_table = df_summary_table.withColumnRenamed('duration', 'measurement_duration')

# Ensure column order
df_summary_table = df_summary_table.select(
    'mdm_country_nm', 'mdm_holding_nm', 'mdm_banner_nm', 'mdm_client_nm',
    'store_acosta_number', 'epos_organization_unit_num', 'epos_retailer_item_id',
    'objective_typ', 'call_id', 'response_id',
    'nars_response_text', 'standard_response_text', 'standard_response_cd',
    'measurement_duration', 'is_complete',
    'total_intervention_effect', 'total_qintervention_effect', 'total_impact', 'total_qimpact', 'load_ts',
    'mdm_country_id', 'mdm_holding_id', 'mdm_banner_id', 'mdm_client_id',
    'call_date'
)

print(f'{df_summary_table.cache().count():,} Interventions')

# COMMAND ----------

# Let's only look as 2022 interventions
df_summary_table_2022 = df_summary_table.where((pyf.col("call_date") < pyf.lit("2023-01-01")) & (pyf.col("call_date") >= pyf.lit("2022-01-01")))

print(df_summary_table_2022.count())
print((df_summary_table_2022.distinct()).count())

# COMMAND ----------

# Get count of interventions by call date in 2022
df_summary_table_2022_call_dates_num_interventions_by_call_date = df_summary_table_2022.groupBy('call_date', 
                                     ).agg(pyf.count("response_id").alias('num_interventions')).orderBy('call_date')

display(df_summary_table_2022_call_dates_num_interventions_by_call_date)

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
# MAGIC SELECT count (distinct *) FROM acosta_retail_analytics_im.ds_intervention_summary
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
# MAGIC SELECT call_date, count (distinct *) FROM acosta_retail_analytics_im.ds_intervention_summary
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
# MAGIC group by call_date
# MAGIC order by call_date

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
# MAGIC SELECT count (distinct *) FROM acosta_retail_analytics_im.ds_intervention_summary
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
    .save(f'/mnt/artifacts/hugh/all_measurable_asda_nestle_uk_rep_responses_and_results_20221012')

# COMMAND ----------

display(df_all_measurable_asda_nestle_uk_rep_responses_and_results_20221012)

# COMMAND ----------

# MAGIC %sql
# MAGIC desc acosta_retail_analytics_im.vw_fact_question_response

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acosta_retail_analytics_im.vw_fact_question_response

# COMMAND ----------

# MAGIC %sql
# MAGIC desc acosta_retail_analytics_im.vw_fact_question_response

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acosta_retail_analytics_im.vw_dimension_itemlevel where client_id = 16320 and category_description = 'Cereal'

# COMMAND ----------

# MAGIC %sql
# MAGIC desc acosta_retail_analytics_im.vw_dimension_itemlevel

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

# IMPORTS
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# from datetime import datetime, timedelta

# COMMAND ----------

# GET ALL "Cereal" INTERVENTIONS

query = f"""
SELECT * 
FROM acosta_retail_analytics_im.vw_fact_question_response
WHERE country_id = 30
AND item_dimension_id IN (
  SELECT item_dimension_id
  FROM acosta_retail_analytics_im.vw_dimension_itemlevel
  WHERE category_description = "Cereal"
  )
"""

df_ps = spark.sql(query)

print(f"{df_ps.cache().count():,}")

display(df_ps)

# COMMAND ----------

df_aug_interventions = df_ps.where(df_ps.calendar_key.between(20220801, 20220831))
print(df_aug_interventions.select('call_id').distinct().count())

df_sep_interventions = df_ps.where(df_ps.calendar_key.between(20220901, 20220930))
print(df_sep_interventions.select('call_id').distinct().count())

df_oct_interventions = df_ps.where(df_ps.calendar_key.between(20221001, 20221031))
print(df_oct_interventions.select('call_id').distinct().count())

df_nov_interventions = df_ps.where(df_ps.calendar_key.between(20221101, 20221130))
print(df_nov_interventions.select('call_id').distinct().count())

df_dec_interventions = df_ps.where(df_ps.calendar_key.between(20221201, 20221231))
print(df_dec_interventions.select('call_id').distinct().count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## GET ALL "Cereal" INTERVENTIONS (SUMMARY DATA)

# COMMAND ----------

# "Cereal" interventions / ds_intervention_summary
query = f"""
select
iin.mdm_country_id,
iin.mdm_country_nm,
iin.mdm_holding_id,
iin.mdm_holding_nm,
iin.mdm_client_id,
iin.mdm_client_nm,
iin.mdm_banner_id,
iin.mdm_banner_nm,
COUNT(iin.response_id),
WEEKOFYEAR(DATE_ADD(call_date, 1)) AS call_week

from 
  acosta_retail_analytics_im.ds_intervention_summary iin

join 
  acosta_retail_analytics_im.vw_fact_question_response fqr
on 
iin.response_id = fqr.response_id

join
acosta_retail_analytics_im.vw_dimension_itemlevel il
on 
fqr.item_level_id = il.item_level_id

where
(
  call_date >= '2022-08-01' AND
  call_date <= '2022-12-31'
)
and
iin.mdm_country_id = 30 -- UK
and
iin.mdm_client_id = 16320 -- Nestle UK
and
iin.mdm_holding_id = 3257 -- AcostaRetailUK
and
il.category_description = "Cereal"

group by
iin.mdm_country_id,
iin.mdm_country_nm,
iin.mdm_holding_id,
iin.mdm_holding_nm,
iin.mdm_client_id,
iin.mdm_client_nm,
iin.mdm_banner_id,
iin.mdm_banner_nm,
call_week

order by 
iin.mdm_country_id,
iin.mdm_country_nm,
iin.mdm_holding_id,
iin.mdm_holding_nm,
iin.mdm_client_id,
iin.mdm_client_nm,
iin.mdm_banner_id,
iin.mdm_banner_nm,
call_week
"""

df_ps = spark.sql(query)

print(f"{df_ps.cache().count():,}")

display(df_ps)

# COMMAND ----------

df = df_ps.toPandas()

# COMMAND ----------

asda = df[df['mdm_banner_nm'] == 'Asda']
morrisons = df[df['mdm_banner_nm'] == 'Morrisons']
sainsburys = df[df['mdm_banner_nm'] == 'Sainsburys']
tesco = df[df['mdm_banner_nm'] == 'Tesco']

plt.figure(figsize=(10, 6))
plt.plot(asda['call_week'], asda['count(response_id)'], label='Asda')
plt.plot(morrisons['call_week'], morrisons['count(response_id)'], label='Morrisons')
plt.plot(sainsburys['call_week'], sainsburys['count(response_id)'], label='Sainsburys')
plt.plot(tesco['call_week'], tesco['count(response_id)'], label='Tesco')
plt.legend(fontsize=14)
plt.title('Cereal Interventions (Summary data, Aug-Dec 2022)', fontsize=16)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## GET ALL INTERVENTIONS (SUMMARY DATA)

# COMMAND ----------

# All interventions / ds_intervention_summary

query = f"""
select
iin.mdm_country_id,
iin.mdm_country_nm,
iin.mdm_holding_id,
iin.mdm_holding_nm,
iin.mdm_client_id,
iin.mdm_client_nm,
iin.mdm_banner_id,
iin.mdm_banner_nm,
COUNT(iin.response_id),
WEEKOFYEAR(DATE_ADD(call_date, 1)) AS call_week

from 
  acosta_retail_analytics_im.ds_intervention_summary iin

join 
  acosta_retail_analytics_im.vw_fact_question_response fqr
on 
iin.response_id = fqr.response_id

join
acosta_retail_analytics_im.vw_dimension_itemlevel il
on 
fqr.item_level_id = il.item_level_id

where
(
  call_date >= '2022-08-01' AND
  call_date <= '2022-12-31'
)
and
iin.mdm_country_id = 30 -- UK
and
iin.mdm_client_id = 16320 -- Nestle UK
and
iin.mdm_holding_id = 3257 -- AcostaRetailUK

group by
iin.mdm_country_id,
iin.mdm_country_nm,
iin.mdm_holding_id,
iin.mdm_holding_nm,
iin.mdm_client_id,
iin.mdm_client_nm,
iin.mdm_banner_id,
iin.mdm_banner_nm,
call_week

order by 
iin.mdm_country_id,
iin.mdm_country_nm,
iin.mdm_holding_id,
iin.mdm_holding_nm,
iin.mdm_client_id,
iin.mdm_client_nm,
iin.mdm_banner_id,
iin.mdm_banner_nm,
call_week
"""

df_ps2 = spark.sql(query)

print(f"{df_ps2.cache().count():,}")

display(df_ps2)

# COMMAND ----------

df2 = df_ps2.toPandas()

# COMMAND ----------

asda = df2[df2['mdm_banner_nm'] == 'Asda']
morrisons = df2[df2['mdm_banner_nm'] == 'Morrisons']
sainsburys = df2[df2['mdm_banner_nm'] == 'Sainsburys']
tesco = df2[df2['mdm_banner_nm'] == 'Tesco']

plt.figure(figsize=(10, 6))
plt.plot(asda['call_week'], asda['count(response_id)'], label='Asda')
plt.plot(morrisons['call_week'], morrisons['count(response_id)'], label='Morrisons')
plt.plot(sainsburys['call_week'], sainsburys['count(response_id)'], label='Sainsburys')
plt.plot(tesco['call_week'], tesco['count(response_id)'], label='Tesco')
plt.legend(fontsize=14)
plt.title('All Interventions (Summary data, Aug-Dec 2022)', fontsize=16)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## GET ALL "Cereal" INTERVENTIONS (INPUT INTERVENTION NARS DATA)

# COMMAND ----------

# "Cereal" interventions / vw_ds_intervention_input_nars
query = f"""
select
iin.mdm_country_id,
iin.mdm_country_nm,
iin.mdm_holding_id,
iin.mdm_holding_nm,
iin.mdm_client_id,
iin.mdm_client_nm,
iin.mdm_banner_id,
iin.mdm_banner_nm,
COUNT(iin.response_id),
WEEKOFYEAR(DATE_ADD(call_date, 1)) AS call_week

from 
  acosta_retail_analytics_im.vw_ds_intervention_input_nars iin

join 
  acosta_retail_analytics_im.vw_fact_question_response fqr
on 
iin.response_id = fqr.response_id

join
acosta_retail_analytics_im.vw_dimension_itemlevel il
on 
fqr.item_level_id = il.item_level_id

where
(
  call_date >= '2022-08-01' AND
  call_date <= '2022-12-31'
)
and
iin.mdm_country_id = 30 -- UK
and
iin.mdm_client_id = 16320 -- Nestle UK
and
iin.mdm_holding_id = 3257 -- AcostaRetailUK
and
il.category_description = "Cereal"

group by
iin.mdm_country_id,
iin.mdm_country_nm,
iin.mdm_holding_id,
iin.mdm_holding_nm,
iin.mdm_client_id,
iin.mdm_client_nm,
iin.mdm_banner_id,
iin.mdm_banner_nm,
call_week

order by 
iin.mdm_country_id,
iin.mdm_country_nm,
iin.mdm_holding_id,
iin.mdm_holding_nm,
iin.mdm_client_id,
iin.mdm_client_nm,
iin.mdm_banner_id,
iin.mdm_banner_nm,
call_week
"""

df_ps3 = spark.sql(query)

print(f"{df_ps3.cache().count():,}")

display(df_ps3)

# COMMAND ----------

df3 = df_ps3.toPandas()

# COMMAND ----------

asda = df3[df3['mdm_banner_nm'] == 'Asda']
morrisons = df3[df3['mdm_banner_nm'] == 'Morrisons']
sainsburys = df3[df3['mdm_banner_nm'] == 'Sainsburys']
tesco = df3[df3['mdm_banner_nm'] == 'Tesco']

plt.figure(figsize=(10, 6))
plt.plot(asda['call_week'], asda['count(response_id)'], label='Asda')
plt.plot(morrisons['call_week'], morrisons['count(response_id)'], label='Morrisons')
plt.plot(sainsburys['call_week'], sainsburys['count(response_id)'], label='Sainsburys')
plt.plot(tesco['call_week'], tesco['count(response_id)'], label='Tesco')
plt.legend(fontsize=14)
plt.title('Cereal Interventions (Input intervention data, Aug-Dec 2022)', fontsize=16)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## GET ALL INTERVENTIONS (INPUT INTERVENTION NARS DATA)

# COMMAND ----------

# All interventions / vw_ds_intervention_input_nars

query = f"""
select
iin.mdm_country_id,
iin.mdm_country_nm,
iin.mdm_holding_id,
iin.mdm_holding_nm,
iin.mdm_client_id,
iin.mdm_client_nm,
iin.mdm_banner_id,
iin.mdm_banner_nm,
COUNT(iin.response_id),
WEEKOFYEAR(DATE_ADD(call_date, 1)) AS call_week

from 
  acosta_retail_analytics_im.vw_ds_intervention_input_nars iin

join 
  acosta_retail_analytics_im.vw_fact_question_response fqr
on 
iin.response_id = fqr.response_id

join
acosta_retail_analytics_im.vw_dimension_itemlevel il
on 
fqr.item_level_id = il.item_level_id

where
(
  call_date >= '2022-08-01' AND
  call_date <= '2022-12-31'
)
and
iin.mdm_country_id = 30 -- UK
and
iin.mdm_client_id = 16320 -- Nestle UK
and
iin.mdm_holding_id = 3257 -- AcostaRetailUK

group by
iin.mdm_country_id,
iin.mdm_country_nm,
iin.mdm_holding_id,
iin.mdm_holding_nm,
iin.mdm_client_id,
iin.mdm_client_nm,
iin.mdm_banner_id,
iin.mdm_banner_nm,
call_week

order by 
iin.mdm_country_id,
iin.mdm_country_nm,
iin.mdm_holding_id,
iin.mdm_holding_nm,
iin.mdm_client_id,
iin.mdm_client_nm,
iin.mdm_banner_id,
iin.mdm_banner_nm,
call_week
"""

df_ps4 = spark.sql(query)

print(f"{df_ps4.cache().count():,}")

display(df_ps4)

# COMMAND ----------

df4 = df_ps4.toPandas()

# COMMAND ----------

asda = df4[df4['mdm_banner_nm'] == 'Asda']
morrisons = df4[df4['mdm_banner_nm'] == 'Morrisons']
sainsburys = df4[df4['mdm_banner_nm'] == 'Sainsburys']
tesco = df4[df4['mdm_banner_nm'] == 'Tesco']

plt.figure(figsize=(10, 6))
plt.plot(asda['call_week'], asda['count(response_id)'], label='Asda')
plt.plot(morrisons['call_week'], morrisons['count(response_id)'], label='Morrisons')
plt.plot(sainsburys['call_week'], sainsburys['count(response_id)'], label='Sainsburys')
plt.plot(tesco['call_week'], tesco['count(response_id)'], label='Tesco')
plt.legend(fontsize=14)
plt.title('All Interventions (Input intervention data, Aug-Dec 2022)', fontsize=16)
plt.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Check the number of candidate alerts...
# MAGIC select distinct SALES_DT from tesco_nestlecore_uk_retail_alert_im.alert_on_shelf_availability
# MAGIC order by SALES_DT desc
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Check the number of candidate alerts...
# MAGIC select SALES_DT, count(*) from tesco_nestlecore_uk_retail_alert_im.alert_on_shelf_availability
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc ds_intervention_summary

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
