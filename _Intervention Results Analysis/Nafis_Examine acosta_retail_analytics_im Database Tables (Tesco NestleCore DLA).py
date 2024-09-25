# Databricks notebook source
from pprint import pprint

import pyspark.sql.functions as pyf

from acosta.measurement import required_columns, process_notebook_inputs

import acosta
from acosta.alerting.preprocessing import read_pos_data

print(acosta.__version__)

# COMMAND ----------

country_id = 30  # UK
client_id = 16320 # Nestle UK
holding_id = 3257 # AcostaRetailUK
banner_id = 7746 # Tesco

# COMMAND ----------

# MAGIC %md
# MAGIC # Explore tables (Hugh)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * from acosta_retail_analytics_im.interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * from acosta_retail_analytics_im.interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC objective_typ = 'DLA'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESC acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct objective_typ FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct nars_response_text, standard_response_text FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC objective_typ = 'DLA'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct objective_typ FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct nars_response_text, standard_response_text FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC objective_typ='DLA'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC objective_typ='DLA'
# MAGIC and
# MAGIC call_date >= '2021-08-28'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct
# MAGIC standard_response_text,
# MAGIC nars_response_text
# MAGIC FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7746 -- Tesco
# MAGIC and
# MAGIC objective_typ = 'DLA'

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
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC objective_typ = 'DLA'
# MAGIC and
# MAGIC call_date >= '2021-09-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC and
# MAGIC objective_typ='DLA'
# MAGIC and
# MAGIC call_date >= '2021-08-29'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

df_sql_query_acosta_retail_analytics_im = """
  SELECT
  mdm_country_nm,
  mdm_holding_nm,
  mdm_client_nm,
  epos_organization_unit_num,
  call_id,
  response_id,
  total_intervention_effect,
  total_impact,
  WEEKOFYEAR(DATE_ADD(call_date, 1)) AS call_week
  FROM 
  acosta_retail_analytics_im.ds_intervention_summary
  WHERE
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  coalesce(mdm_banner_id, -1) = {banner_id} AND -- default
  (
      call_date >= '2021-09-01' AND
      call_date <= '2021-09-30'
  ) AND -- calls happened between 2021-09-01 and 2021-09-30
  load_ts like '2021-0%' -- loaded in 2021
""".format(country_id=country_id, client_id=client_id, holding_id=holding_id, banner_id=banner_id)

# COMMAND ----------

df_acosta_retail_analytics_im = spark.sql(df_sql_query_acosta_retail_analytics_im)

# COMMAND ----------

display(df_acosta_retail_analytics_im)

# COMMAND ----------

df_acosta_retail_analytics_im.count()

# COMMAND ----------

# stores = spark.read.format('csv')\
#     .options(header='true', inferSchema='true')\
#     .load('/mnt/processed/alerting/fieldTest/experiment-nestlewaters_v2.csv')

# COMMAND ----------

# display(stores)

# COMMAND ----------

# test_stores = stores.filter((stores.experiment_group == 'test')).select('Store#').withColumnRenamed('Store#', 'epos_organization_unit_num')
# control_stores = stores.filter((stores.experiment_group == 'control')).select('Store#').withColumnRenamed('Store#', 'epos_organization_unit_num')

# display(test_stores)

# COMMAND ----------

# df_acosta_retail_analytics_im_test = df_acosta_retail_analytics_im.join(test_stores, df_acosta_retail_analytics_im.epos_organization_unit_num == test_stores.epos_organization_unit_num, "inner")

# df_acosta_retail_analytics_im_test.count()

# COMMAND ----------

# df_acosta_retail_analytics_im_control = df_acosta_retail_analytics_im.join(control_stores, df_acosta_retail_analytics_im.epos_organization_unit_num == control_stores.epos_organization_unit_num, "inner")

# df_acosta_retail_analytics_im_control.count()

# COMMAND ----------

# df_acosta_retail_analytics_im_test.createOrReplaceTempView("test_alerts")

# df_sql_query_acosta_retail_analytics_im_test_agg = """
#   SELECT
#   mdm_holding_nm,
#   mdm_client_nm,
#   COUNT(total_intervention_effect),
#   SUM(total_intervention_effect),
#   SUM(total_impact),
#   call_week
#   FROM 
#   test_alerts
#   GROUP BY
#   mdm_holding_nm,
#   mdm_client_nm,
#   call_week
#   ORDER BY
#   call_week
# """

# COMMAND ----------

# df_acosta_retail_analytics_im_test_agg = spark.sql(df_sql_query_acosta_retail_analytics_im_test_agg)
# display(df_acosta_retail_analytics_im_test_agg)

# COMMAND ----------

# df_acosta_retail_analytics_im_control.createOrReplaceTempView("control_alerts")

# df_sql_query_acosta_retail_analytics_im_control_agg = """
#   SELECT
#   mdm_holding_nm,
#   mdm_client_nm,
#   COUNT(total_intervention_effect),
#   SUM(total_intervention_effect),
#   SUM(total_impact),
#   call_week
#   FROM 
#   control_alerts
#   GROUP BY
#   mdm_holding_nm,
#   mdm_client_nm,
#   call_week
#   ORDER BY
#   call_week
# """

# COMMAND ----------

# df_acosta_retail_analytics_im_control_agg = spark.sql(df_sql_query_acosta_retail_analytics_im_control_agg)
# display(df_acosta_retail_analytics_im_control_agg)

# COMMAND ----------

# intervention_grp = spark.read.format('csv')\
#     .options(header='true', inferSchema='true')\
#     .load('/mnt/processed/alerting/fieldTest/Intervention_Parameters_Table.csv')

# COMMAND ----------

# display(intervention_grp)

# COMMAND ----------

# MAGIC %md
# MAGIC # Response Analysis

# COMMAND ----------

import pandas as pd

client_id, holding_id, banner_id = 16320, 3257, 7746
retailer, client, country_code = 'tesco', 'nestlecore', 'uk'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC WEEKOFYEAR('2021-04-03'),
# MAGIC WEEKOFYEAR('2021-04-04'),
# MAGIC WEEKOFYEAR(DATE_ADD(CAST('2021-04-04' AS DATE), 1)),
# MAGIC WEEKOFYEAR('2021-04-05')

# COMMAND ----------

def get_response_data_from_view(client_id, holding_id, banner_id, start_date, end_date): 
    query = f"""
    select 
        epos_organization_unit_num,
        response_id,
        call_id,
        nars_response_text,
        call_date,
        intervention_group,
        actionable_flg,
        intervention_start_day,
        intervention_end_day,
        
        WEEKOFYEAR(DATE_ADD(call_date, 1)) AS call_week
    from
    (
        select * from acosta_retail_analytics_im.vw_ds_intervention_input_nars
        where
        mdm_client_id = \'{client_id}\' AND
        mdm_holding_id = \'{holding_id}\' AND
        coalesce(mdm_banner_id, -1) = \'{banner_id}\' AND
        call_date >= \'{start_date}\' AND 
        call_date <= \'{end_date}\' AND
        objective_typ= 'DLA'
    )
    """
    df = spark.sql(query) 
    return df

# COMMAND ----------

def get_measurement_effect(client_id, holding_id, banner_id, start_date, end_date):
    query = f"""  
          SELECT
          -- mdm_country_nm,
          -- mdm_holding_nm,
          -- mdm_client_nm,
          -- epos_organization_unit_num,
          -- call_id,
          response_id,
          total_intervention_effect,
          total_impact
          FROM 
          acosta_retail_analytics_im.ds_intervention_summary
          WHERE
          mdm_client_id = \'{client_id}\' AND
          mdm_holding_id = \'{holding_id}\' AND
          coalesce(mdm_banner_id, -1) = \'{banner_id}\' AND
          call_date >= \'{start_date}\' AND 
          call_date <= \'{end_date}\' AND
          objective_typ= 'DLA'
    """   
    df = spark.sql(query)   
    return df

# COMMAND ----------

response_df = get_response_data_from_view(client_id, holding_id, banner_id, '2021-08-29', '2021-11-30')
print(f'{response_df.cache().count():,}')

# COMMAND ----------

display(response_df)

# COMMAND ----------

#test/control stores:
df_test_control_stores = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load(f'/mnt/processed/alerting/fieldTest/experiment_design/{retailer}_{client}_{country_code}_store_assignment.csv')

# COMMAND ----------

display(df_test_control_stores)

# COMMAND ----------

response_df_labeled = response_df.join(
        df_test_control_stores.select('StoreID', 'experiment_group'),
        df_test_control_stores['StoreID'] == response_df['epos_organization_unit_num']
)

# COMMAND ----------

display(response_df_labeled)

# COMMAND ----------

response_df_pd = response_df_labeled.toPandas()
response_df_pd.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

#version1
pd.crosstab([response_df_pd['experiment_group'], response_df_pd['nars_response_text']],
            response_df_pd['call_week'], dropna=False)

# COMMAND ----------

#version2
pd.crosstab(response_df_pd['nars_response_text'],
            [response_df_pd['experiment_group'],response_df_pd['call_week']],
           dropna=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Measurement

# COMMAND ----------

measurement_df = get_measurement_effect(client_id, holding_id, banner_id, '2021-08-29', '2021-11-30')
print(measurement_df.cache().count())

# COMMAND ----------

display(measurement_df)

# COMMAND ----------

df = response_df_labeled.join(measurement_df, 'response_id')
print(df.cache().count())

# COMMAND ----------

display(df)

# COMMAND ----------

df_pd = df.toPandas()

for col in ['total_intervention_effect','total_impact']:
    df_pd[col] = pd.to_numeric(df_pd[col])
df_pd.head()

# COMMAND ----------

#version1
#NOTE: call_week = week_of_year -1
pd.pivot_table(df_pd,
               index='experiment_group',
               columns='call_week',
               values=['response_id'],
               aggfunc='count'
              )

# COMMAND ----------

#version2
#NOTE: call_week = week_of_year -1
pd.pivot_table(df_pd,
               index='experiment_group',
               columns='call_week',
               values=['total_intervention_effect', 'total_impact'],
               aggfunc='sum'
              )

# COMMAND ----------


