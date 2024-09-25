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
# MAGIC show tables in acosta_retail_analytics_im

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.interventions_response_mapping
# MAGIC where
# MAGIC standard_response_text in
# MAGIC ("Increased Facings",
# MAGIC "Merchandising Fixture",
# MAGIC "Pallet Corner",
# MAGIC "Promotion Re-Charge")
# MAGIC order by
# MAGIC standard_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 16320 -- Nestle UK
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC standard_response_text in
# MAGIC ("Increased Facings",
# MAGIC "Merchandising Fixture",
# MAGIC "Pallet Corner",
# MAGIC "Promotion Re-Charge")
# MAGIC order by
# MAGIC standard_response_text,
# MAGIC mdm_banner_id

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
# MAGIC -- and
# MAGIC -- call_date >= '2021-08-28'
# MAGIC group by call_date
# MAGIC order by call_date

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
# MAGIC coalesce(mdm_banner_id, -1) = 7746 -- Tesco
# MAGIC -- and
# MAGIC -- objective_typ='DLA'
# MAGIC and
# MAGIC call_date >= '2021-12-07'

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
# MAGIC and
# MAGIC call_date >= '2021-12-01'

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
# MAGIC -- and
# MAGIC -- objective_typ='DLA'
# MAGIC and
# MAGIC call_date >= '2021-08-28'
# MAGIC group by call_date
# MAGIC order by call_date

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
# MAGIC objective_typ='UK Opportunities'
# MAGIC and
# MAGIC call_date >= '2021-08-29'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc acosta_retail_analytics_im.ds_intervention_summary

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
# MAGIC objective_typ='DLA'

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Response Analysis

# COMMAND ----------

import pandas as pd

client_id, holding_id, banner_id = 16320, 3257, 7746
retailer, client, country_code = 'tesco', 'nestlecore', 'uk'

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

response_df = get_response_data_from_view(client_id, holding_id, banner_id, '2021-08-29', '2021-12-31')
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

def get_measured_effect(client_id, holding_id, banner_id, start_date, end_date):
    query = f"""  
          SELECT
          mdm_country_nm,
          mdm_holding_nm,
          mdm_client_nm,
          store_acosta_number,
          epos_organization_unit_num,
          epos_retailer_item_id,
          call_date,
          call_id,
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

measured_df = get_measured_effect(client_id, holding_id, banner_id, '2021-06-01', '2021-11-30')
print(measured_df.cache().count())

# COMMAND ----------

measured_df_labeled = measured_df.join(
        df_test_control_stores.select('StoreID', 'experiment_group'),
        df_test_control_stores['StoreID'] == measured_df['epos_organization_unit_num']
)

# COMMAND ----------

measured_df_filtered = measured_df_labeled.where(pyf.col("experiment_group") == 'test')

# COMMAND ----------

display(measured_df_filtered)

# COMMAND ----------

measured_df_filtered.count()

# COMMAND ----------

measured_df_filtered.where(pyf.col("call_date") >= '2021-09-01').count()

# COMMAND ----------

measured_df_filtered.where(pyf.col("call_date") < '2021-09-01').count()

# COMMAND ----------

def read_pos_data(retailer, client, country_code, sql_context):
    """
    Reads in POS data from the Data Vault and returns a DataFrame

    The output DataFrame is suitable for use in the pos_to_training_data function.

    Note that when reading from the data vault, we use a pre-defined view that will
    sort out the restatement records for us.  This means we don't have to write our
    own logic for handling duplicate rows within the source dataset.

    :param string retailer: the name of the retailer to pull
    :param string client: the name of the client to pull
    :param string country_code: the two character name of the country code to pull
    :param pyspark.sql.context.HiveContext sql_context: PySparkSQL context that can be used to connect to the Data Vault

    :return DataFrame:
    """

    retailer = retailer.strip().upper()
    client = client.strip().upper()
    country_code = country_code.strip().upper()

    database = '{retailer}_{client}_{country_code}_dv'.format(retailer=retailer.strip().lower(),
                                                              client=client.strip().lower(),
                                                              country_code=country_code.strip().lower())

    sql_statement = """
        SELECT
              \'{retailer}\' AS RETAILER,
              \'{client}\' AS CLIENT,
              \'{country_code}\' AS COUNTRY_CODE,
              HRI.RETAILER_ITEM_ID,
              HOU.ORGANIZATION_UNIT_NUM,
              VSLES.SALES_DT,
              VSLES.POS_ITEM_QTY,
              VSLES.POS_AMT,
              VSLES.ON_HAND_INVENTORY_QTY,
              VSLES.POS_AMT / VSLES.POS_ITEM_QTY AS UNIT_PRICE,
              'DATA VAULT' AS ROW_ORIGIN,
              'COMPUTED' AS PRICE_ORIGIN,
              'IGNORE' AS TRAINING_ROLE
           FROM {database}.vw_sat_link_epos_summary VSLES
           LEFT OUTER JOIN {database}.hub_retailer_item HRI 
                ON HRI.HUB_RETAILER_ITEM_HK = VSLES.HUB_RETAILER_ITEM_HK
           LEFT OUTER JOIN {database}.hub_organization_unit HOU 
                ON HOU.HUB_ORGANIZATION_UNIT_HK = VSLES.HUB_ORGANIZATION_UNIT_HK
        """

    sql_statement = sql_statement.format(database=database, retailer=retailer, client=client, country_code=country_code)

    return sql_context.sql(sql_statement)

# COMMAND ----------

# Read POS data
data_vault_data = read_pos_data(retailer, client, country_code, sqlContext).repartition('SALES_DT')

# COMMAND ----------

data_vault_data.count()

# COMMAND ----------

# Let's only look as 2021 onwards
data = data_vault_data.where((pyf.col("SALES_DT") >= pyf.lit("2021-01-01")) & (pyf.col("POS_ITEM_QTY") >= 0))

# COMMAND ----------

total_sales_by_store_item = data.groupBy("ORGANIZATION_UNIT_NUM", "RETAILER_ITEM_ID").agg(pyf.sum("POS_ITEM_QTY").alias('ANNUAL_POS_ITEM_QTY'), pyf.sum("POS_AMT").alias('ANNUAL_POS_AMT')).orderBy(["ORGANIZATION_UNIT_NUM", "RETAILER_ITEM_ID"], ascending=True)

# COMMAND ----------

total_sales_by_store_item.count()

# COMMAND ----------

display(total_sales_by_store_item)

# COMMAND ----------

total_sales_by_store_item_high = total_sales_by_store_item.where(pyf.col("ANNUAL_POS_ITEM_QTY") >= 5000)

# COMMAND ----------

total_sales_by_store_item_high.count()

# COMMAND ----------

measured_df_combined = measured_df_filtered.join(total_sales_by_store_item,
    (measured_df_filtered['epos_organization_unit_num'] == total_sales_by_store_item['ORGANIZATION_UNIT_NUM']) &
    (measured_df_filtered['epos_retailer_item_id'] == total_sales_by_store_item['RETAILER_ITEM_ID']), how = 'inner')

# COMMAND ----------

measured_df_combined = measured_df_combined.drop('StoreID', 'ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID')

# COMMAND ----------

measured_df_combined.count()

# COMMAND ----------

measured_df_final = measured_df_combined.where(pyf.col("total_intervention_effect") >= 0)

# COMMAND ----------

measured_df_final.count()

# COMMAND ----------

measured_df_pretest = measured_df_final.where(pyf.col("call_date") < '2021-09-01')

# COMMAND ----------

display(measured_df_pretest)

# COMMAND ----------

measured_df_posttest = measured_df_final.where(pyf.col("call_date") >= '2021-09-01')

# COMMAND ----------

display(measured_df_posttest)

# COMMAND ----------


