# Databricks notebook source
# MAGIC %md
# MAGIC ## Apply LOESS Value Measurement Formulas

# COMMAND ----------

from pprint import pprint

import numpy as np
import pandas as pd

import pyspark.sql.functions as pyf
import pyspark.sql.types as pyt

from acosta.measurement import process_notebook_inputs

import acosta

print(acosta.__version__)


# COMMAND ----------

# Inputs get required inputs
dbutils.widgets.text('country_id', '-1', 'Country ID')
dbutils.widgets.text('client_id', '-1', 'Client ID')
dbutils.widgets.text('holding_id', '-1', 'Holding ID')
dbutils.widgets.text('banner_id', '-1', 'Banner ID')

input_names = ('country_id', 'client_id', 'holding_id', 'banner_id')

country_id, client_id, holding_id, banner_id = [process_notebook_inputs(dbutils.widgets.get(s)) for s in input_names]

print('Country ID =', country_id)
print('Client ID =', client_id)
print('Holding ID =', holding_id)
print('Banner ID =', banner_id)

# country_id = 30 # UK
# client_id = 16320 # Nestle UK
# banner_id = 7743 # Asda
# holding_id = 3257 # AcostaRetailUK

# COMMAND ----------

#######################
### Load from Cache ###
df_merged = spark.read.format('delta').load(
    f'/mnt/processed/loess_measurement/cache/{client_id}-{country_id}-{holding_id}-{banner_id}')

print(f'{df_merged.cache().count():,}')

# COMMAND ----------

df_merged.printSchema()

# COMMAND ----------

display(df_merged)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply LOESS Value Measurement Formulas

# COMMAND ----------

df_results_by_day = df_merged

# COMMAND ----------

# This is akin to the intervention effect in IVM
#
# If day >= 0 then it's considered an uplift intervention, so we take the actual sales quantity minus the baseline sales quantity
# If day < 0 then it's considered a recovery intervention, so we take the baseline sales quantity minus the actual sales quantity

df_results_by_day = df_results_by_day.withColumn(
        'DIFF_POS_ITEM_QTY',
        pyf.when(pyf.col('diff_day') >= 0, pyf.col('POS_ITEM_QTY') - pyf.col('BASELINE_POS_ITEM_QTY')
                ).otherwise(pyf.col('BASELINE_POS_ITEM_QTY') - pyf.col('POS_ITEM_QTY')
                           ).cast(pyt.DecimalType(15, 2))
)
  
# If the result is 0 or negative, then we set it to NULL so that we ignore it later on, because that's how the UK formula does it
df_results_by_day = df_results_by_day.withColumn(
        'DIFF_POS_ITEM_QTY',
        pyf.when(pyf.col('DIFF_POS_ITEM_QTY') <= 0, pyf.lit(None)
                ).otherwise(pyf.col('DIFF_POS_ITEM_QTY'))
)

# COMMAND ----------

 display(df_results_by_day)

# COMMAND ----------

df_results = df_results_by_day.groupBy('mdm_country_id', 
                                       'mdm_country_nm',
                                       'mdm_client_id',
                                       'mdm_client_nm',
                                       'mdm_holding_id',
                                       'mdm_holding_nm',
                                       'mdm_banner_id',
                                       'mdm_banner_nm',
                                       'store_acosta_number',
                                       'epos_organization_unit_num',
                                       'epos_retailer_item_id',
                                       'objective_typ',
                                       'call_date',
                                       'call_id',
                                       'response_id',
                                       'standard_response_cd',
                                       'standard_response_text',
                                       'nars_response_text',
                                       'intervention_rank',
                                       'intervention_group',
                                       'intervention_start_day',
                                       'intervention_end_day',
                                       'actionable_flg',
                                       'measure_start',
                                       'measure_end',
                                       'duration'
                                      ).agg(pyf.avg("DIFF_POS_ITEM_QTY").alias('AVG_DIFF_POS_ITEM_QTY'),
                                            pyf.avg("PRICE").alias('AVG_PRICE')
                                           ).orderBy(["epos_organization_unit_num", "epos_retailer_item_id", "call_date", "response_id"], ascending=True)

# COMMAND ----------

# This is the average intervention effect of the days where there was a net positive intervention effect
# The UK calls this average uplift amount
df_results = df_results.withColumn('AVG_DIFF_POS_ITEM_QTY',
    pyf.when(pyf.col('AVG_DIFF_POS_ITEM_QTY').isNull(), 0).otherwise(pyf.col('AVG_DIFF_POS_ITEM_QTY'))
)

df_results = df_results.withColumn('AVG_PRICE',
    pyf.when(pyf.col('AVG_PRICE').isNull(), 0).otherwise(pyf.col('AVG_PRICE'))
)

# This is akin to the average uplift value in UK speak
df_results = df_results.withColumn('uplift_value',
    pyf.col('AVG_DIFF_POS_ITEM_QTY') * pyf.col('AVG_PRICE')
)

# This is akin to the total uplift value (in pounds) in UK speak
df_results = df_results.withColumn('uplift_total',
    pyf.col('uplift_value') * pyf.col('duration')
)

# This is akin to the total uplift amount in UK speak
df_results = df_results.withColumn('total_intervention_effect',
    pyf.col('AVG_DIFF_POS_ITEM_QTY') * pyf.col('duration')
)

# This is akin to the total uplift value (in pounds) in UK speak
df_results = df_results.withColumn('total_impact',
    pyf.col('total_intervention_effect') * pyf.col('AVG_PRICE')
)

print(f'Results Count = {df_results.cache().count():,}')

# COMMAND ----------

display(df_results)

# COMMAND ----------

df_results.write.format('delta')\
    .mode('overwrite')\
    .option('mergeSchema', 'true')\
    .save(f'/mnt/processed/loess_measurement/results/{client_id}-{country_id}-{holding_id}-{banner_id}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO NOTE Using a shadow database for now for testing only. We need to change it to acosta_retail_analytics_im later before we prmote code to production
# MAGIC create table if not exists acosta_retail_analytics_im.ds_intervention_summary
# MAGIC (
# MAGIC     mdm_country_nm  String,
# MAGIC     mdm_holding_nm  String,
# MAGIC     mdm_banner_nm string,
# MAGIC     mdm_client_nm string,
# MAGIC     store_acosta_number Int,
# MAGIC     epos_organization_unit_num  String,
# MAGIC     epos_retailer_item_id String,
# MAGIC     objective_typ String,
# MAGIC     call_id String,
# MAGIC     response_id String,
# MAGIC     nars_response_text  String,
# MAGIC     standard_response_text  String,
# MAGIC     standard_response_cd  String,
# MAGIC     measurement_duration  Int,
# MAGIC     is_complete   Boolean,
# MAGIC     total_intervention_effect Decimal(15, 2),
# MAGIC     total_qintervention_effect  Decimal(15, 2),
# MAGIC     total_impact  Decimal(15, 2),
# MAGIC     total_qimpact Decimal(15, 2),
# MAGIC     load_ts timestamp,
# MAGIC     mdm_country_id  Int,
# MAGIC     mdm_holding_id  Int,
# MAGIC     mdm_banner_id Int,
# MAGIC     mdm_client_id Int,
# MAGIC     call_date Date
# MAGIC )
# MAGIC  
# MAGIC USING delta
# MAGIC tblproperties (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true)
# MAGIC LOCATION 'abfss://data@eus2psag2dpcoredatalake.dfs.core.windows.net/informationmart/acosta_retail_report/ds_intervention_summary'

# COMMAND ----------

df_summary_table = df_results

df_summary_table = df_summary_table.withColumn('is_complete', pyf.lit(True))
df_summary_table = df_summary_table.withColumn('total_qintervention_effect', pyf.lit(None))
df_summary_table = df_summary_table.withColumn('total_qimpact', pyf.lit(None))
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

df_summary_table.printSchema()

# COMMAND ----------

display(df_summary_table)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists adhoc_acosta_retail_report_im.ds_intervention_summary
# MAGIC (
# MAGIC     mdm_country_nm  String,
# MAGIC     mdm_holding_nm  String,
# MAGIC     mdm_banner_nm string,
# MAGIC     mdm_client_nm string,
# MAGIC     store_acosta_number Int,
# MAGIC     epos_organization_unit_num  String,
# MAGIC     epos_retailer_item_id String,
# MAGIC     objective_typ String,
# MAGIC     call_id String,
# MAGIC     response_id String,
# MAGIC     nars_response_text  String,
# MAGIC     standard_response_text  String,
# MAGIC     standard_response_cd  String,
# MAGIC     measurement_duration  Int,
# MAGIC     is_complete   Boolean,
# MAGIC     total_intervention_effect Decimal(15, 2),
# MAGIC     total_qintervention_effect  Decimal(15, 2),
# MAGIC     total_impact  Decimal(15, 2),
# MAGIC     total_qimpact Decimal(15, 2),
# MAGIC     load_ts timestamp,
# MAGIC     mdm_country_id  Int,
# MAGIC     mdm_holding_id  Int,
# MAGIC     mdm_banner_id Int,
# MAGIC     mdm_client_id Int,
# MAGIC     call_date Date
# MAGIC )
# MAGIC -- Not creating a DELTA table as we would like to overwrite partitions.
# MAGIC USING org.apache.spark.sql.parquet
# MAGIC PARTITIONED BY (mdm_country_id, mdm_holding_id, mdm_banner_id, mdm_client_id, call_date)
# MAGIC OPTIONS (
# MAGIC     `compression` 'snappy',
# MAGIC     path 'adl://azueus2prdadlsdatalake.azuredatalakestore.net/informationmart/adhoc_acosta_retail_report_im/ds_intervention_summary'
# MAGIC );

# COMMAND ----------

# %sql
# -- TODO NOTE Using a shadow database for now for testing only. We need to change it to acosta_retail_analytics_im later before we prmote code to production
# create table if not exists adhoc_acosta_retail_report_im.ds_intervention_summary
# (
#     mdm_country_nm  String,
#     mdm_holding_nm  String,
#     mdm_banner_nm string,
#     mdm_client_nm string,
#     store_acosta_number Int,
#     epos_organization_unit_num  String,
#     epos_retailer_item_id String,
#     objective_typ String,
#     call_id String,
#     response_id String,
#     nars_response_text  String,
#     standard_response_text  String,
#     standard_response_cd  String,
#     measurement_duration  Int,
#     is_complete   Boolean,
#     total_intervention_effect Decimal(15, 2),
#     total_qintervention_effect  Decimal(15, 2),
#     total_impact  Decimal(15, 2),
#     total_qimpact Decimal(15, 2),
#     load_ts timestamp,
#     mdm_country_id  Int,
#     mdm_holding_id  Int,
#     mdm_banner_id Int,
#     mdm_client_id Int,
#     call_date Date
# )

# USING delta
# tblproperties (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true)
# LOCATION 'abfss://data@eus2psag2dpcoredatalake.dfs.core.windows.net/informationmart/acosta_retail_report/ds_intervention_summary'

# COMMAND ----------

# Save without overwriting previously completed
completed_response_ids = spark.sql(f'''
    select response_id, is_complete
    from adhoc_acosta_retail_report_im.ds_intervention_summary
    where is_complete = true
''')
completed_response_ids = completed_response_ids.select('response_id').distinct().toPandas()['response_id']
completed_response_ids = set(completed_response_ids)

# Filter out completed interventions
df_summary_table = df_summary_table.filter(~df_summary_table['response_id'].isin(completed_response_ids))

# Save the new interventions data
df_summary_table \
    .write \
    .mode('overwrite') \
    .insertInto('adhoc_acosta_retail_report_im.ds_intervention_summary', overwrite=True)

# COMMAND ----------


