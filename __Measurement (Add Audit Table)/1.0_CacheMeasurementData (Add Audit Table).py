# Databricks notebook source
# MAGIC %md
# MAGIC # Cache Measurement Data
# MAGIC
# MAGIC Write the data that `2.0_Fitting` notebook needs to run

# COMMAND ----------

from pprint import pprint

import datetime
import warnings
from dateutil.relativedelta import relativedelta

import pyspark.sql.functions as pyf
import pyspark.sql.types as pyt
from pyspark.sql.window import Window

from acosta.alerting.preprocessing.functions import (
    get_pos_data_ivm_measurement, all_possible_days_measurement_withupc,
    _replace_negative_and_null_with,
    compute_price_and_lag_lead_price_measurement)
from acosta.measurement import required_columns, process_notebook_inputs
from acosta.alerting.helpers import features as acosta_features

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

# COMMAND ----------

# MAGIC %md
# MAGIC # Load & Preprocess Data

# COMMAND ----------

client_config = spark.sql(f'''
  SELECT * FROM acosta_retail_analytics_im.interventions_retailer_client_config
  WHERE
  mdm_country_id = {country_id} AND
  mdm_client_id = {client_id} AND
  mdm_holding_id = {holding_id} AND
  coalesce(mdm_banner_id, -1) = {banner_id}
''')
assert client_config.cache().count() == 1

# Create config dict
config_dict = client_config.toPandas().T
config_dict = dict(zip(config_dict.index, config_dict[0]))
pprint(config_dict)

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPC - retailer_item_id table

# COMMAND ----------

def upc_itemid_mapping(client_id, banner_id, holding_id, country_id):
  upc_retailer_id = spark.sql(
    f'''select distinct whse_code as retailer_item_id, upc from acosta_retail_analytics_im.fact_apl
                       where country_id = {country_id} 
                       and client_id = {client_id} 
                       and banner_id = {banner_id}
                       and holding_id = {holding_id}
                       and whse_code is Not null
                       and whse_code != ""''')
  grouped_df = upc_retailer_id.groupBy('retailer_item_id').agg(
    pyf.collect_set('upc').alias('upc_set'))
  joined_df = grouped_df.alias('a').join(grouped_df.alias('b'), pyf.lit(1) == pyf.lit(1))\
                  .where(pyf.size(pyf.array_intersect(pyf.col('a.upc_set'), pyf.col('b.upc_set'))) >= 1)\
                  .select(pyf.col('a.retailer_item_id').alias('retailer_item_id'), pyf.array_union(pyf.col('a.upc_set'), pyf.col('b.upc_set')).alias('upc_set'))
  temp_df = joined_df.groupBy('retailer_item_id').agg(pyf.collect_set('upc_set').alias('upc_set'))\
                      .withColumn('upc_set', pyf.sort_array(pyf.array_distinct(pyf.flatten(pyf.col('upc_set')))))
  temp_df = temp_df \
      .withColumn('length', pyf.size(temp_df['upc_set']))
  max_length = temp_df.select(pyf.max(pyf.col('length'))).collect()[0][0]

  temp_df = temp_df.select(
      pyf.col('retailer_item_id'),
      pyf.explode(pyf.array([pyf.array([pyf.lit(temp_df.upc_set[0]), pyf.col('upc_set')[i]]) for i in range(max_length)]))
      .alias('split_arr')
  )
  temp_df = temp_df.select(
      pyf.col('retailer_item_id'),
      pyf.col('split_arr')[0].alias('reported_upc'),
      pyf.col('split_arr')[1].alias('nonreported_upc')
  ).dropna(subset=['nonreported_upc'])
  upc_itemid_mapper = temp_df.select('retailer_item_id', 'reported_upc').dropDuplicates()
  return upc_itemid_mapper

# COMMAND ----------

upc_itemid_mapper = upc_itemid_mapping(client_id, banner_id, holding_id, country_id)
upc_itemid_mapper.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## POS data

# COMMAND ----------

def get_pos_with_upc(pos_database, min_date, client_id, banner_id, holding_id, country_id, spark):
  df = get_pos_data_ivm_measurement(pos_database, min_date, spark)
  df = df.join(upc_itemid_mapper, df['RETAILER_ITEM_ID'] == upc_itemid_mapper['retailer_item_id'], how='left')\
       .drop(upc_itemid_mapper['retailer_item_id'])\
       .withColumn('UPC',
                   pyf.when(pyf.col('reported_upc').isNull(),
                            pyf.col('RETAILER_ITEM_ID')
                           ).otherwise(pyf.col('reported_upc')))\
       .drop('reported_upc')
  df = all_possible_days_measurement_withupc(
    df, upc_itemid_mapper, 'SALES_DT',
    ['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'], 'UPC',
    'ORGANIZATION_UNIT_NUM')

  # Fill in the zeros for On hand inventory quantity
  df = df.withColumn(
      'ON_HAND_INVENTORY_QTY',
      pyf.when(pyf.col('ON_HAND_INVENTORY_QTY') < 0,
              pyf.lit(0)).otherwise(pyf.col('ON_HAND_INVENTORY_QTY')))
  df = _replace_negative_and_null_with(df, ['POS_ITEM_QTY', 'POS_AMT'], 0)
  grouped_values = df.groupBy('UPC', 'ORGANIZATION_UNIT_NUM', 'SALES_DT')\
    .agg(pyf.sum('POS_ITEM_QTY').alias('POS_ITEM_QTY'),
        pyf.sum('POS_AMT').alias('POS_AMT'),
        pyf.sum('ON_HAND_INVENTORY_QTY').alias('ON_HAND_INVENTORY_QTY'),
        pyf.avg(pyf.when(df.UNIT_PRICE > 0, df.UNIT_PRICE)).alias('UNIT_PRICE'))

  df = df.select('SALES_DT',
               'RETAILER_ITEM_ID',
               'ORGANIZATION_UNIT_NUM',
               'HUB_ORGANIZATION_UNIT_HK',
               'HUB_RETAILER_ITEM_HK',
               'LINK_ePOS_Summary_HK',
               'SAT_LINK_EPOS_SUMMARY_HDIFF',
               'LOAD_TS',
               'RECORD_SOURCE_CD','UPC')\
      .join(grouped_values, ['UPC', 'ORGANIZATION_UNIT_NUM', 'SALES_DT'])
  window_item_store = Window.partitionBy(['UPC', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM']).orderBy('SALES_DT')

  for lag in acosta_features.get_lag_days():
      df = df.withColumn(
          f'LAG_UNITS_{lag}',
          pyf.lag(pyf.col('POS_ITEM_QTY'), lag).over(window_item_store)
      )
      if lag <= 14:
          df = df.withColumn(
              f'LAG_INV_{lag}',
              pyf.lag(pyf.col('ON_HAND_INVENTORY_QTY'), lag).over(window_item_store)
          )
  df = compute_price_and_lag_lead_price_measurement(df, 'UPC')
  df = df.withColumn('RECENT_ON_HAND_INVENTORY_QTY', pyf.coalesce(
          pyf.when(pyf.isnan(pyf.col('LAG_INV_1')), None).otherwise(pyf.col('LAG_INV_1')),
          pyf.when(pyf.isnan(pyf.col('LAG_INV_2')), None).otherwise(pyf.col('LAG_INV_2')),
          pyf.when(pyf.isnan(pyf.col('LAG_INV_3')), None).otherwise(pyf.col('LAG_INV_3')),
          pyf.when(pyf.isnan(pyf.col('LAG_INV_4')), None).otherwise(pyf.col('LAG_INV_4')),
          pyf.when(pyf.isnan(pyf.col('LAG_INV_5')), None).otherwise(pyf.col('LAG_INV_5')),
          pyf.when(pyf.isnan(pyf.col('LAG_INV_6')), None).otherwise(pyf.col('LAG_INV_6')),
          pyf.when(pyf.isnan(pyf.col('LAG_INV_7')), None).otherwise(pyf.col('LAG_INV_7')),
          pyf.when(pyf.isnan(pyf.col('LAG_INV_8')), None).otherwise(pyf.col('LAG_INV_8')),
          pyf.when(pyf.isnan(pyf.col('LAG_INV_9')), None).otherwise(pyf.col('LAG_INV_9')),
          pyf.when(pyf.isnan(pyf.col('LAG_INV_10')), None).otherwise(pyf.col('LAG_INV_10')),
          pyf.when(pyf.isnan(pyf.col('LAG_INV_11')), None).otherwise(pyf.col('LAG_INV_11')),
          pyf.when(pyf.isnan(pyf.col('LAG_INV_12')), None).otherwise(pyf.col('LAG_INV_12')),
          pyf.when(pyf.isnan(pyf.col('LAG_INV_13')), None).otherwise(pyf.col('LAG_INV_13')),
          pyf.when(pyf.isnan(pyf.col('LAG_INV_14')), None).otherwise(pyf.col('LAG_INV_14'))
      )).drop(*[f'LAG_INV_{i}' for i in range(1, 15)])

  # The RECENT_ON_HAND_INVENTORY_DIFF is the prior day end's inventory minus two days ago
  # RECENT_ON_HAND_INVENTORY_QTY is at least the prior day's ending inventory
  df = df.withColumn(
      'RECENT_ON_HAND_INVENTORY_DIFF',
      pyf.col('RECENT_ON_HAND_INVENTORY_QTY')\
      - pyf.lag(pyf.col('RECENT_ON_HAND_INVENTORY_QTY'), 1).over(window_item_store)
  )
  df = df.dropDuplicates(['UPC', 'ORGANIZATION_UNIT_NUM', 'SALES_DT']) 
  # Add day of features
  df = df.withColumn('DOW', pyf.dayofweek('SALES_DT'))
  df = df.withColumn('DOM', pyf.dayofmonth('SALES_DT'))
  df = df.withColumn('DOY', pyf.dayofyear('SALES_DT'))
  return df

# COMMAND ----------

today_date = datetime.date.today()
min_date_filter = (today_date - relativedelta(years=2)).strftime(format='%Y-%m-%d')

# COMMAND ----------

df_pos = get_pos_with_upc(config_dict['epos_datavault_db_nm'], min_date_filter, client_id, banner_id, holding_id, country_id, spark)
print(f'{df_pos.cache().count():,}')
display(df_pos)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Intervention Data

# COMMAND ----------

df_intervention_pre_0 = spark.sql(f'''
    SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
    WHERE
    mdm_country_id = {country_id} and
    mdm_client_id = {client_id} and
    mdm_holding_id = {holding_id} and
    coalesce(mdm_banner_id, -1) = {banner_id}
''')
     
df_intervention_pre_0 = df_intervention_pre_0.where(pyf.col('call_date') >= min_date_filter)

print(
    f"{df_intervention_pre_0.cache().count():,} Total distinct interventions in input view within the last 2 years (this is the starting point for our audit table)"
)

# Filter on interventions which are actionable
df_intervention_pre_1 = df_intervention_pre_0.filter(
    df_intervention_pre_0["actionable_flg"].isNotNull()
)

print(
    f"{df_intervention_pre_1.cache().count():,} Interventions with an actionable response text"
)

# Filter on interventions which have an identifiable intervention_id and retailer_item_id
df_intervention_pre_2 = (
    df_intervention_pre_1.filter(df_intervention_pre_1["response_id"].isNotNull())
    .filter(df_intervention_pre_1["epos_retailer_item_id"].isNotNull())
    .withColumn(
        "measure_start", pyf.expr("date_add(call_date, intervention_start_day)")
    )
    .withColumn("measure_end", pyf.expr("date_add(call_date, intervention_end_day)"))
)

print(f"{df_intervention_pre_2.cache().count():,} Interventions with identifiable response_id and retailer_item_id that have an actionable response text (aka measurable interventions)")

# Note this step does not filter out any additional interventions
# Reason being we are doing a left join, and if reported_upc is null then we just use epos_retailer_item_id
df_intervention = df_intervention_pre_2.join(upc_itemid_mapper, 
                    df_intervention_pre_2['epos_retailer_item_id'] == upc_itemid_mapper['retailer_item_id'],
                     how='left')\
      .drop(upc_itemid_mapper['retailer_item_id'])\
      .withColumn('UPC',
                  pyf.when(pyf.col('reported_upc').isNull(),
                          pyf.col('epos_retailer_item_id')
                          ).otherwise(pyf.col('reported_upc')))\
      .drop('reported_upc')

print(f'Before = {df_intervention.cache().count():,} (Candidate interventions to measure)')

# Create sales date for ever single date (required for rapidly joining to POS data)
df_intervention = df_intervention.withColumn(
    'duration',
    pyf.expr('intervention_end_day - intervention_start_day')
)

df_intervention = df_intervention.withColumn(
    'repeat',
    pyf.expr('split(repeat(",", duration), ",")')
)
df_intervention = df_intervention.select(
    '*',
    pyf.posexplode('repeat').alias('sales_dt', 'placeholder')
)
df_intervention = df_intervention.withColumn(
    'sales_dt',
    pyf.expr('date_add(measure_start, sales_dt)')
)

# Compute diff days columns
df_intervention = df_intervention.withColumn(
    'diff_day',
    pyf.datediff(pyf.col('sales_dt'), pyf.col('measure_start'))
)

# Drop unnecessary columns
df_intervention = df_intervention.drop('repeat', 'placeholder')

print(f'After = {df_intervention.cache().count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge POS and Intervention Data

# COMMAND ----------

# Filter POS data
min_date = df_intervention.select(pyf.min('sales_dt')).collect()[0].asDict().values()
max_date = df_intervention.select(pyf.max('sales_dt')).collect()[0].asDict().values()

min_date, max_date = list(min_date)[0], list(max_date)[0]
print(min_date, '|', max_date)

print(f'POS before = {df_pos.count():,}')
df_pos = df_pos.filter(
    (pyf.col('SALES_DT') >= min_date) &
    (pyf.col('SALES_DT') <= max_date)
)
print(f'POS after = {df_pos.cache().count():,}')

# COMMAND ----------

df_pos.select('RETAILER_ITEM_ID').distinct().sort('RETAILER_ITEM_ID').show()
df_intervention.select('epos_retailer_item_id').distinct().sort('epos_retailer_item_id').show()

# COMMAND ----------

# Merge datasets
df_merged = df_pos.join(
    df_intervention,
    (df_pos['SALES_DT'] == df_intervention['sales_dt']) &
    (df_pos['UPC'] == df_intervention['UPC']) &
    (df_pos['ORGANIZATION_UNIT_NUM'] == df_intervention['epos_organization_unit_num']),
    how='outer').drop(df_intervention['sales_dt']).drop(df_intervention['UPC'])
print(f'{df_merged.cache().count():,}')

# Clean data
df_merged = df_merged.withColumn(
    'is_intervention',
    pyf.col('standard_response_cd').isNotNull().cast('float'))
df_merged = df_merged.fillna({'standard_response_cd': 'none'})

# Filter out nonsense products
df_merged = df_merged.filter(df_merged['UPC'].isNotNull())
print(f'{df_merged.cache().count():,}')

# Filter out products with no interventions
pdf_mean_intervention = df_merged.select('UPC', 'is_intervention').groupby('UPC').mean().toPandas()
display(pdf_mean_intervention)
pdf_mean_intervention = pdf_mean_intervention[
    pdf_mean_intervention['avg(is_intervention)'] > 0]
allowed_item_set = set(pdf_mean_intervention['UPC'])
df_merged = df_merged.filter(
    df_merged['UPC'].isin(allowed_item_set))
print(f'{df_merged.cache().count():,}')

required_columns.append('UPC')
required_columns.remove('RETAILER_ITEM_ID')
required_columns.sort()
df_merged = df_merged.select(*required_columns)

# Cast to float
cat_features_list = ['ORGANIZATION_UNIT_NUM', 'UPC', 'DOW', 'standard_response_cd']
for col_name, col_type in df_merged.dtypes:
    if (col_type == 'bigint' or col_type == 'long' or col_type == 'double') and col_name not in cat_features_list:
        df_merged = df_merged.withColumn(
            col_name,
            df_merged[col_name].cast('float')
        )

# Check dataset size
n_samples = df_merged.cache().count()
print(f'{n_samples:,}')
if n_samples == 0:
    raise ValueError(
        'Dataset size is 0. Check NARs and ePOS data sources have specified correct `retailer_item_id`'
    )

# COMMAND ----------

display(df_merged)

# COMMAND ----------

# Do an inner join instead of the outer join that was used to construct df_merged
# This will allow us to get the interventions that have matching ePOS data on the UPC
df_merged_1 = df_pos.join(
    df_intervention,
    (df_pos['SALES_DT'] == df_intervention['sales_dt']) &
    (df_pos['UPC'] == df_intervention['UPC']) &
    (df_pos['ORGANIZATION_UNIT_NUM'] == df_intervention['epos_organization_unit_num']),
    how='inner').drop(df_intervention['sales_dt']).drop(df_intervention['UPC'])

partition_cols = [
    "mdm_country_id",
    "mdm_holding_id",
    "mdm_banner_id",
    "ORGANIZATION_UNIT_NUM",
    "mdm_client_id",
    "call_id",
    "UPC",
]

# This dataframe is needed for the audit table
df_intervention_1 = df_merged_1.dropDuplicates(partition_cols + ["response_id"])

print(
    f"{df_intervention_1.count():,} Interventions to measure that have matching ePOS data on the UPC"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start Audit Table

# COMMAND ----------

# Pre-processing for audit table

step_1 = df_intervention_pre_1.select(
    pyf.col("response_id").alias("measurable_standard_response_text")
)
step_2 = df_intervention_pre_2.select(
    pyf.col("response_id").alias("has_valid_epos_retailer_item_id")
)
step_3 = df_intervention_1.select(
    pyf.col("response_id").alias("has_matching_epos_data")
)

# Only consider recent data (currently set to most recent 3 months) for the audit table
df_intervention_pre = df_intervention_pre_0.where(pyf.col('call_date') >= (datetime.date.today() - relativedelta(months=3)).strftime(format="%Y-%m-%d"))

# Creating the tracking table for dropped interventions
df_stats = (
    df_intervention_pre.join(
        step_1,
        df_intervention_pre.response_id == step_1.measurable_standard_response_text,
        "left",
    )
    .join(
        step_2,
        df_intervention_pre.response_id == step_2.has_valid_epos_retailer_item_id,
        "left",
    )
    .join(
        step_3,
        df_intervention_pre.response_id == step_3.has_matching_epos_data,
        "left",
    )
)

print(f"Raw intervention Count = {df_stats.cache().count():,}")

# with 12 months the raw intervention count for Morrisons Nestle UK is 1,087,035

# COMMAND ----------

print('Intervention Summary Statistics')
df_merged.select('is_intervention').describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Save Results

# COMMAND ----------

# Write data
df_merged.write.format('delta')\
    .mode('overwrite')\
    .option('overwriteSchema', 'true')\
    .save(f'/mnt/processed/measurement/cache/{client_id}-{country_id}-{holding_id}-{banner_id}')
print(f'{df_merged.cache().count():,}')

df_intervention.write.format('delta')\
    .mode('overwrite')\
    .option('overwriteSchema', 'true')\
    .save(f'/mnt/processed/measurement/cache/{client_id}-{country_id}-{holding_id}-{banner_id}-intervention')
print(f'{df_intervention.cache().count():,}')

# COMMAND ----------

df_stats.write.format('delta')\
    .mode('overwrite')\
    .option('overwriteSchema', 'true')\
    .save(f'/mnt/processed/measurement/cache/{client_id}-{country_id}-{holding_id}-{banner_id}-audit-table-cache')
