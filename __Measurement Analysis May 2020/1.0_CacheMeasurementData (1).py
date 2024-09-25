# Databricks notebook source
# MAGIC %sql
# MAGIC select * from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC   mdm_country_id = 1 AND
# MAGIC   mdm_client_id = 16161 AND
# MAGIC   mdm_holding_id = 71 AND
# MAGIC   coalesce(mdm_banner_id, -1) = -1

# COMMAND ----------

# MAGIC %md
# MAGIC # Cache Measurement Data
# MAGIC
# MAGIC Write the data that `2.0_Fitting` notebook needs to run

# COMMAND ----------

from pprint import pprint

import pyspark.sql.functions as pyf

from acosta.alerting.preprocessing.functions import get_pos_data
from acosta.measurement import required_columns, process_notebook_inputs

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
# MAGIC ### POS data

# COMMAND ----------

# Load POS data (with the nextgen processing function)
df_pos = get_pos_data(config_dict['epos_datavault_db_nm'], spark)

df_pos = df_pos.fillna({
    'ON_HAND_INVENTORY_QTY': 0,
    'RECENT_ON_HAND_INVENTORY_QTY': 0,
    'RECENT_ON_HAND_INVENTORY_DIFF': 0
})

print(f'{df_pos.cache().count():,}')

# COMMAND ----------

display(df_pos)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Intervention Data

# COMMAND ----------

# Example Structure
df_intervention = spark.sql(f'''
    SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
    WHERE
    mdm_country_id = {country_id} and
    mdm_client_id = {client_id} and
    mdm_holding_id = {holding_id} and
    coalesce(mdm_banner_id, -1) = {banner_id}
''')

print(f'Before = {df_intervention.cache().count():,}')

df_intervention = df_intervention.withColumn(
    'measure_start',
    pyf.expr('date_add(call_date, intervention_start_day)')
)
df_intervention = df_intervention.withColumn(
    'measure_end',
    pyf.expr('date_add(call_date, intervention_end_day)')
)

# COMMAND ----------

display(df_intervention)

# COMMAND ----------

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

display(df_intervention)

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
    (df_pos['RETAILER_ITEM_ID'] == df_intervention['epos_retailer_item_id']) &
    (df_pos['ORGANIZATION_UNIT_NUM'] == df_intervention['epos_organization_unit_num']),
    how='outer'
).drop(df_intervention['sales_dt'])
print(f'{df_merged.cache().count():,}')

# Clean data
df_merged = df_merged.withColumn('is_intervention', pyf.col('standard_response_cd').isNotNull().cast('float'))
df_merged = df_merged.fillna({'standard_response_cd': 'none'})

# Filter out nonsense products
df_merged = df_merged.filter(df_merged['RETAILER_ITEM_ID'].isNotNull())
print(f'{df_merged.cache().count():,}')

# Filter out products with no interventions
pdf_mean_intervention = df_merged.select('RETAILER_ITEM_ID', 'is_intervention').groupby('RETAILER_ITEM_ID').mean().toPandas()
display(pdf_mean_intervention)
pdf_mean_intervention = pdf_mean_intervention[pdf_mean_intervention['avg(is_intervention)'] > 0]
allowed_item_set = set(pdf_mean_intervention['RETAILER_ITEM_ID'])
df_merged = df_merged.filter(df_merged['RETAILER_ITEM_ID'].isin(allowed_item_set))
print(f'{df_merged.cache().count():,}')

required_columns.sort()
df_merged = df_merged.select(*required_columns)

# Cast to float
cat_features_list = ['ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID', 'DOW', 'standard_response_cd']
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
    raise ValueError('Dataset size is 0. Check NARs and ePOS data sources have specified correct `retailer_item_id`')

# COMMAND ----------

display(df_merged)

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
