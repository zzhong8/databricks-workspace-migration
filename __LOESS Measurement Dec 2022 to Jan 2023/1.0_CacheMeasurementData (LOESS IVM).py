# Databricks notebook source
# MAGIC %md
# MAGIC # Cache Measurement Data
# MAGIC
# MAGIC Write the data that `2.0_ApplyMeasurementFormulas` notebook needs to run

# COMMAND ----------

from pprint import pprint

import datetime
import warnings
from dateutil.relativedelta import relativedelta

from pyspark.sql import Window
import pyspark.sql.functions as pyf
import pyspark.sql.types as pyt

from acosta.alerting.preprocessing.functions import _all_possible_days
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

# MAGIC %md
# MAGIC ### Load & Preprocess Data

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

# today_date = datetime.date.today()
today_date = datetime.date(2022, 11, 30) 

min_date = (today_date - relativedelta(months=2))
min_date_filter = min_date.strftime(format='%Y-%m-%d')
# min_date_filter = (today_date - relativedelta(years=2)).strftime(format='%Y-%m-%d')

print(min_date_filter)

# COMMAND ----------

def populate_price_new(df):
    """
    Calculate the price and populate any null price backward and forward.
    Price is calculated as POS_AMT divided by POS_ITEM_QTY.
    The column PRICE is added to the dataframe.
    :param DataFrame df: Includes columns for POS_ITEM_QTY,
    POS_AMT, ORGANIZATION_UNIT_NUM, RETAILER_ITEM_ID, and SALES_DT.
    """
    print("POPULATING PRICE (NEW)")
    df = df.withColumn(
        'PRICE',
        pyf.when(
            (pyf.col('POS_ITEM_QTY') == 0) | (pyf.col('POS_AMT') == 0) |
            (pyf.isnan(pyf.col('POS_ITEM_QTY'))) |
            (pyf.isnan(pyf.col('POS_AMT'))),
            None).otherwise(pyf.col('POS_AMT') / pyf.col('POS_ITEM_QTY')).cast(
                pyt.DecimalType(15, 2)))
    window_spec_forward = Window\
        .partitionBy(
            'RETAILER_ITEM_ID',
            'ORGANIZATION_UNIT_NUM'
        )\
        .orderBy('SALES_DT')\
        .rowsBetween(0, 1_000_000)  # a random large number

    window_spec_backward = Window\
        .partitionBy(
            'RETAILER_ITEM_ID',
            'ORGANIZATION_UNIT_NUM'
        )\
        .orderBy('SALES_DT')\
        .rowsBetween(-1_000_000, 0)  # a random large number

    # Fill backward
    df = df.withColumn(
        'FILLED_PRICE',
        pyf.last('PRICE', ignorenulls=True).over(window_spec_backward))
    # Fill forward
    df = df.withColumn(
        'PRICE',
        pyf.first(
            'FILLED_PRICE',
            ignorenulls=True).over(window_spec_forward)).drop('FILLED_PRICE')
    
    # *** Fill any remaining price ***
    w = Window.partitionBy(df.RETAILER_ITEM_ID)
    
    df = df.withColumn('PRICE',
        pyf.when(pyf.col('PRICE').isNull(), pyf.round(pyf.avg(pyf.col('PRICE')).over(w), 2)).otherwise(pyf.col('PRICE'))
    )
    
    return df

# COMMAND ----------

def get_pos_data(pos_database: str, min_date: str, spark):
    """
    Takes in a POS DataFrame and:
    - Explodes data to include all days between min and max of original dataframe.
    - Computes Price and a lag/leading price.
    - Relies on a global DATE_FIELD being defined.

    :param pos_database: Name of the database the POS data is in
    :param min_date: The oldest date for which we will import POS data
    :param spark: Spark instance
    :return:
    """
    try:
        # Gen 2 version of getting the POS data
        df = spark.sql(f'select * from {pos_database}.vw_latest_sat_epos_summary')
        df = df.where(pyf.col("SALES_DT") >= min_date)
        df = df.where(pyf.col("SALES_DT") <= (today_date + relativedelta(days=1)).strftime(format='%Y-%m-%d'))
        
    except Exception:
        # Deprecated version of getting the POS data
        warnings.warn('Deprecated POS data format detected. Please update to Gen 2 POS data format')
        df = spark.sql(f'select * from {pos_database}.vw_sat_link_epos_summary')
        df = df.where(pyf.col("SALES_DT") >= min_date)
        df = df.where(pyf.col("SALES_DT") <= (today_date + relativedelta(days=1)).strftime(format='%Y-%m-%d'))

        retailer_items = spark.sql(f'''
            select RETAILER_ITEM_ID, HUB_RETAILER_ITEM_HK
            from {pos_database}.hub_retailer_item
        ''')

        stores_names = spark.sql(f'''
            select ORGANIZATION_UNIT_NUM, HUB_ORGANIZATION_UNIT_HK 
            from {pos_database}.hub_organization_unit
        ''')

        # Join data
        df = df.join(
            retailer_items,
            df['HUB_RETAILER_ITEM_HK'] == retailer_items['HUB_RETAILER_ITEM_HK'],
            'left_outer'
        ).drop(retailer_items['HUB_RETAILER_ITEM_HK'])

        df = df.join(
            stores_names,
            df['HUB_ORGANIZATION_UNIT_HK'] == stores_names['HUB_ORGANIZATION_UNIT_HK'],
            'left_outer'
        ).drop(stores_names['HUB_ORGANIZATION_UNIT_HK'])

    # Polish POS data
    df = df.withColumn(
        'UNIT_PRICE',
        df['POS_AMT'] / df['POS_ITEM_QTY']
    )
    df = df.withColumn(
        'POS_ITEM_QTY',
        pyf.when(pyf.col('POS_ITEM_QTY') >= 0, pyf.col('POS_ITEM_QTY')).otherwise(0)
    )
    df = df.withColumn(
        'POS_AMT',
        pyf.when(pyf.col('POS_AMT') >= 0, pyf.col('POS_AMT')).otherwise(0)
    )

    # Casting
    for col_name, col_type in df.dtypes:
        if 'decimal' in col_type:
            df = df.withColumn(col_name, df[col_name].cast('float'))
    
    df = df.withColumn('ORGANIZATION_UNIT_NUM', df['ORGANIZATION_UNIT_NUM'].cast('string'))

    df = _all_possible_days(df, 'SALES_DT', ['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'])

    df = populate_price_new(df)
    
    return df

# COMMAND ----------

# Load POS data (with the nextgen processing function)
df_pos = get_pos_data(config_dict['epos_datavault_db_nm'], min_date_filter, spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ### LOESS baseline forecast data

# COMMAND ----------

# Load LOESS baseline quantities
df_sql_query_loess_baseline = """
    SELECT HUB_ORGANIZATION_UNIT_HK, HUB_RETAILER_ITEM_HK, BASELINE_POS_ITEM_QTY, SALES_DT from
    {alertgen_im_db_nm}.loess_forecast_baseline_unit
""".format(alertgen_im_db_nm=config_dict['alertgen_im_db_nm'])

df_loess_baseline = spark.sql(df_sql_query_loess_baseline)

df_loess_baseline = df_loess_baseline.where(pyf.col("SALES_DT") >= min_date)
df_loess_baseline = df_loess_baseline.where(pyf.col("SALES_DT") <= (datetime.date.today() + relativedelta(days=1)).strftime(format='%Y-%m-%d'))

print(f'{df_loess_baseline.cache().count():,}')

# COMMAND ----------

# Merge datasets
df_pos_and_baseline = df_pos.join(
    df_loess_baseline,
    (df_pos['SALES_DT'] == df_loess_baseline['SALES_DT']) &
    (df_pos['HUB_ORGANIZATION_UNIT_HK'] == df_loess_baseline['HUB_ORGANIZATION_UNIT_HK']) &
    (df_pos['HUB_RETAILER_ITEM_HK'] == df_loess_baseline['HUB_RETAILER_ITEM_HK']),
    how = 'leftouter'
).drop(df_loess_baseline['HUB_ORGANIZATION_UNIT_HK']).drop(df_loess_baseline['HUB_RETAILER_ITEM_HK']).drop(df_loess_baseline['SALES_DT'])

# Drop unnecessary columns
columns_to_drop = ('HUB_ORGANIZATION_UNIT_HK', 'HUB_RETAILER_ITEM_HK', 'SAT_LINK_EPOS_SUMMARY_HDIFF', 'LINK_ePOS_Summary_HK', 'ON_HAND_INVENTORY_QTY', 'UNIT_PRICE', 'LOAD_TS', 'RECORD_SOURCE_CD')

df_pos_and_baseline = df_pos_and_baseline.drop(*columns_to_drop)

print(f'{df_pos_and_baseline.cache().count():,}')

# COMMAND ----------

display(df_pos_and_baseline)

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

# Filter for only recent interventions
df_intervention = df_intervention.where(pyf.col("call_date") >= min_date_filter)

# Filter for only recent interventions that have an identifiable intervention id
df_intervention = df_intervention.filter(df_intervention['response_id'].isNotNull())

# Filter for only recent interventions that have an identifiable retailer item number
df_intervention = df_intervention.filter(df_intervention['epos_retailer_item_id'].isNotNull())

# Filter for only recent interventions that are actionable
df_intervention = df_intervention.filter(df_intervention['actionable_flg'].isNotNull())

df_intervention = df_intervention.withColumn(
    'measure_start',
    pyf.expr('date_add(call_date, intervention_start_day)')
)
df_intervention = df_intervention.withColumn(
    'measure_end',
    pyf.expr('date_add(call_date, intervention_end_day)')
)

# Get max sales date
max_sales_date_filter = df_pos_and_baseline.select(pyf.max('SALES_DT')).collect()[0][0]

print(f'Date of latest POS data = {max_sales_date_filter}')

# Filter out interventions that have not completed yet based on the date of the latest available POS data
df_intervention = df_intervention.where(pyf.col("measure_end") <= max_sales_date_filter)

print(f'Before = {df_intervention.cache().count():,}')

# COMMAND ----------

display(df_intervention)

# COMMAND ----------

df_intervention.printSchema()

# df_intervention_date_min_max = df_intervention.select(pyf.min('measure_start'), pyf.max('measure_end')).collect()

# df_intervention_date_min = df_intervention_date_min_max[0][0]
# df_intervention_date_max = df_intervention_date_min_max[0][1]

# print(df_intervention_date_min)
# print(df_intervention_date_max)

# COMMAND ----------

# Create sales date for every single date (required for rapidly joining to POS data)
df_intervention = df_intervention.withColumn(
    'duration',
    pyf.expr('intervention_end_day - intervention_start_day')
)
df_intervention_mod = df_intervention.withColumn(
    'repeat',
    pyf.expr('split(repeat(",", duration), ",")')
)
df_intervention_all_days = df_intervention_mod.select(
    '*',
    pyf.posexplode('repeat').alias('sales_dt', 'placeholder')
)
df_intervention_all_days = df_intervention_all_days.withColumn(
    'sales_dt',
    pyf.expr('date_add(measure_start, sales_dt)')
)

# Compute diff days columns
df_intervention_all_days = df_intervention_all_days.withColumn(
    'diff_day',
    pyf.datediff(pyf.col('sales_dt'), pyf.col('measure_start'))
)

# Drop unnecessary columns
df_intervention_all_days = df_intervention_all_days.drop('repeat', 'placeholder')

print(f'After = {df_intervention_all_days.cache().count():,}')

# COMMAND ----------

display(df_intervention_all_days)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge POS and Intervention Data

# COMMAND ----------

# Merge datasets
df_merged = df_intervention_all_days.join(
    df_pos_and_baseline,
    (df_pos_and_baseline['SALES_DT'] == df_intervention_all_days['sales_dt']) &
    (df_pos_and_baseline['RETAILER_ITEM_ID'] == df_intervention_all_days['epos_retailer_item_id']) &
    (df_pos_and_baseline['ORGANIZATION_UNIT_NUM'] == df_intervention_all_days['epos_organization_unit_num']),
    how = 'inner'
).drop(df_pos_and_baseline['ORGANIZATION_UNIT_NUM']).drop(df_pos_and_baseline['RETAILER_ITEM_ID']).drop(df_pos_and_baseline['SALES_DT'])

# Clean data
df_merged = df_merged.fillna({'standard_response_cd': 'none'})

# Cast to float
cat_features_list = ['mdm_country_id', 'mdm_holding_id', 'mdm_banner_id', 'mdm_client_id', 'store_acosta_number' 'epos_organization_unit_num', 'epos_retailer_item_id', 'standard_response_cd']
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

df_merged.printSchema()

# df = df_merged.where('ORGANIZATION_UNIT_NUM == 4126')

# df = df.where('RETAILER_ITEM_ID == 5502716')

# display(df)

# df2 = df_merged.where('ORGANIZATION_UNIT_NUM == 4126')

# df2 = df2.where('RETAILER_ITEM_ID == 5557994')

# display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC # Save Results

# COMMAND ----------

# Write data
df_intervention.write.format('delta')\
    .mode('overwrite')\
    .option('mergeSchema', 'true')\
    .save(f'/mnt/processed/loess_measurement/cache/{client_id}-{country_id}-{holding_id}-{banner_id}-intervention')

print(f'Intervention Count = {df_intervention.cache().count():,}')

df_intervention_all_days.write.format('delta')\
    .mode('overwrite')\
    .option('mergeSchema', 'true')\
    .save(f'/mnt/processed/loess_measurement/cache/{client_id}-{country_id}-{holding_id}-{banner_id}-intervention-all-days')

print(f'Intervention All Days Count = {df_intervention_all_days.cache().count():,}')

df_merged.write.format('delta')\
    .mode('overwrite')\
    .option('mergeSchema', 'true')\
    .save(f'/mnt/processed/loess_measurement/cache/{client_id}-{country_id}-{holding_id}-{banner_id}')

print(f'Merged Count = {df_merged.cache().count():,}')
