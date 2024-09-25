# Databricks notebook source
# MAGIC %md
# MAGIC # Cache Measurement Data

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

from delta.tables import DeltaTable

print(acosta.__version__)

# COMMAND ----------

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
# MAGIC ### Load & Preprocess Data

# COMMAND ----------

# Get parameters from interventions_retailer_client_config table
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
config_dict = client_config.toPandas().to_dict('records')[0]
pprint(config_dict)

# COMMAND ----------

# MAGIC %md
# MAGIC ### POS data

# COMMAND ----------

# import only recent data (currently set to most recent 6 months)
today_date = datetime.date.today()
min_date = (today_date - relativedelta(months=6))
min_date_filter = min_date.strftime(format='%Y-%m-%d')
print(min_date_filter)

# COMMAND ----------

def populate_price(df):
    """
    Calculate the price and populate any null price backward and forward.
    Price is calculated as POS_AMT divided by POS_ITEM_QTY.
    The column PRICE is added to the dataframe.
    :param DataFrame df: Includes columns for POS_ITEM_QTY,
    POS_AMT, ORGANIZATION_UNIT_NUM, RETAILER_ITEM_ID, and SALES_DT.
    """
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
           pyf.when(
             pyf.col('PRICE').isNull(),
             pyf.round(pyf.avg(pyf.col('PRICE')).over(w), 2)
           ).otherwise(pyf.col('PRICE'))
    )
    return df

# COMMAND ----------

# This function definition, which adds a mechanism to fill-in 'null' price values, supercedes the get_pos_data function definition from the Acosta library
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
        df = df.where(pyf.col("SALES_DT") <= (datetime.date.today() + relativedelta(days=1)).strftime(format='%Y-%m-%d'))
        
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
    df = populate_price(df)
    return df

# COMMAND ----------

# Load POS data (with the nextgen processing function)
df_pos = get_pos_data(config_dict['epos_datavault_db_nm'], min_date_filter, spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ### LOESS baseline forecast data

# COMMAND ----------

# Load LOESS baseline quantities
df_sql_query_loess_baseline = f"""
    SELECT 
      HUB_ORGANIZATION_UNIT_HK,
      HUB_RETAILER_ITEM_HK,
      BASELINE_POS_ITEM_QTY,
      SALES_DT 
    FROM
      {config_dict['alertgen_im_db_nm']}.loess_forecast_baseline_unit
"""

df_loess_baseline = spark.sql(df_sql_query_loess_baseline)
df_loess_baseline = df_loess_baseline\
  .where(pyf.col('SALES_DT') >= min_date)\
  .where(pyf.col('SALES_DT') <= (datetime.date.today()\
        + relativedelta(days=1)).strftime(format='%Y-%m-%d'))

print(f'{df_loess_baseline.cache().count():,}')

# COMMAND ----------

#check before join
display(df_loess_baseline.select(pyf.max(pyf.col('SALES_DT'))))

# COMMAND ----------

print(f'{df_loess_baseline.count():,}')
print('--')
print(f'{df_loess_baseline.filter(pyf.col("BASELINE_POS_ITEM_QTY") > 0).count():,}')
print(f'{df_loess_baseline.filter(pyf.col("BASELINE_POS_ITEM_QTY") == 0).count():,}')
print(f'{df_loess_baseline.filter(pyf.col("BASELINE_POS_ITEM_QTY").isNull()).count():,}')
print(f'{df_loess_baseline.filter(pyf.col("BASELINE_POS_ITEM_QTY").isNull()).count()/df_loess_baseline.count():.2%}')
print('--')

print(f'''{df_loess_baseline.filter(pyf.col("BASELINE_POS_ITEM_QTY") > 0).count() + 
          df_loess_baseline.filter(pyf.col("BASELINE_POS_ITEM_QTY") == 0).count() +
          df_loess_baseline.filter(pyf.col("BASELINE_POS_ITEM_QTY").isNull()).count():,}
      ''')

# COMMAND ----------

df_pos_and_baseline = df_pos.join(
    df_loess_baseline,
    (df_pos['SALES_DT'] == df_loess_baseline['SALES_DT']) &
    (df_pos['HUB_ORGANIZATION_UNIT_HK'] == df_loess_baseline['HUB_ORGANIZATION_UNIT_HK']) &
    (df_pos['HUB_RETAILER_ITEM_HK'] == df_loess_baseline['HUB_RETAILER_ITEM_HK']),
    how = 'leftouter'
).drop(df_loess_baseline['HUB_ORGANIZATION_UNIT_HK']
).drop(df_loess_baseline['HUB_RETAILER_ITEM_HK']
).drop(df_loess_baseline['SALES_DT'])

columns_to_drop = ('HUB_ORGANIZATION_UNIT_HK', 'HUB_RETAILER_ITEM_HK',
                   'SAT_LINK_EPOS_SUMMARY_HDIFF', 'LINK_ePOS_Summary_HK',
                   'ON_HAND_INVENTORY_QTY', 'UNIT_PRICE', 'LOAD_TS', 'RECORD_SOURCE_CD')

df_pos_and_baseline = df_pos_and_baseline.drop(*columns_to_drop)

print(f'{df_pos_and_baseline.cache().count():,}')

# COMMAND ----------

display(df_pos_and_baseline)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Intervention Data

# COMMAND ----------

df_intervention = spark.sql(f'''
    SELECT * 
    FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
    WHERE
    mdm_country_id = {country_id} and
    mdm_client_id = {client_id} and
    mdm_holding_id = {holding_id} and
    coalesce(mdm_banner_id, -1) = {banner_id}
''')

# Filter recent interventions which have an identifiable intervention_id and retailer_item_id, and are actionable
df_intervention = df_intervention\
    .where(pyf.col('call_date') >= min_date_filter)\
    .filter(df_intervention['response_id'].isNotNull())\
    .filter(df_intervention['epos_retailer_item_id'].isNotNull())\
    .filter(df_intervention['actionable_flg'].isNotNull())\
    .withColumn('measure_start',
        pyf.expr('date_add(call_date, intervention_start_day)'))\
    .withColumn('measure_end',
        pyf.expr('date_add(call_date, intervention_end_day)')
    )

# Get max sales date
max_sales_date_filter = df_pos_and_baseline.select(pyf.max('SALES_DT')).collect()[0][0]
print(f'Date of latest POS data = {max_sales_date_filter}')

# Filter out interventions that have not completed yet based on the date of the latest available POS data
df_intervention = df_intervention.where(pyf.col('measure_end') <= max_sales_date_filter)

print(f'Before = {df_intervention.cache().count():,}')

# COMMAND ----------

df_intervention.write.format('delta')\
    .mode('overwrite')\
    .option('mergeSchema', 'true')\
    .save(f'/mnt/processed/loess_measurement/cache/{client_id}-{country_id}-{holding_id}-{banner_id}-intervention')

print(f'Intervention Count = {df_intervention.cache().count():,}')

# COMMAND ----------

window = Window.partitionBy(
    'mdm_country_id',
    'mdm_country_nm',
    'mdm_holding_id',
    'mdm_holding_nm',
    'mdm_banner_id',
    'mdm_banner_nm',
    'store_acosta_number',
    'epos_organization_unit_num',
    'mdm_client_id',
    'mdm_client_nm',
    'epos_retailer_item_id',
    'objective_typ',
    'call_date'  
    ).orderBy(df_intervention['intervention_rank'])

# Filter on the intervention with the lowest rank for a given store/item/date
df_intervention = df_intervention\
  .select('*', pyf.rank().over(window).alias('rank'))\
  .filter(pyf.col('rank') == 1)\
  .drop('rank')

print(f'Now = {df_intervention.cache().count():,}')

# COMMAND ----------

display(df_intervention)

# COMMAND ----------

# Create sales date for every single date (required for rapidly joining to POS data)
df_intervention_all_days = df_intervention\
  .withColumn('duration',
    pyf.expr('intervention_end_day - intervention_start_day'))\
  .withColumn('repeat',
    pyf.expr('split(repeat(",", duration), ",")'))\
  .select('*',
    pyf.posexplode('repeat').alias('sales_dt', 'placeholder'))\
  .withColumn('sales_dt',
    pyf.expr('date_add(measure_start, sales_dt)'))\
  .withColumn('diff_day',
    pyf.datediff(pyf.col('sales_dt'), pyf.col('call_date')))\
  .drop('repeat', 'placeholder')

print(f'After = {df_intervention_all_days.cache().count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge POS and Intervention Data

# COMMAND ----------

df_merged = df_intervention_all_days.join(
    df_pos_and_baseline,
    (df_pos_and_baseline['SALES_DT'] == df_intervention_all_days['sales_dt']) &
    (df_pos_and_baseline['RETAILER_ITEM_ID'] == df_intervention_all_days['epos_retailer_item_id']) &
    (df_pos_and_baseline['ORGANIZATION_UNIT_NUM'] == df_intervention_all_days['epos_organization_unit_num']),
    how = 'inner'
).drop(df_pos_and_baseline['ORGANIZATION_UNIT_NUM']
).drop(df_pos_and_baseline['RETAILER_ITEM_ID']
).drop(df_pos_and_baseline['SALES_DT'])

df_merged = df_merged.fillna({'standard_response_cd': 'none'})

# Cast to float
cat_features_list = ['mdm_country_id', 'mdm_holding_id', 'mdm_banner_id',
                     'mdm_client_id', 'store_acosta_number' 'epos_organization_unit_num',
                     'epos_retailer_item_id', 'standard_response_cd']
for col_name, col_type in df_merged.dtypes:
    if (col_type == 'bigint' or col_type == 'long' or col_type == 'double') and col_name not in cat_features_list:
        df_merged = df_merged.withColumn(
            col_name,
            df_merged[col_name].cast('float')
        )

n_samples = df_merged.cache().count()
print(f'{n_samples:,}')

if n_samples == 0:
    raise ValueError('Dataset size is 0. Check NARs and ePOS data sources have specified correct `retailer_item_id`')

# COMMAND ----------

display(df_merged)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Cached Data

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply LOESS Value Measurement Formulas

# COMMAND ----------

# This is akin to the intervention effect in IVM
# If day >= 0 then it's considered an uplift intervention, so we take the actual sales quantity minus the baseline sales quantity
# If day < 0 then it's considered a recovery intervention, so we take the baseline sales quantity minus the actual sales quantity

df_results_by_day = df_merged.withColumn(
        'DIFF_POS_ITEM_QTY',
        pyf.when(pyf.col('diff_day') >= 0,
                 pyf.col('POS_ITEM_QTY') - pyf.col('BASELINE_POS_ITEM_QTY')
                ).otherwise(pyf.col('BASELINE_POS_ITEM_QTY') - pyf.col('POS_ITEM_QTY')
                           ).cast(pyt.DecimalType(15, 2))
)
# If the result is 0 or negative, then we set it to NULL so that we ignore it later on, because that's how the UK formula does it
df_results_by_day = df_results_by_day.withColumn(
        'DIFF_POS_ITEM_QTY',
        pyf.when(pyf.col('DIFF_POS_ITEM_QTY') <= 0, pyf.lit(None)
                ).otherwise(pyf.col('DIFF_POS_ITEM_QTY'))
)
display(df_results_by_day)

# COMMAND ----------

# Get the average average intervention effect of the intervention days where intervention effect > 0 and the average price of all the intervention days
df_results = df_results_by_day\
  .groupBy('mdm_country_id', 
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
           'duration')\
  .agg(pyf.avg('DIFF_POS_ITEM_QTY').alias('AVG_DIFF_POS_ITEM_QTY'),
       pyf.avg('PRICE').alias('AVG_PRICE'))\
  .orderBy(['epos_organization_unit_num', 'epos_retailer_item_id',
            'call_date', 'response_id'], ascending=True)

# COMMAND ----------

# This is the average intervention effect of the days where there was a net positive intervention effect
# The UK calls this average uplift amount
# uplift_value: This is akin to the average uplift value (in pounds) in UK speak
# uplift_total: This is akin to the total uplift value (in pounds) in UK speak
# total_intervention_effect: This is akin to the total uplift amount in UK speak
# total_impact: This is akin to the total uplift value (in pounds) in UK speak

df_results = df_results.withColumn('AVG_DIFF_POS_ITEM_QTY',
    pyf.when(pyf.col('AVG_DIFF_POS_ITEM_QTY').isNull(), 0).otherwise(pyf.col('AVG_DIFF_POS_ITEM_QTY'))
).withColumn('AVG_PRICE',
    pyf.when(pyf.col('AVG_PRICE').isNull(), 0).otherwise(pyf.col('AVG_PRICE'))
).withColumn('uplift_value',
    pyf.col('AVG_DIFF_POS_ITEM_QTY') * pyf.col('AVG_PRICE')
).withColumn('uplift_total',
    pyf.col('uplift_value') * pyf.col('duration')
).withColumn('total_intervention_effect',
    pyf.col('AVG_DIFF_POS_ITEM_QTY') * pyf.col('duration')
).withColumn('total_impact',
    pyf.col('total_intervention_effect') * pyf.col('AVG_PRICE')
)
print(f'Results Count = {df_results.cache().count():,}')

# COMMAND ----------

display(df_results)

# COMMAND ----------

# Write data
df_results.write.format('delta')\
    .mode('overwrite')\
    .option('mergeSchema', 'true')\
    .save(f'/mnt/processed/loess_measurement/results/{client_id}-{country_id}-{holding_id}-{banner_id}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results in Database

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

# Create the output dataframe that will be used to write to the ds_intervention_summary table
df_summary_table = df_results.withColumn('is_complete', pyf.lit(True))\
  .withColumn('total_qintervention_effect', pyf.lit(None))\
  .withColumn('total_qimpact', pyf.lit(None))\
  .withColumn('load_ts', pyf.current_timestamp())\
  .withColumnRenamed('duration', 'measurement_duration')

# Ensure column order
df_summary_table = df_summary_table.select(
    'mdm_country_nm', 'mdm_holding_nm', 'mdm_banner_nm', 'mdm_client_nm',
    'store_acosta_number', 'epos_organization_unit_num', 'epos_retailer_item_id',
    'objective_typ', 'call_id', 'response_id',
    'nars_response_text', 'standard_response_text', 'standard_response_cd',
    'measurement_duration', 'is_complete','total_intervention_effect',
    'total_qintervention_effect', 'total_impact', 'total_qimpact', 'load_ts',
    'mdm_country_id', 'mdm_holding_id', 'mdm_banner_id', 'mdm_client_id',
    'call_date'
)

df_summary_table.cache()
df_summary_table.printSchema()

# COMMAND ----------

display(df_summary_table)

# COMMAND ----------

# Save without overwriting previously completed
completed_response_ids = spark.sql(f'''
    select response_id, is_complete
    from acosta_retail_analytics_im.ds_intervention_summary
    where is_complete = true
    and mdm_client_id = {client_id}
''')
completed_response_ids = completed_response_ids.select('response_id').distinct().toPandas()['response_id']
completed_response_ids = set(completed_response_ids)

# Filter out completed interventions
df_summary_table = df_summary_table.filter(~df_summary_table['response_id'].isin(completed_response_ids))

# COMMAND ----------

# Save the new interventions data
df_summary_table = df_summary_table.alias('update')
delta_summary_table = DeltaTable\
    .forName(spark, 'acosta_retail_analytics_im.ds_intervention_summary')\
    .alias('source')

delta_summary_table.merge(
    df_summary_table,
    '''
    source.mdm_banner_id = update.mdm_banner_id and
    source.mdm_client_id = update.mdm_client_id and
    source.mdm_country_id = update.mdm_country_id and
    source.mdm_holding_id = update.mdm_holding_id and
    source.response_id = update.response_id and
    source.standard_response_text = update.standard_response_text
    '''
).whenMatchedUpdateAll()\
.whenNotMatchedInsertAll()\
.execute()
