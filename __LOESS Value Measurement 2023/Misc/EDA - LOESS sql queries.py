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

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED acosta_retail_analytics_im.vw_ds_intervention_input_dla

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED acosta_retail_analytics_im.vw_ds_intervention_input_uk_opportunities

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN select * from acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars TABLESAMPLE (0.01 PERCENT) REPEATABLE (42)

# COMMAND ----------

# MAGIC %sql
# MAGIC UNCACHE table acosta_retail_analytics_im.vw_ds_intervention_input_nars;
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars TABLESAMPLE (0.01 PERCENT) REPEATABLE (42)
