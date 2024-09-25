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

# import datetime
# import warnings
# from dateutil.relativedelta import relativedelta

# import numpy as np
# import pandas as pd
# import pyspark.sql.functions as pyf
# import pyspark.sql.types as pyt
# from pyspark.sql import DataFrame
# from pyspark.sql.window import Window

# from acosta.alerting.helpers import check_path_exists
# from acosta.alerting.helpers import features as acosta_features

# from acosta.alerting.preprocessing.functions import get_pos_data
# from acosta.measurement import required_columns, process_notebook_inputs

# import acosta

# print(acosta.__version__)

# COMMAND ----------

today_date = datetime.date.today()

# max_sales_date_filter = (today_date - datetime.timedelta(days=180)).strftime(format='%Y-%m-%d')
max_sales_date_filter = (today_date - relativedelta(years=2)).strftime(format='%Y-%m-%d')

print(max_sales_date_filter)

# COMMAND ----------

def get_pos_data(pos_database: str, spark):
    """
    Takes in a POS DataFrame and:
    - Explodes data to include all days between min and max of original dataframe.
    - Computes Price and a lag/leading price.
    - Relies on a global DATE_FIELD being defined.

    :param pos_database: Name of the database the POS data is in
    :param spark: Spark instance
    :return:
    """
    try:
        # Gen 2 version of getting the POS data
        df = spark.sql(f'select * from {pos_database}.vw_latest_sat_epos_summary')
        
        print(f'{df.cache().count():,}')
        
        df = df.where(pyf.col("SALES_DT") >= max_sales_date_filter)
        
        print(f'{df.cache().count():,}')
        
    except Exception:
        # Deprecated version of getting the POS data
        warnings.warn('Deprecated POS data format detected. Please update to Gen 2 POS data format')
        df = spark.sql(f'select * from {pos_database}.vw_sat_link_epos_summary')
        
        print(f'{df.cache().count():,}')
        
        df = df.where(pyf.col("SALES_DT") >= max_sales_date_filter)
        
        print(f'{df.cache().count():,}')

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

#     # Polish POS data
#     df = df.withColumn(
#         'UNIT_PRICE',
#         df['POS_AMT'] / df['POS_ITEM_QTY']
#     )
#     df = df.withColumn(
#         'POS_ITEM_QTY',
#         pyf.when(pyf.col('POS_ITEM_QTY') >= 0, pyf.col('POS_ITEM_QTY')).otherwise(0)
#     )
#     df = df.withColumn(
#         'POS_AMT',
#         pyf.when(pyf.col('POS_AMT') >= 0, pyf.col('POS_AMT')).otherwise(0)
#     )

#     # Casting
#     df = cast_decimal_to_number(df, cast_to='float')
#     df = df.withColumn('ORGANIZATION_UNIT_NUM', df['ORGANIZATION_UNIT_NUM'].cast('string'))

#     df = _all_possible_days(df, 'SALES_DT', ['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'])

#     # Fill in the zeros for On hand inventory quantity
#     df = df.withColumn(
#         'ON_HAND_INVENTORY_QTY',
#         pyf.when(
#             pyf.col('ON_HAND_INVENTORY_QTY') < 0,
#             pyf.lit(0)
#         ).otherwise(
#             pyf.col('ON_HAND_INVENTORY_QTY')
#         )
#     )

#     df = _replace_negative_and_null_with(df, ['POS_ITEM_QTY', 'POS_AMT'], 0)

#     # Very important to get all of the window functions using this partitioning to be done at the same time
#     # This will minimize the amount of shuffling being done
#     window_item_store = Window.partitionBy(['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM']).orderBy('SALES_DT')

#     for lag in acosta_features.get_lag_days():
#         df = df.withColumn(
#             f'LAG_UNITS_{lag}',
#             pyf.lag(pyf.col('POS_ITEM_QTY'), lag).over(window_item_store)
#         )

#         if lag <= 14:
#             df = df.withColumn(
#                 f'LAG_INV_{lag}',
#                 pyf.lag(pyf.col('ON_HAND_INVENTORY_QTY'), lag).over(window_item_store)
#             )

#     df = compute_price_and_lag_lead_price(df)

#     # Use the Lag_INV columns to generate RECENT_ON_HAND_INVENTORY_QTY
#     # The Lag_INV columns are dropped after this command and are no longer referenced
#     df = df.withColumn('RECENT_ON_HAND_INVENTORY_QTY', pyf.coalesce(
#         pyf.when(pyf.isnan(pyf.col('LAG_INV_1')), None).otherwise(pyf.col('LAG_INV_1')),
#         pyf.when(pyf.isnan(pyf.col('LAG_INV_2')), None).otherwise(pyf.col('LAG_INV_2')),
#         pyf.when(pyf.isnan(pyf.col('LAG_INV_3')), None).otherwise(pyf.col('LAG_INV_3')),
#         pyf.when(pyf.isnan(pyf.col('LAG_INV_4')), None).otherwise(pyf.col('LAG_INV_4')),
#         pyf.when(pyf.isnan(pyf.col('LAG_INV_5')), None).otherwise(pyf.col('LAG_INV_5')),
#         pyf.when(pyf.isnan(pyf.col('LAG_INV_6')), None).otherwise(pyf.col('LAG_INV_6')),
#         pyf.when(pyf.isnan(pyf.col('LAG_INV_7')), None).otherwise(pyf.col('LAG_INV_7')),
#         pyf.when(pyf.isnan(pyf.col('LAG_INV_8')), None).otherwise(pyf.col('LAG_INV_8')),
#         pyf.when(pyf.isnan(pyf.col('LAG_INV_9')), None).otherwise(pyf.col('LAG_INV_9')),
#         pyf.when(pyf.isnan(pyf.col('LAG_INV_10')), None).otherwise(pyf.col('LAG_INV_10')),
#         pyf.when(pyf.isnan(pyf.col('LAG_INV_11')), None).otherwise(pyf.col('LAG_INV_11')),
#         pyf.when(pyf.isnan(pyf.col('LAG_INV_12')), None).otherwise(pyf.col('LAG_INV_12')),
#         pyf.when(pyf.isnan(pyf.col('LAG_INV_13')), None).otherwise(pyf.col('LAG_INV_13')),
#         pyf.when(pyf.isnan(pyf.col('LAG_INV_14')), None).otherwise(pyf.col('LAG_INV_14'))
#     )).drop(*[f'LAG_INV_{i}' for i in range(1, 15)])

#     # The RECENT_ON_HAND_INVENTORY_DIFF is the prior day end's inventory minus two days ago
#     # RECENT_ON_HAND_INVENTORY_QTY is at least the prior day's ending inventory
#     df = df.withColumn(
#         'RECENT_ON_HAND_INVENTORY_DIFF',
#         pyf.col('RECENT_ON_HAND_INVENTORY_QTY')\
#         - pyf.lag(pyf.col('RECENT_ON_HAND_INVENTORY_QTY'), 1).over(window_item_store)
#     )

#     # Add day of features
#     df = df.withColumn('DOW', pyf.dayofweek('SALES_DT'))
#     df = df.withColumn('DOM', pyf.dayofmonth('SALES_DT'))
#     df = df.withColumn('DOY', pyf.dayofyear('SALES_DT'))

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### POS data

# COMMAND ----------

df_pos = get_pos_data(config_dict['epos_datavault_db_nm'], spark)

# COMMAND ----------

print(f'{df_pos.cache().count():,}')

# COMMAND ----------


