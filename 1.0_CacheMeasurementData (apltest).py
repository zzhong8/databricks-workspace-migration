# Databricks notebook source
# MAGIC %md
# MAGIC # Cache Measurement Data
# MAGIC
# MAGIC Write the data that `2.0_Fitting` notebook needs to run

# COMMAND ----------

from pprint import pprint

import datetime
import warnings

import numpy as np
import pandas as pd
import pyspark.sql.functions as pyf
import pyspark.sql.types as pyt
from pyspark.sql import DataFrame
from pyspark.sql.window import Window


from acosta.alerting.helpers import check_path_exists
from acosta.alerting.helpers import features as acosta_features

from dateutil.relativedelta import relativedelta

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

today_date = datetime.date.today()

min_date_filter = (today_date - relativedelta(years=8)).strftime(format='%Y-%m-%d')
# min_date_filter = (today_date - relativedelta(years=2)).strftime(format='%Y-%m-%d')

# COMMAND ----------

# Note: As as 2023-03-14 the view default.vw_sat_link_epos_summary_apltest contains only Morrisons Nestle UK ePOS data

def _list_days(start_date, end_date):
    """
      Take an RDD Row with startDate and endDate and produce a list of
      all days including and between start and endDate.

      :return: list of all possible days including and between start and end date.
    """
    day_range = (end_date - start_date).days + 1
    return [start_date + datetime.timedelta(i) for i in range(day_range)]
  
def cast_decimal_to_number(df: DataFrame, cast_to='double'):
    """
    Finds and casts decimal dtypes to another pandas_udf friendly number type

    :param df:
    :param cast_to:
    :return:
    """
    for col_name, col_type in df.dtypes:
        if 'decimal' in col_type:
            df = df.withColumn(col_name, df[col_name].cast(cast_to))
    return df
  
def _all_possible_days(df, date_field, join_fields_list):
    """
    Return the data frame with all dates represented from
    the min and max of the <date_field>.

    :param DataFrame df:
    :param string date_field: The column name in <df>
    containing a date type.
    :param list(string) join_fields_list: A list of columns
    that uniquely represent a row.

    :return DataFrame:
    """
    min_max_date = df.groupBy('ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID').agg(
        pyf.min(date_field).alias('startDate'),
        pyf.max(date_field).alias('endDate')
    )

    # TODO (2/20/2020) This should be moved into a Dataframe rather than RDD to increase the chance of pipelining
    explode_dates = min_max_date.rdd \
        .map(lambda r: (r.ORGANIZATION_UNIT_NUM, r.RETAILER_ITEM_ID, _list_days(r.startDate, r.endDate))) \
        .toDF(['ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID', 'dayList']) \
        .select('ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID', pyf.explode(pyf.col('dayList')).alias(date_field))

    return explode_dates.join(df, [date_field] + join_fields_list, 'left')
  
# Not necessary when using intermediate data that has any negatives and zeroes removed.
def _replace_negative_and_null_with(df, cols, replacement_constant):
    """
    Given a DataFrame and a set of columns, replace any negative value
      and null values with the replacement_constant.  Selects only
      columns that are of a numeric Spark data type (DecimalType, DoubleType,
      FloatType, IntegerType, LongType, ShortType).

    :param DataFrame df: Contains the columns referenced by the cols list.
    :param list cols: A list of columns that will be checked for zeros
      and the nulls will be replaced with the replacement_constant.
    :param int replacement_constant: Value that will be used instead of
      the null values in the selected cols.

    :return DataFrame df: The DataFrame with negative and null values
      replaced with replacement_constant.
    """
    numeric_types = [pyt.DecimalType, pyt.DoubleType, pyt.FloatType, pyt.IntegerType, pyt.LongType, pyt.ShortType]

    # TODO: Consider adding a check if replacement_constant is
    # a numeric type, otherwise skip these loops

    # Loop through columns, look for numeric types
    numeric_cols = []
    for struct in df.select(cols).schema:
        for typ in numeric_types:
            if isinstance(struct.dataType, typ):
                numeric_cols.append(struct.name)
            else:
                continue

    # Check if any of those columns contain negative values
    # If so, replace with null
    for num_col in numeric_cols:
        df = df.withColumn(num_col, pyf.when(pyf.col(num_col) > 0, pyf.col(num_col)).otherwise(None))

    # Use na.fill to replace with replacement_constant
    df = df.na.fill(value=replacement_constant, subset=cols)

    return df

# TODO: Make this more generic (lags and leads)
def compute_price_and_lag_lead_price(df, total_lags=14, threshold=0.025, max_discount_run=49):
    """
    Calculate the price and populate any null price as
    the previous non-null price within the past 14 days.
    Price is calculated as POS_AMT divided by POS_ITEM_QTY.
    The column PRICE is added to the dataframe.

    :param total_lags:
    :param threshold: arbitrary threshold required to call price changes
     significant
    :param max_discount_run: How many days a discount needs to run before it is
     considered the REG_PRICE should be
        changed to match the PRICE
    :param DataFrame df: Includes columns for POS_ITEM_QTY,
    POS_AMT, ORGANIZATION_UNIT_NUM, RETAILER_ITEM_ID, and SALES_DT.
    """

    def create_lag_cols(input_df):
        # Define how the partition of data the lags and leads should be calculated on
        window_spec = Window.partitionBy(
            ['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM']
        ).orderBy('SALES_DT')

        cols_to_drop, coalesce_funcs = [], []
        for nth_lag in range(1, total_lags + 1):
            col_name = 'LAG{}'.format(nth_lag)
            input_df = input_df.withColumn(
                col_name, pyf.lag(pyf.col('PRICE'), nth_lag
                                  ).over(window_spec))

            coalesce_funcs.append(
                pyf.when(
                    pyf.isnull(pyf.col(col_name)), None
                ).when(
                    pyf.col(col_name) == 0, None
                ).otherwise(
                    pyf.col(col_name)
                )
            )
            cols_to_drop.append(col_name)

        return input_df, cols_to_drop, coalesce_funcs

    # Calculate pricing case / when style
    df_priced = df.withColumn(
        'PRICE',
        pyf.when(
            (pyf.col('POS_ITEM_QTY') == 0) |
            (pyf.col('POS_AMT') == 0) |
            (pyf.isnan(pyf.col('POS_ITEM_QTY'))) |
            (pyf.isnan(pyf.col('POS_AMT')))
            , None
        ).otherwise(pyf.col('POS_AMT') / pyf.col('POS_ITEM_QTY'))
    )
    df = None

    # Apply the window to 14 lags
    # At the end, the calculated Price follows these rules -
    # Take the original price if it is not null
    # Take the next most recent price, with lag (i.e. the day in the past)
    # first.
    df_lagged, columns_to_drop, coalesce_functions = create_lag_cols(df_priced)
    df_priced = None

    df_calculated = df_lagged.withColumn(
        'CALCULATED_PRICE',
        pyf.coalesce(
            pyf.when(pyf.isnull(pyf.col('PRICE')), None) \
                .when(pyf.col('PRICE') == 0, None) \
                .otherwise(pyf.col('PRICE')),
            *coalesce_functions
        )
    )
    df_lagged = None
    # Price has nulls. We delete Price in rename Calculated_Price to Price.
    columns_to_drop += ['PRICE']
    df_calculated = df_calculated.drop(*columns_to_drop)
    df_calculated = df_calculated.withColumnRenamed('CALCULATED_PRICE', 'PRICE')
    return df_calculated

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
        # Temp version of getting the POS data
        df = spark.sql(f'select * from default.vw_sat_link_epos_summary_apltest')
        df = df.where(pyf.col("SALES_DT") >= min_date)
        
    except Exception:
        # Deprecated version of getting the POS data
        warnings.warn('Deprecated POS data format detected. Please update to Gen 2 POS data format')
        df = spark.sql(f'select * from {pos_database}.vw_sat_link_epos_summary')
        df = df.where(pyf.col("SALES_DT") >= min_date)

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
    df = cast_decimal_to_number(df, cast_to='float')
    df = df.withColumn('ORGANIZATION_UNIT_NUM', df['ORGANIZATION_UNIT_NUM'].cast('string'))

    df = _all_possible_days(df, 'SALES_DT', ['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'])

    # Fill in the zeros for On hand inventory quantity
    df = df.withColumn(
        'ON_HAND_INVENTORY_QTY',
        pyf.when(
            pyf.col('ON_HAND_INVENTORY_QTY') < 0,
            pyf.lit(0)
        ).otherwise(
            pyf.col('ON_HAND_INVENTORY_QTY')
        )
    )

    df = _replace_negative_and_null_with(df, ['POS_ITEM_QTY', 'POS_AMT'], 0)

    # Very important to get all of the window functions using this partitioning to be done at the same time
    # This will minimize the amount of shuffling being done
    window_item_store = Window.partitionBy(['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM']).orderBy('SALES_DT')

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

    df = compute_price_and_lag_lead_price(df)

    # Use the Lag_INV columns to generate RECENT_ON_HAND_INVENTORY_QTY
    # The Lag_INV columns are dropped after this command and are no longer referenced
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

    # Add day of features
    df = df.withColumn('DOW', pyf.dayofweek('SALES_DT'))
    df = df.withColumn('DOM', pyf.dayofmonth('SALES_DT'))
    df = df.withColumn('DOY', pyf.dayofyear('SALES_DT'))

    return df

# COMMAND ----------

# Load POS data (with the nextgen processing function)
df_pos = get_pos_data(config_dict['epos_datavault_db_nm'], min_date_filter, spark)

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
    SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars_apltest
    WHERE
    mdm_country_id = {country_id} and
    mdm_client_id = {client_id} and
    mdm_holding_id = {holding_id} and
    coalesce(mdm_banner_id, -1) = {banner_id}
''')

df_intervention = df_intervention.where(pyf.col("call_date") >= min_date_filter)

print(f'Before = {df_intervention.cache().count():,}')

df_intervention = df_intervention.withColumn(
    'measure_start',
    pyf.expr('date_add(call_date, intervention_start_day)')
)
df_intervention = df_intervention.withColumn(
    'measure_end',
    pyf.expr('date_add(call_date, intervention_end_day)')
)

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
cat_feautres_list = ['ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID', 'DOW', 'standard_response_cd']
for col_name, col_type in df_merged.dtypes:
    if (col_type == 'bigint' or col_type == 'long' or col_type == 'double') and col_name not in cat_feautres_list:
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
    .option('mergeSchema', 'true')\
    .save(f'/mnt/processed/measurement/cache/{client_id}-{country_id}-{holding_id}-{banner_id}-apltest')
print(f'{df_merged.cache().count():,}')

df_intervention.write.format('delta')\
    .mode('overwrite')\
    .option('mergeSchema', 'true')\
    .save(f'/mnt/processed/measurement/cache/{client_id}-{country_id}-{holding_id}-{banner_id}-intervention-apltest')
print(f'{df_intervention.cache().count():,}')
