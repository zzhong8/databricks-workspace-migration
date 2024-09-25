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
# MAGIC ## UPC - retailer_item_id table

# COMMAND ----------

# upc_id = spark.sql('select * from default.vw_upc_retailer_id')
# upc_id.cache().count()

# TODO - read from blob storage to save some time - it takes a few minutes to get data from table -- delete later 
upc_id = spark.read.format('delta').option('inferSchema', 'true').load(f'/mnt/processed/nafis/vm_upc/upc_retailer_store_mapping')
upc_id.cache().count()

# COMMAND ----------

store_ids = spark.sql('select store_id, subbanner_description, division_description from acosta_retail_analytics_im.vw_dimension_store where holding_id = 3257')
store_ids.cache().count()

# COMMAND ----------

upc_id_banner = upc_id.join(store_ids, 'store_id')\
                      .dropDuplicates(['retailer_item_id', 'upc', 'division_description'])\
                      .select('retailer_item_id', 'upc', 'division_description')
upc_id_banner = upc_id_banner.filter(pyf.col('division_description') == config_dict['mdm_banner_nm'])
print(upc_id_banner.cache().count())
display(upc_id_banner)

# COMMAND ----------

grouped_df = upc_id_banner.groupBy('retailer_item_id').agg(pyf.collect_set('upc').alias('upc_set'))
joined_df = grouped_df.alias('a').join(grouped_df.alias('b'), pyf.lit(1) == pyf.lit(1))\
                .where(pyf.size(pyf.array_intersect(pyf.col('a.upc_set'), pyf.col('b.upc_set'))) >= 1)\
                .select(pyf.col('a.retailer_item_id').alias('retailer_item_id'), pyf.array_union(pyf.col('a.upc_set'), pyf.col('b.upc_set')).alias('upc_set'))
temp_df = joined_df.groupBy('retailer_item_id').agg(pyf.collect_set('upc_set').alias('upc_set'))\
                    .withColumn('upc_set', pyf.sort_array(pyf.array_distinct(pyf.flatten(pyf.col('upc_set')))))
display(temp_df)

# COMMAND ----------

temp_df = temp_df \
    .withColumn('lenght', pyf.size(temp_df['upc_set']))
max_lenght = temp_df.select(pyf.max(pyf.col('lenght'))).collect()[0][0]
print(max_lenght)
display(temp_df.filter(pyf.col('lenght')> 1))

# COMMAND ----------

temp_df = temp_df.select(
    pyf.col('retailer_item_id'),
    pyf.explode(pyf.array([pyf.array([pyf.lit(temp_df.upc_set[0]), pyf.col('upc_set')[i]]) for i in range(max_lenght)]))
    .alias('split_arr')
)
temp_df = temp_df.select(
    pyf.col('retailer_item_id'),
    pyf.col('split_arr')[0].alias('reported_upc'),
    pyf.col('split_arr')[1].alias('nonreported_upc')
).dropna(subset=['nonreported_upc'])
display(temp_df)

# COMMAND ----------

upc_itemid_mapper = temp_df.select('retailer_item_id', 'reported_upc').dropDuplicates()
print(upc_itemid_mapper.cache().count())
display(upc_itemid_mapper)

# COMMAND ----------

# MAGIC %md
# MAGIC ## POS data

# COMMAND ----------

today_date = datetime.date.today()

min_date_filter = (today_date - relativedelta(years=1)).strftime(format='%Y-%m-%d') #TODO check with Hugh or Eric -- why we go 10 years back?????? - change it back to 10 years?! 
# min_date_filter = (today_date - relativedelta(years=2)).strftime(format='%Y-%m-%d')

# COMMAND ----------

# TODO - delete later - have to use updated function
# # Load POS data (with the nextgen processing function)
# df_pos = get_pos_data(config_dict['epos_datavault_db_nm'], min_date_filter, spark)

# df_pos = df_pos.fillna({
#     'ON_HAND_INVENTORY_QTY': 0,
#     'RECENT_ON_HAND_INVENTORY_QTY': 0,
#     'RECENT_ON_HAND_INVENTORY_DIFF': 0
# })
# print(f'{df_pos.cache().count():,}')
# display(df_pos)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### UPDATE get_pos_data

# COMMAND ----------

# copied from package
import datetime
import warnings

import numpy as np
import pandas as pd
import pyspark.sql.functions as pyf
import pyspark.sql.types as pyt
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

# from .constants import week_seasonality_spark_map, year_seasonality_map

from acosta.alerting.helpers import check_path_exists
from acosta.alerting.helpers import features as acosta_features


# NOT USED. Period. Right now
def shape(df):
    """
    Get the shape of the matrix
    :param df:
    :return:
    """
    return df.count(), len(df.columns)


def _list_days(start_date, end_date):
    """
      Take an RDD Row with startDate and endDate and produce a list of
      all days including and between start and endDate.
      :return: list of all possible days including and between start and end date.
    """
    day_range = (end_date - start_date).days + 1
    return [start_date + datetime.timedelta(i) for i in range(day_range)]


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


# NOT USED. Reason: There is now a massive country dates table that already contains all the holidays
#                   The table is created in Create_Country_Dates_Table.py in the 'Reference' folder
def _append_holidays(df, tier=None):
    """
    Given a spark dataframe, load a holiday table and left join
    the two tables based on SALES_DT.
    """
    holidays_usa = spark.read.format('orc').load('dbfs:/mnt/processed/ref/holidays_usa.orc')

    df = df.join(holidays_usa.hint('broadcast'), on=['SALES_DT'], how='left')
    return df


# NOT USED. Reason: We are creating this in the notebook Make_Snap_Paycycle_Index.py in the 'Reference' folder
def _append_snap_pay_cycle_index(df, retailer, client, country_code):
    """
    Given a spark dataframe, load the snap and paycycle table and left join
    the two tables based on SALES_DT (DAYOFMONTH) and ORGANIZATION_UNIT_NUM.
    :param Dataframe df: Includes columns for ORGANIZATION_UNIT_NUMBER, SALES_DT.
    :param string retailer: A valid retailer.
    :param string client: A valid client.
    :param string country_code: A valid country code.
    """
    retailer = retailer.lower()
    client = client.lower()
    country_code = country_code.lower()

    snap_pay_cycle = spark.read.parquet(
        '/mnt/prod-ro/artifacts/country_code/reference/snap_paycycle/retailer={retailer}/client={client}/country_code={country_code}/'.format(
            retailer=retailer,
            client=client,
            country_code=country_code
        )
    )
    df_with_day_of_month = df.withColumn('DAYOFMONTH', pyf.dayofmonth(pyf.col('SALES_DT')))
    return df_with_day_of_month \
        .join(snap_pay_cycle.hint('broadcast'), ['ORGANIZATION_UNIT_NUM', 'DAYOFMONTH'], 'inner') \
        .drop('DAYOFMONTH', 'STATE')


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


def convert_dict_to_spark_df(d, col_names, sc):
    a = [(key,) + (value,) for key, value in d.items()]
    df = sc.parallelize(a).toDF(col_names)
    return df


def repeated_rbf(xin, center, period, std=1):
    # DEPRECATED
    n_repeats = int(xin.max() // period)
    repeats = np.ones(n_repeats + 1) * np.arange(0, 1 + n_repeats) * 365

    center_vec = repeats - (-center)
    center_vec = np.tile(center_vec, (len(xin), 1))

    diff = (xin - center_vec.T).T

    mod = diff
    return np.exp((-(mod) ** 2) / (2 * std ** 2)).max(axis=1)  # calling max on the row gets the strongest RBF


def decompose_seasonality(df, year_level_resolution=52):
    """
    Returns 2 new DataFrames.
    One contains the year seasonality and the other contains the week level seasonality.
    DOY is the day of year
    DOW is the day of week
    PRICE is the price of the product. This is to de-confound the relationship between demand and price.
    :param df: DataFrame must contain the [POS_ITEM_QTY, PRICE, DOY, DOW]
    :param year_level_resolution: How much P-Splines to use to estimate the year seasonality
    :return:
    """
    from pygam import LinearGAM, s, f

    df.columns = [x.upper() for x in df.columns]

    if 'POS_ITEM_QTY' not in df.columns or 'PRICE' not in df.columns or 'DOY' not in df.columns or 'DOW' not in df.columns:
        raise ValueError('Input `df` MUST have columns [POS_ITEM_QTY, PRICE, DOY, DOW]')

    decomposer = LinearGAM(s(0, n_splines=year_level_resolution, lam=0.25) + f(1, lam=0.25) + s(2, lam=0.25))
    decomposer.fit(df[['DOY', 'DOW', 'PRICE']].values, df['POS_ITEM_QTY'].values)

    # Decompose Seasonality Trends
    n_non_intercept_terms = len(decomposer.terms) - 1

    doy_range = np.arange(df['DOY'].min(), df['DOY'].max() + 1)
    x_grid_year = np.zeros((len(doy_range), n_non_intercept_terms))
    x_grid_year[:, 0] = doy_range

    dow_range = np.arange(df['DOW'].min(), df['DOW'].max() + 1)
    x_grid_week = np.zeros((len(dow_range), n_non_intercept_terms))
    x_grid_week[:, 1] = dow_range

    # Compute partial dependence
    year_partial_dependence, _ = decomposer.partial_dependence(0, x_grid_year, width=0.95)
    week_partial_dependence, _ = decomposer.partial_dependence(1, x_grid_week, width=0.95)

    # Create the 2 output DataFrames
    df_year_seasonality = pd.DataFrame({'DOY': doy_range, 'YEAR_SEASONALITY': year_partial_dependence})
    df_week_seasonality = pd.DataFrame({'DOW': dow_range, 'WEEK_SEASONALITY': week_partial_dependence})
    return df_year_seasonality, df_week_seasonality


def compute_reg_price_and_discount_features(df, threshold=0.025, max_discount_run=49):
    """
    Computes the REG_PRICE, DISCOUNT_PERCENT & DAYS_DISCOUNTED from a dataframe that contains
    a PRICE, ORGANIZATION_UNIT_NUM, RETAILER_ITEM_ID
    :param df:
    :param threshold: arbitrary threshold required to call price changes significant
    :param max_discount_run: How many days a discount needs to run before it is considered the REG_PRICE should be
        changed to match the PRICE
    :return:
    """
    # Create pricing dummy columns

    df_pricing_logic = df[['ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID', 'SALES_DT', 'PRICE']]
    df_pricing_logic = df_pricing_logic \
        .withColumn('REG_PRICE', pyf.lit(0.0)) \
        .withColumn('DISCOUNT_PERCENT', pyf.lit(0.0)) \
        .withColumn('DAYS_DISCOUNTED', pyf.lit(0.0))

    @pyf.pandas_udf(df_pricing_logic.schema, pyf.PandasUDFType.GROUPED_MAP)
    def pricing_data_udf(d: pd.DataFrame) -> pd.DataFrame:
        """
        This generates the REG_PRICE, DISCOUNT_PERCENT, DAYS_DISCOUNTED from the PRICE
        using fancy pants logic
        NOTE: Pandas UDF cannot have any NANs in it at all and more importantly cannot produce a NAN anywhere.
            All the code below was used to make sure that NANs never occur.
        :param d:
        :return:
        """
        # NOTE: Function is nested to use the extra args `threshold` and `max_discount_run`
        d = d.sort_values('SALES_DT')
        d.loc[:, 'PRICE'] = d['PRICE'] \
            .interpolate(method='nearest', limit_direction='both') \
            .fillna(method='ffill') \
            .fillna(method='bfill')

        # Check if some columns still contain NaNs
        null_check_data = d.isnull().sum()
        if null_check_data.any():
            # Some columns still contain NaNs this should only be possible if ALL data in the calculated price is nan
            if null_check_data['PRICE'] == len(d):
                fill_value = -1.0
                warnings.warn(
                    'All data in the `PRICE` price column in NaNs. Setting all to {}'.format(fill_value)
                )
                d.loc[:, 'PRICE'] = fill_value
                return d
            else:
                raise AssertionError('Pandas UDF contains unexpected {} NaNs. N = {} \n NaNs found at \n{}'.format(
                    null_check_data,
                    len(d),
                    d[null_check_data.any(axis=1)]
                ))

        # Row wise business logic
        for i, _ in d.iterrows():
            if i == 0:
                d.loc[i, 'REG_PRICE'] = d.loc[i, 'PRICE']
                continue

            calc_price_delta = d.loc[i, 'PRICE'] - d.loc[i - 1, 'PRICE']
            ratio = (d.loc[i, 'PRICE'] - d.loc[i - 1, 'REG_PRICE']) / d.loc[i - 1, 'REG_PRICE']
            if calc_price_delta > 0:
                # Calculated price increase
                if ratio >= threshold:
                    # Significant price increase
                    # Check for index out of bounds error
                    if i < 2:
                        # Computing yesterday_ratio will trigger an KeyError for the index being out of bounds
                        d.loc[i, 'REG_PRICE'] = d.loc[i - 1, 'REG_PRICE']
                    else:
                        # Check if this has continued for 2 days
                        yesterday_ratio = (d.loc[i - 1, 'PRICE'] - d.loc[i - 2, 'REG_PRICE'])
                        yesterday_ratio = yesterday_ratio / d.loc[i - 2, 'REG_PRICE']
                        if yesterday_ratio >= threshold:
                            # Set as a real price increase and back fill *(Still don't like this idea)
                            d.loc[i - 1, 'REG_PRICE'] = d.loc[i, 'PRICE']
                            d.loc[i, 'REG_PRICE'] = d.loc[i, 'PRICE']
                        else:
                            # Carry over yesterday's data for now
                            d.loc[i, 'REG_PRICE'] = d.loc[i - 1, 'REG_PRICE']
                else:
                    # Insignificant price increase
                    d.loc[i, 'REG_PRICE'] = d.loc[i - 1, 'REG_PRICE']
                    # Check if the increase is still in a discount
                    if ratio <= -threshold:
                        d.loc[i, 'DAYS_DISCOUNTED'] = d.loc[i - 1, 'DAYS_DISCOUNTED'] + 1
                        d.loc[i, 'DISCOUNT_PERCENT'] = (d.loc[i, 'REG_PRICE'] - d.loc[i, 'PRICE'])
                        d.loc[i, 'DISCOUNT_PERCENT'] = d.loc[i, 'DISCOUNT_PERCENT'] / d.loc[i, 'REG_PRICE']

            elif calc_price_delta < 0:
                # Calculated price decrease
                if ratio <= -threshold:
                    # This is a discount therefore start counting
                    d.loc[i, 'REG_PRICE'] = d.loc[i - 1, 'REG_PRICE']
                    d.loc[i, 'DAYS_DISCOUNTED'] = d.loc[i - 1, 'DAYS_DISCOUNTED'] + 1
                    d.loc[i, 'DISCOUNT_PERCENT'] = (d.loc[i, 'REG_PRICE'] - d.loc[i, 'PRICE'])
                    d.loc[i, 'DISCOUNT_PERCENT'] = d.loc[i, 'DISCOUNT_PERCENT'] / d.loc[i, 'REG_PRICE']
                else:
                    # Insignificant price decrease
                    d.loc[i, 'REG_PRICE'] = d.loc[i - 1, 'REG_PRICE']

            else:
                # Steady price
                d.loc[i, 'REG_PRICE'] = d.loc[i - 1, 'REG_PRICE']

                # Increase steady-state logic
                if ratio >= threshold:
                    if i < 2:
                        # Computing yesterday_ratio will trigger an KeyError for the index being out of bounds
                        d.loc[i, 'REG_PRICE'] = d.loc[i - 1, 'REG_PRICE']
                        continue

                    # Check if this has continued for 2 days
                    yesterday_ratio = (d.loc[i - 1, 'PRICE'] - d.loc[i - 2, 'REG_PRICE'])
                    yesterday_ratio = yesterday_ratio / d.loc[i - 2, 'REG_PRICE']
                    if yesterday_ratio >= threshold:
                        # Set as a real price increase and back fill *(Still don't like this idea)
                        d.loc[i - 1, 'REG_PRICE'] = d.loc[i, 'PRICE']
                        d.loc[i, 'REG_PRICE'] = d.loc[i, 'PRICE']
                    else:
                        # Carry over yesterday's data for now
                        d.loc[i, 'REG_PRICE'] = d.loc[i - 1, 'REG_PRICE']

                # Discount steady-state logic
                if ratio <= -threshold:
                    # Discount
                    if d.loc[i - 1, 'DAYS_DISCOUNTED'] < max_discount_run:
                        # Discount is on going
                        d.loc[i, 'REG_PRICE'] = d.loc[i - 1, 'REG_PRICE']
                        d.loc[i, 'DAYS_DISCOUNTED'] = d.loc[i - 1, 'DAYS_DISCOUNTED'] + 1
                        d.loc[i, 'DISCOUNT_PERCENT'] = (d.loc[i, 'REG_PRICE'] - d.loc[i, 'PRICE'])
                        d.loc[i, 'DISCOUNT_PERCENT'] = d.loc[i, 'DISCOUNT_PERCENT'] / d.loc[i, 'REG_PRICE']
                    else:
                        # Discount is now the new price
                        d.loc[i, 'REG_PRICE'] = d.loc[i, 'PRICE']
                        d.loc[i, 'DAYS_DISCOUNTED'] = 0
                        d.loc[i, 'DISCOUNT_PERCENT'] = (d.loc[i, 'REG_PRICE'] - d.loc[i, 'PRICE'])
                        d.loc[i, 'DISCOUNT_PERCENT'] = d.loc[i, 'DISCOUNT_PERCENT'] / d.loc[i, 'REG_PRICE']

        return d

    df_pricing_logic = df_pricing_logic.groupBy(['ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID']).apply(pricing_data_udf)
    df_pricing_logic = df_pricing_logic.drop('PRICE')  # dropping because `df` contains the calculated price

    df = df.join(df_pricing_logic, ['ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID', 'SALES_DT'], 'inner')
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


# NOT USED. Reason: We have created a set of features for weekly amplitude in pos_to_training_data that we are using instead
def _append_day_of_week_indicator(df, date_field):
    temp_df = df.withColumn('temp_day_of_week', pyf.dayofweek(date_field))

    out_df = temp_df \
        .withColumn('DAY_OF_WEEK_02TUES', (pyf.col('temp_day_of_week') == 3).cast('integer')) \
        .withColumn('DAY_OF_WEEK_03WEDS', (pyf.col('temp_day_of_week') == 4).cast('integer')) \
        .withColumn('DAY_OF_WEEK_04THUR', (pyf.col('temp_day_of_week') == 5).cast('integer')) \
        .withColumn('DAY_OF_WEEK_05FRID', (pyf.col('temp_day_of_week') == 6).cast('integer')) \
        .withColumn('DAY_OF_WEEK_06SATU', (pyf.col('temp_day_of_week') == 7).cast('integer')) \
        .withColumn('DAY_OF_WEEK_07SUND', (pyf.col('temp_day_of_week') == 1).cast('integer')) \
        .drop('temp_day_of_week')

    return out_df


# NOT USED. Reason: The reference data for Snap and Paycycles that we are using already contains this information
def _append_day_of_month_indicator(df, date_field):
    temp_df = df.withColumn('temp_day_of_month', pyf.dayofmonth(date_field))

    out_df = temp_df \
        .withColumn('DAY_OF_MONTH_02', (pyf.col('temp_day_of_month') == 2).cast('integer')) \
        .withColumn('DAY_OF_MONTH_03', (pyf.col('temp_day_of_month') == 3).cast('integer')) \
        .withColumn('DAY_OF_MONTH_04', (pyf.col('temp_day_of_month') == 4).cast('integer')) \
        .withColumn('DAY_OF_MONTH_05', (pyf.col('temp_day_of_month') == 5).cast('integer')) \
        .withColumn('DAY_OF_MONTH_06', (pyf.col('temp_day_of_month') == 6).cast('integer')) \
        .withColumn('DAY_OF_MONTH_07', (pyf.col('temp_day_of_month') == 7).cast('integer')) \
        .withColumn('DAY_OF_MONTH_08', (pyf.col('temp_day_of_month') == 8).cast('integer')) \
        .withColumn('DAY_OF_MONTH_09', (pyf.col('temp_day_of_month') == 9).cast('integer')) \
        .withColumn('DAY_OF_MONTH_10', (pyf.col('temp_day_of_month') == 10).cast('integer')) \
        .withColumn('DAY_OF_MONTH_11', (pyf.col('temp_day_of_month') == 11).cast('integer')) \
        .withColumn('DAY_OF_MONTH_12', (pyf.col('temp_day_of_month') == 12).cast('integer')) \
        .withColumn('DAY_OF_MONTH_13', (pyf.col('temp_day_of_month') == 13).cast('integer')) \
        .withColumn('DAY_OF_MONTH_14', (pyf.col('temp_day_of_month') == 14).cast('integer')) \
        .withColumn('DAY_OF_MONTH_15', (pyf.col('temp_day_of_month') == 15).cast('integer')) \
        .withColumn('DAY_OF_MONTH_16', (pyf.col('temp_day_of_month') == 16).cast('integer')) \
        .withColumn('DAY_OF_MONTH_17', (pyf.col('temp_day_of_month') == 17).cast('integer')) \
        .withColumn('DAY_OF_MONTH_18', (pyf.col('temp_day_of_month') == 18).cast('integer')) \
        .withColumn('DAY_OF_MONTH_19', (pyf.col('temp_day_of_month') == 19).cast('integer')) \
        .withColumn('DAY_OF_MONTH_20', (pyf.col('temp_day_of_month') == 20).cast('integer')) \
        .withColumn('DAY_OF_MONTH_21', (pyf.col('temp_day_of_month') == 21).cast('integer')) \
        .withColumn('DAY_OF_MONTH_22', (pyf.col('temp_day_of_month') == 22).cast('integer')) \
        .withColumn('DAY_OF_MONTH_23', (pyf.col('temp_day_of_month') == 23).cast('integer')) \
        .withColumn('DAY_OF_MONTH_24', (pyf.col('temp_day_of_month') == 24).cast('integer')) \
        .withColumn('DAY_OF_MONTH_25', (pyf.col('temp_day_of_month') == 25).cast('integer')) \
        .withColumn('DAY_OF_MONTH_26', (pyf.col('temp_day_of_month') == 26).cast('integer')) \
        .withColumn('DAY_OF_MONTH_27', (pyf.col('temp_day_of_month') == 27).cast('integer')) \
        .withColumn('DAY_OF_MONTH_28', (pyf.col('temp_day_of_month') == 28).cast('integer')) \
        .withColumn('DAY_OF_MONTH_29', (pyf.col('temp_day_of_month') == 29).cast('integer')) \
        .withColumn('DAY_OF_MONTH_30', (pyf.col('temp_day_of_month') == 30).cast('integer')) \
        .withColumn('DAY_OF_MONTH_31', (pyf.col('temp_day_of_month') == 31).cast('integer')) \
        .drop('temp_day_of_month')

    return out_df


# NOT USED: Reason: We are now doing this in-line in the pos_to_training_data function instead
def _compute_inventory_lag(df, n_lags=14):
    """
    For sales datasets with inventory updated only on selling days, we need to
    compute the expected inventory available on successive days without sales.
    This function takes an input dataframe and creates up to 14 days of carried-forward
    inventory estimates. Logic is aligned to Walmart formatted data - OH Inventory is
    previous day's qty.
    :param Dataframe df: Includes columns for ORGANIZATION_UNIT_NUMBER, RETAILER_ITEM_ID, SALES_DT,
    and  ON_HAND_INVENTORY_QTY
    """
    window_spec = Window.partitionBy(['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM']).orderBy('SALES_DT')
    lag_inv_strings = ['LAG_INV_{}'.format(i) for i in range(1, n_lags + 1)]

    output_df = df \
        .withColumn('LAG_INV_1', pyf.lag(pyf.col('ON_HAND_INVENTORY_QTY'), 1).over(window_spec)) \
        .withColumn('LAG_INV_2', pyf.lag(pyf.col('ON_HAND_INVENTORY_QTY'), 2).over(window_spec)) \
        .withColumn('LAG_INV_3', pyf.lag(pyf.col('ON_HAND_INVENTORY_QTY'), 3).over(window_spec)) \
        .withColumn('LAG_INV_4', pyf.lag(pyf.col('ON_HAND_INVENTORY_QTY'), 4).over(window_spec)) \
        .withColumn('LAG_INV_5', pyf.lag(pyf.col('ON_HAND_INVENTORY_QTY'), 5).over(window_spec)) \
        .withColumn('LAG_INV_6', pyf.lag(pyf.col('ON_HAND_INVENTORY_QTY'), 6).over(window_spec)) \
        .withColumn('LAG_INV_7', pyf.lag(pyf.col('ON_HAND_INVENTORY_QTY'), 7).over(window_spec)) \
        .withColumn('LAG_INV_8', pyf.lag(pyf.col('ON_HAND_INVENTORY_QTY'), 8).over(window_spec)) \
        .withColumn('LAG_INV_9', pyf.lag(pyf.col('ON_HAND_INVENTORY_QTY'), 9).over(window_spec)) \
        .withColumn('LAG_INV_10', pyf.lag(pyf.col('ON_HAND_INVENTORY_QTY'), 10).over(window_spec)) \
        .withColumn('LAG_INV_11', pyf.lag(pyf.col('ON_HAND_INVENTORY_QTY'), 11).over(window_spec)) \
        .withColumn('LAG_INV_12', pyf.lag(pyf.col('ON_HAND_INVENTORY_QTY'), 12).over(window_spec)) \
        .withColumn('LAG_INV_13', pyf.lag(pyf.col('ON_HAND_INVENTORY_QTY'), 13).over(window_spec)) \
        .withColumn('LAG_INV_14', pyf.lag(pyf.col('ON_HAND_INVENTORY_QTY'), 14).over(window_spec)) \
        .withColumn('RECENT_ON_HAND_INVENTORY_QTY', pyf.coalesce(
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
        ))

    return output_df.drop(*lag_inv_strings)


def _hash_org_unit_num(org_unit_num_series: pd.Series):
    def hash_func(string):
        return hash(string.casefold())

    if 'int' not in str(org_unit_num_series.dtype):
        # Check if it numerical at all
        try:
            if org_unit_num_series.str.isdigit().all():
                # It looks like a number. Just try to parse and return it
                return org_unit_num_series.astype(int)
            else:
                # Return hashed return
                return org_unit_num_series.apply(hash_func)

        except AttributeError:
            # This is not a string or an number? Truly unexpected type. Just break
            raise ValueError('Unexpected content in column. Hashing was not performed')

    else:
        import warnings
        # Is type integer. Do nothing
        warnings.warn('NOTE: Input is already an integer and does not need to be hashed')
        return org_unit_num_series


def get_hash_org_unit_num_udf() -> callable:
    """
    Return a pandas_udf to hashing the organization unit number
    This UDF is generated lazily because if the decorator (pandas_udf())
    is called before the spark session has fully initialized it will fail.
    Therefore the cyclomatic complexity observed here is sadly required.
    :return callable: pandas_udf
    """
    return pyf.pandas_udf(_hash_org_unit_num, 'int', pyf.PandasUDFType.SCALAR)


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


# # Deprecate this function
# def pos_to_training_data(
#         df, retailer, client, country_code, model_source, spark, spark_context, include_discount_features=False,
#         item_store_cols=None
# ):
#     """
#     Takes in a POS DataFrame and:
#     - Explodes data to include all days between min and max of original dataframe.
#     - Computes Price and a lag/leading price.
#     - Joins global tables:
#       - holidays_usa
#     - Relies on a global DATE_FIELD being defined.
#     :param DataFrame df: A POS fact table containing <item_store_cols>, POS_ITEM_QTY, POS_AMTS
#       and the field defined in a constant <DATE_FIELD>.
#     :param string retailer: A string containing a valid RETAILER.
#     :param string client: A string containing a valid CLIENT for the given RETAILER.
#     :param string country_code: A string containing a valid COUNTRY CODE for the given RETAILER/CLIENT.
#     :param string model_source: Whether the location of the model source data is local or production.
#     :param SparkSession spark: The current spark session.
#     :param SparkContext spark_context: In databricks you get this for free in the `sc` object
#     :param boolean include_discount_features: Whether or not the advanced discount features should be
#         calculated.  Setting to true provides
#     :param list(string) item_store_cols: The list of column names that represent the RETAILER_ITEM_ID
#       and stores and are used in joining to NARS datasets (Note: deprecated).
#     :param is_inference: if this is True then it MUST look up a previously computed seasonality
#         for the store and items it is looking for.
#     :return DataFrame:
#     """
#     from pygam.utils import b_spline_basis

#     # HIGH LEVEL COMMENT: The idea of setting the variables to None was done with the hope
#     #   of keeping the driver memory low.  However, since that is no longer necessary,
#     #   consider overwriting the variables for cleanliness or stick with the unique
#     #   variables to keep it scala-esque.
#     # TODO (2/21/2020) replace all these needless None calling with nothing or cached execution if necessary

#     # Necessary to handle the Pricing logic
#     # @Stephen Rose for more details
#     df = cast_decimal_to_number(df, cast_to='float')

#     df010 = _all_possible_days(
#         df=df,
#         date_field='SALES_DT',
#         join_fields_list=['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM']
#     )

#     # Fill in the zeros for On hand inventory quantity
#     # Could be merged with the _replace_negative_and_null_with
#     # Should verify that the optimizer is not combining this already
#     df110 = df010.withColumn(
#         'ON_HAND_INVENTORY_QTY',
#         pyf.when(pyf.col('ON_HAND_INVENTORY_QTY') < 0, pyf.lit(0)).otherwise(
#             pyf.col('ON_HAND_INVENTORY_QTY'))
#     )

#     df010 = None
#     df020 = _replace_negative_and_null_with(df=df110, cols=['POS_ITEM_QTY', 'POS_AMT'], replacement_constant=0)

#     # Very important to get all of the window functions using this partitioning to be done at the same time
#     # This will minimize the amount of shuffling being done and allow for pipelining
#     window_item_store = Window.partitionBy(['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM']).orderBy('SALES_DT')

#     # Lag for 12 weeks to match the logic from the LOESS baseline model
#     lag_units = acosta_features.get_lag_days()
#     for lag in lag_units:
#         df020 = df020.withColumn(
#             'LAG_UNITS_{}'.format(lag),
#             pyf.lag(pyf.col('POS_ITEM_QTY'), lag).over(window_item_store)
#         )
#         if lag <= 14:
#             # Lag inventory units are created to generate the recent_on_hand_inventory_diff feature
#             df020 = df020.withColumn(
#                 'LAG_INV_{}'.format(lag),
#                 pyf.lag(pyf.col('ON_HAND_INVENTORY_QTY'), lag).over(window_item_store)
#             )

#     # Include the lags for pricing
#     # This note is still very much important to avoid a second shuffle
#     # NOTE: Consider moving this out of a function and embedding into pos_to_training_data
#     # NOTE: (continued) the DAG shows two window functions being used instead of one.
#     df020 = compute_price_and_lag_lead_price(df020)

#     # Use the Lag_INV columns to generate RECENT_ON_HAND_INVENTORY_QTY
#     # The Lag_INV columns are dropped after this command and are no longer referenced
#     df020 = df020.withColumn('RECENT_ON_HAND_INVENTORY_QTY', pyf.coalesce(
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
#     )).drop(*['LAG_INV_{}'.format(i) for i in range(1, 15)])

#     # The RECENT_ON_HAND_INVENTORY_DIFF is the prior day end's inventory minus two days ago
#     # RECENT_ON_HAND_INVENTORY_QTY is at least the prior day's ending inventory
#     df020 = df020.withColumn('RECENT_ON_HAND_INVENTORY_DIFF', pyf.col('RECENT_ON_HAND_INVENTORY_QTY') \
#                              - pyf.lag(pyf.col('RECENT_ON_HAND_INVENTORY_QTY'), 1).over(window_item_store))

#     df110 = None
#     loaded_df01 = None
#     # TODO (5/22/2019) check if the table exists and is not empty.
#     # Reference data that contains Holidays, Days of Week, and Days of Month

#     country_code = country_code.upper()

#     sql_statement = 'SELECT * FROM retail_forecast_engine.country_dates WHERE COUNTRY = "{country}"'
#     sql_statement = sql_statement.format(country=country_code)

#     master_dates_table = spark.sql(sql_statement)

#     # NOTE!  The Country/Date dimension contains some columns we're not yet ready to use in our training process.
#     # This includes the new "RELATIVE" class of columns
#     # So let's filter out those columns
#     master_dates_table = master_dates_table.drop(*["COUNTRY", "DAY_OF_WEEK"])
#     master_dates_table = master_dates_table.drop(
#         *filter(lambda x: x.endswith("_RELATIVE"), master_dates_table.schema.names))

#     # This is all @Stephen
#     # TODO (5/13/2019) [FUTURE] swap with item-client specific seasonality
#     # Add Seasonality Columns
#     master_dates_table = master_dates_table.withColumn('dow', pyf.dayofweek('SALES_DT'))
#     master_dates_table = master_dates_table.withColumn('doy', pyf.dayofyear('SALES_DT'))

#     week_df = convert_dict_to_spark_df(week_seasonality_spark_map, ['dow', 'WEEK_SEASONALITY'], spark_context)
#     year_df = convert_dict_to_spark_df(year_seasonality_map, ['doy', 'YEAR_SEASONALITY'], spark_context)

#     master_dates_table = master_dates_table.join(week_df, week_df['dow'] == master_dates_table['dow'])
#     master_dates_table = master_dates_table.drop('dow')

#     master_dates_table = master_dates_table.join(year_df, year_df['doy'] == master_dates_table['doy'])
#     master_dates_table = master_dates_table.drop('doy')

#     # Add dynamic intercepts (time of year effect)
#     days_of_year = np.arange(366) + 1
#     dynamic_intercepts = b_spline_basis(
#         days_of_year,
#         edge_knots=np.r_[days_of_year.min(), days_of_year.max()],
#         sparse=False, periodic=False
#     )
#     dynamic_intercepts = pd.DataFrame(
#         dynamic_intercepts,
#         columns=['TIME_OF_YEAR_{}'.format(i) for i in range(dynamic_intercepts.shape[1])]
#     )
#     dynamic_intercepts['doy'] = days_of_year

#     dynamic_intercepts = spark.createDataFrame(dynamic_intercepts)
#     master_dates_table = master_dates_table.withColumn('doy', pyf.dayofyear('SALES_DT'))
#     master_dates_table = master_dates_table.join(
#         dynamic_intercepts, dynamic_intercepts['doy'] == master_dates_table['doy']
#     )
#     master_dates_table = master_dates_table.drop('doy')

#     # Add weekly amplitude features
#     master_dates_table = master_dates_table.withColumn('doy', pyf.dayofyear('SALES_DT'))
#     result_df = master_dates_table.toPandas()
#     amplitude = ['WEEK_AMPLITUDE_{}'.format(i) for i in range(15)]
#     for i, week_i in enumerate(amplitude):
#         result_df[week_i] = repeated_rbf(
#             result_df['doy'].values,
#             i * (366 / len(amplitude)),
#             period=366,
#             std=10
#         )
#         result_df[week_i] = result_df[week_i] * result_df['WEEK_SEASONALITY']
#     master_dates_table = spark.createDataFrame(result_df)

#     # Load reference data for Snap and Paycycles
#     # Joins to the data set based on day of month and organization_unit_num (store #)
#     snap_pay_cycle = spark.read.parquet(
#         '/mnt{mod}/artifacts/country_code/reference/snap_paycycle/retailer={retailer}/client={client}/country_code={country_code}/'.format(
#             mod='' if model_source == 'LOCAL' else '/prod-ro',
#             retailer=retailer.lower(),
#             client=client.lower(),
#             country_code=country_code.lower()
#         )
#     ).withColumnRenamed("DAYOFMONTH", "DAY_OF_MONTH")

#     # Join to the list of holidays, day_of_week, and day_of_month
#     # This join is needed before joining snap_paycycle due to the presence of day_of_month
#     df_with_dates = df020.join(master_dates_table, ["SALES_DT"], "inner")

#     df_with_snap = df_with_dates \
#         .join(snap_pay_cycle, ['ORGANIZATION_UNIT_NUM', 'DAY_OF_MONTH'], 'inner') \
#         .drop('STATE')

#     df_penultimate = df_with_snap
#     df_with_dates = None
#     master_dates_table = None
#     snap_pay_cycle = None
#     loaded_df02 = None

#     if include_discount_features:
#         df_out = compute_reg_price_and_discount_features(df_penultimate, threshold=0.025, max_discount_run=49)
#     else:
#         df_out = df_penultimate

#     df_penultimate = None

#     # Add day of the week
#     df_out = df_out.withColumn('DOW', pyf.dayofweek('SALES_DT'))

#     # Hash Organization Unit Number if it exists
#     for col_name, col_type in df_out.dtypes:
#         if col_name.casefold() == 'organization_unit_num' and 'string' in col_type:
#             df_out = df_out.withColumn(
#                 'ORGANIZATION_UNIT_NUM',
#                 get_hash_org_unit_num_udf()(pyf.col('ORGANIZATION_UNIT_NUM'))
#             )

#     return df_out


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

    database = '{retailer}_{client}_{country_code}_dv'.format(
        retailer=retailer.strip().lower(),
        client=client.strip().lower(),
        country_code=country_code.strip().lower()
    )

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

    # Very important to get all of the window functions using this partitioning to be done at the same time
    # This will minimize the amount of shuffling being done
    
#     #TODO - add upc here - maybe is the best place and 
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

def _list_days(start_date, end_date, max_date):
    """
      Take an RDD Row with startDate and endDate and produce a list of
      all days including and between start and endDate.
      :return: list of all possible days including and between start and end date.
    """
    end_date = min(max_date, end_date + datetime.timedelta(30))
    day_range = (end_date - start_date).days + 1
    return [start_date + datetime.timedelta(i) for i in range(day_range)]

# COMMAND ----------

def _all_possible_days(df, date_field, join_fields_list, item_id_col='UPC', store_id_col='ORGANIZATION_UNIT_NUM'):
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
    min_max_date = df.groupBy(store_id_col, item_id_col).agg(
        pyf.min(date_field).alias('startDate'),
        pyf.max(date_field).alias('endDate')
    )
    min_max_date = min_max_date.join(upc_itemid_mapper, min_max_date['UPC'] == upc_itemid_mapper['reported_upc'], how='left')\
        .withColumn('RETAILER_ITEM_ID', pyf.when(pyf.col('retailer_item_id').isNull(),
                                        pyf.col('UPC')).otherwise(pyf.col('retailer_item_id')))\
       .drop('reported_upc')
    max_epos_date = df.select(pyf.max(pyf.col(date_field)))
    max_epos_date = max_epos_date.collect()[0][0]
    explode_dates = min_max_date.rdd \
        .map(lambda r: (r.ORGANIZATION_UNIT_NUM, r.RETAILER_ITEM_ID, r.UPC, _list_days(r.startDate, r.endDate, max_epos_date))) \
        .toDF(['ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID', 'UPC', 'dayList']) \
        .select('ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID', 'UPC', pyf.explode(pyf.col('dayList')).alias(date_field))

    return explode_dates.join(df, [date_field] + join_fields_list + [item_id_col], 'left')

# COMMAND ----------

def compute_price_and_lag_lead_price(df, item_id_col='RETAILER_ITEM_ID', store_id_col='ORGANIZATION_UNIT_NUM', total_lags=14, threshold=0.025, max_discount_run=49):
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

    def create_lag_cols(input_df, item_id_col, store_id_col):
        # Define how the partition of data the lags and leads should be calculated on
        window_spec = Window.partitionBy(
            [item_id_col, store_id_col]
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
    df_lagged, columns_to_drop, coalesce_functions = create_lag_cols(df_priced, item_id_col, store_id_col)
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

# COMMAND ----------

df = get_pos_data(config_dict['epos_datavault_db_nm'], min_date_filter, spark)
print(df.cache().count())
display(df)

# COMMAND ----------

df = df.join(upc_itemid_mapper, df['RETAILER_ITEM_ID'] == upc_itemid_mapper['retailer_item_id'], how='left')\
       .drop(upc_itemid_mapper['retailer_item_id'])\
       .withColumn('UPC', 
                   pyf.when(pyf.col('reported_upc').isNull(),
                            pyf.col('RETAILER_ITEM_ID')
                           ).otherwise(pyf.col('reported_upc')))\
       .drop('reported_upc')

# COMMAND ----------

df = _all_possible_days(df, 'SALES_DT',['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'], 'UPC', 'ORGANIZATION_UNIT_NUM')

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

# COMMAND ----------

grouped_values = df.groupBy('UPC', 'ORGANIZATION_UNIT_NUM', 'SALES_DT')\
  .agg(pyf.sum('POS_ITEM_QTY').alias('POS_ITEM_QTY'),
       pyf.sum('POS_AMT').alias('POS_AMT'),
       pyf.sum('ON_HAND_INVENTORY_QTY').alias('ON_HAND_INVENTORY_QTY'),       
       pyf.avg(pyf.when(df.UNIT_PRICE.isNotNull(), df.UNIT_PRICE)).alias('UNIT_PRICE'))

# COMMAND ----------

display(grouped_values)

# COMMAND ----------

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

# COMMAND ----------

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
df = compute_price_and_lag_lead_price(df, 'UPC')
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
df_pos = df
print(f'{df_pos.cache().count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Intervention Data

# COMMAND ----------

#TODO - read from blob storage for now to save time - remove later
df_intervention_all = spark.read.format('delta').option('inferSchema', 'true').load('/mnt/processed/nafis/vm_upc/nestle-intervention')
df_intervention = df_intervention_all.filter(pyf.col('mdm_banner_id') == banner_id)
print(f'{df_intervention.cache().count():,}')
display(df_intervention)

#  TODO - uncomment later 
# Example Structure
# df_intervention = spark.sql(f'''
#     SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
#     WHERE
#     mdm_country_id = {country_id} and
#     mdm_client_id = {client_id} and
#     mdm_holding_id = {holding_id} and
#     coalesce(mdm_banner_id, -1) = {banner_id}
# ''')

df_intervention = df_intervention.where(pyf.col('call_date') >= min_date_filter)

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

# TODO - remove later - save results
df_merged.write.format('delta')\
    .mode('overwrite')\
    .option('mergeSchema', 'true')\
    .save(f'/mnt/processed/nafis/vm_upc/{client_id}-{country_id}-{holding_id}-{banner_id}')
print(f'{df_merged.cache().count():,}')

df_intervention.write.format('delta')\
    .mode('overwrite')\
    .option('mergeSchema', 'true')\
    .save(f'/mnt/processed/nafis/vm_upc/{client_id}-{country_id}-{holding_id}-{banner_id}-intervention')
print(f'{df_intervention.cache().count():,}')

# COMMAND ----------

display(df_merged)

# COMMAND ----------

print('Intervention Summary Statistics')
df_merged.select('is_intervention').describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Save Results

# COMMAND ----------

#TODO - uncomment
# # Write data
# df_merged.write.format('delta')\
#     .mode('overwrite')\
#     .option('mergeSchema', 'true')\
#     .save(f'/mnt/processed/measurement/cache/{client_id}-{country_id}-{holding_id}-{banner_id}')
# print(f'{df_merged.cache().count():,}')

# df_intervention.write.format('delta')\
#     .mode('overwrite')\
#     .option('mergeSchema', 'true')\
#     .save(f'/mnt/processed/measurement/cache/{client_id}-{country_id}-{holding_id}-{banner_id}-intervention')
# print(f'{df_intervention.cache().count():,}')
