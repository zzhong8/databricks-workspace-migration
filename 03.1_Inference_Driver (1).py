# Databricks notebook source
# MAGIC %md
# MAGIC # Inference Driver

# COMMAND ----------

import pyspark.sql.types as pyt
import pyspark.sql.functions as pyf
from pyspark.sql.types import *
import pandas as pd
import datetime

import pickle

from catboost import CatBoostRegressor

from acosta.alerting.preprocessing import pos_to_training_data, read_pos_data
from acosta.alerting.preprocessing.functions import get_hash_org_unit_num_udf
from acosta.alerting.forecast import distributed_predict, prediction_schema
from acosta.alerting.helpers import check_path_exists, universal_encoder, universal_decoder

import acosta
print(acosta.__version__)

# COMMAND ----------

# Interface
dbutils.widgets.text('RETAILER_PARAM', 'walmart', 'Retailer')
dbutils.widgets.text('CLIENT_PARAM', 'clorox', 'Client')
dbutils.widgets.text('COUNTRY_CODE_PARAM', 'us', 'Country Code')

dbutils.widgets.text('LAST_DAY_TO_PREDICT_PARAM', '', 'Last Day to Predict (YYYYMMDD)')
dbutils.widgets.text('NUM_DAYS_TO_PREDICT_PARAM', '7', 'Number of Days to Predict')
dbutils.widgets.text('MAX_LAG', '30', 'Maximum Days to Lag')

dbutils.widgets.dropdown('ENVIRONMENT', 'dev', ['dev', 'prod'], 'Environment')
dbutils.widgets.dropdown('MODEL_SOURCE', 'local', ['local', 'prod'], 'Model Source')
dbutils.widgets.dropdown('INCLUDE_DISCOUNT_FEATURES', 'no', ['yes', 'no'], 'Include Discount Features')

# Process input parameters
RETAILER = dbutils.widgets.get('RETAILER_PARAM').strip().casefold()
CLIENT = dbutils.widgets.get('CLIENT_PARAM').strip().casefold()
COUNTRY_CODE = dbutils.widgets.get('COUNTRY_CODE_PARAM').strip().casefold()

if len(RETAILER) == 0 or len(CLIENT) == 0 or len(COUNTRY_CODE) == 0:
    raise ValueError('Client, Retailer and Country Code must be filled in.')

LAST_PREDICTED_DATE = datetime.datetime.strptime(dbutils.widgets.get('LAST_DAY_TO_PREDICT_PARAM'), '%Y%m%d')
DAYS_TO_PREDICT = int(dbutils.widgets.get('NUM_DAYS_TO_PREDICT_PARAM'))
MINIMUM_DAYS_TO_LAG = int(dbutils.widgets.get('MAX_LAG'))
MODEL_SOURCE = dbutils.widgets.get('MODEL_SOURCE').casefold()
ENVIRONMENT = dbutils.widgets.get('ENVIRONMENT').casefold()

ENVIRONMENT = 'prod' if ENVIRONMENT.startswith('prod') else 'dev'
MODEL_SOURCE = 'local' if MODEL_SOURCE.startswith('local') else 'prod'

input_discount_string = dbutils.widgets.get('INCLUDE_DISCOUNT_FEATURES').strip().casefold()
INCLUDE_DISCOUNT_FEATURES = 'y' in input_discount_string or 't' in input_discount_string

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and preprocess data

# COMMAND ----------

champions_path = '/mnt{mod}/artifacts/country_code/champion_models/retailer={retailer}/client={client}/country_code={country_code}'.format(
    mod='' if MODEL_SOURCE == 'local' else '/prod-ro',
    retailer=RETAILER,
    client=CLIENT,
    country_code=COUNTRY_CODE
)

check_path_exists(champions_path, 'delta')

champions = spark.read.format('delta') \
    .load(champions_path) \
    .drop('RETAILER', 'CLIENT', 'COUNTRY_CODE') \
    .withColumnRenamed('ORGANIZATION_UNIT_NUM', 'ORGANIZATION_UNIT_LIST')

# COMMAND ----------

# Read POS data
sales_data_subset = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext)  # .repartition('SALES_DT')

# Generate the filter the last N days
LAST_N_DAYS_FILTER = 'SALES_DT >= "{startDateHyphen}" and SALES_DT < "{endDateHyphen}"'.format(
    startDateHyphen=(LAST_PREDICTED_DATE - datetime.timedelta(MINIMUM_DAYS_TO_LAG + DAYS_TO_PREDICT)).strftime(
        '%Y-%m-%d'),
    endDateHyphen=(LAST_PREDICTED_DATE - datetime.timedelta(DAYS_TO_PREDICT)).strftime('%Y-%m-%d')
)
print(LAST_N_DAYS_FILTER)

# Process negative values
sales_data_subset = sales_data_subset \
    .filter(LAST_N_DAYS_FILTER) \
    .withColumn('POS_ITEM_QTY', pyf.when(pyf.col('POS_ITEM_QTY') >= 0, pyf.col('POS_ITEM_QTY')).otherwise(0)) \
    .withColumn('POS_AMT', pyf.when(pyf.col('POS_AMT') >= 0, pyf.col('POS_AMT')).otherwise(0))

# COMMAND ----------

# Get distinct values and combine them
unique_org_item = sales_data_subset.select('ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID').distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check to see if the cross-join table exists

# COMMAND ----------

date_reference_path = '/mnt{mod}/artifacts/country_code/reference/store_item_dates/retailer={retailer}/client={client}/country_code={country_code}/'.format(
    mod='' if MODEL_SOURCE == 'local' else '/prod-ro',
    retailer=RETAILER,
    client=CLIENT,
    country_code=COUNTRY_CODE
)
print(date_reference_path)

cross_join_table_exists_flag = check_path_exists(date_reference_path, 'delta', 'ignore')
if cross_join_table_exists_flag is False:
    # TODO (2/22/2020) is the comment below actually true?
    # NOTE: we would like there to be a way for the notebook to notify us if this happens since it should not happen
    print('Cross-join Table not found for client')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cross-join table Exists, so load it

# COMMAND ----------

# Retrieve the store_item_dates table from the 'reference folder' in the storage account
if cross_join_table_exists_flag:
    df_store_item_dates = spark.read.format('delta').load(date_reference_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check to see if the cross-join table encompasses the dates that we are using for our forecast

# COMMAND ----------

if cross_join_table_exists_flag:
    df_store_item_date_min_max = df_store_item_dates.select(pyf.min('SALES_DT'), pyf.max('SALES_DT')).collect()

    df_store_item_date_min = datetime.datetime.combine(df_store_item_date_min_max[0][0], datetime.datetime.min.time())
    df_store_item_date_max = datetime.datetime.combine(df_store_item_date_min_max[0][1], datetime.datetime.min.time())

# COMMAND ----------

# Dates that we care about
if cross_join_table_exists_flag:
    LAST_PREDICTED_DATE  # Directly From Widget
    FIRST_PREDICTED_DATE = LAST_PREDICTED_DATE - datetime.timedelta(DAYS_TO_PREDICT - 1)
    FIRST_DAY_OF_LAGS = LAST_PREDICTED_DATE - datetime.timedelta(MINIMUM_DAYS_TO_LAG + DAYS_TO_PREDICT - 1)

    # If the first day of lags is earlier than the earliest date that we have in the cross-join table
    if FIRST_DAY_OF_LAGS < df_store_item_date_min:
        cross_join_table_exists_flag = False

        print('The first day of lags is earlier than the earliest date that we have in the cross-join table')

    # If the last day to predict is later than the latest date that we have in the cross-join table
    elif LAST_PREDICTED_DATE > df_store_item_date_max:
        cross_join_table_exists_flag = False

        print('The last day to predict is later than the latest date that we have in the cross-join table')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cross-join table encompasses the dates that we are using for our forecast, so use it

# COMMAND ----------

if cross_join_table_exists_flag:
    df_store_item_last_n_days_filter = '(SALES_DT >= "{startDateHyphen}" and SALES_DT <= "{endDateHyphen}")'.format(
        startDateHyphen=FIRST_DAY_OF_LAGS.strftime('%Y-%m-%d'),
        endDateHyphen=(LAST_PREDICTED_DATE).strftime('%Y-%m-%d')
    )

    df_store_item_last_n_days = df_store_item_dates.filter(df_store_item_last_n_days_filter) \
        .select(['SALES_DT', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM']) \
        .withColumn('IS_PREDICTION_ROW', pyf.col('SALES_DT') >= FIRST_PREDICTED_DATE.strftime('%Y-%m-%d'))

# COMMAND ----------

if cross_join_table_exists_flag:
    # Get all combinations of the orgs + items and the dates selected
    df_store_item_last_n_days = df_store_item_last_n_days.join(unique_org_item,
                                                               ['ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID'])

    # Add columns from the loaded sales data
    input_dataset = df_store_item_last_n_days.join(
        sales_data_subset,
        ['SALES_DT', 'ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID'],
        'left'
    )

else:
    print('Either the cross-join table does not exist for this client, or some of the required dates are missing')
    # Setup the list of days needed to collect and flag those within the days to predict range
    days_to_collect = [(LAST_PREDICTED_DATE - datetime.timedelta(days=i), False)
                       for i in range(0, MINIMUM_DAYS_TO_LAG + DAYS_TO_PREDICT)]

    days_flagged = [[day.date(), day > (LAST_PREDICTED_DATE - datetime.timedelta(days=DAYS_TO_PREDICT))]
                    for day, _ in days_to_collect]

    dates_and_predict_flag = sc.parallelize(days_flagged).toDF(['SALES_DT', 'IS_PREDICTION_ROW'])

    # Get all combinations of the orgs + items and the dates selected
    all_rows_of_loaded_and_dates = dates_and_predict_flag.crossJoin(unique_org_item)

    input_dataset = all_rows_of_loaded_and_dates.join(
        sales_data_subset,
        ['SALES_DT', 'ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID'],
        'left'
    )

# COMMAND ----------

df_pos = pos_to_training_data(
    df=input_dataset,
    retailer=RETAILER,
    client=CLIENT,
    country_code=COUNTRY_CODE,
    model_source=MODEL_SOURCE,
    spark=spark,
    spark_context=sc,
    include_discount_features=INCLUDE_DISCOUNT_FEATURES,
    item_store_cols=['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM']
).filter('IS_PREDICTION_ROW == True')

df_pos = df_pos \
    .withColumn('RETAILER', pyf.lit(RETAILER)) \
    .withColumn('CLIENT', pyf.lit(CLIENT)) \
    .withColumn('COUNTRY_CODE', pyf.lit(COUNTRY_CODE))

# Add all the correct log transformations now!
columns_to_log = ['POS_ITEM_QTY', 'RECENT_ON_HAND_INVENTORY_QTY', 'RECENT_ON_HAND_INVENTORY_DIFF'] \
                 + ['WEEK_SEASONALITY', 'YEAR_SEASONALITY'] \
                 + [col for col in df_pos.columns if 'LAG_UNITS' in col]

for column_name in columns_to_log:
    if column_name not in df_pos.columns:
        continue  # Skip this column

    df_pos = df_pos.withColumn(
        column_name,
        pyf.signum(column_name) * pyf.log(pyf.abs(pyf.col(column_name)) + pyf.lit(1))
    )
    print(column_name, 'done')

# COMMAND ----------

# Compress pos data for inference
compress_pos_data_schema = pyt.StructType([
    pyt.StructField('RETAILER', pyt.StringType()),
    pyt.StructField('CLIENT', pyt.StringType()),
    pyt.StructField('COUNTRY_CODE', pyt.StringType()),
    pyt.StructField('RETAILER_ITEM_ID', pyt.StringType()),
    pyt.StructField('DATAFRAME_PICKLE', pyt.StringType())
])
compress_pos_data_cols = [col.name for col in compress_pos_data_schema]


@pyf.pandas_udf(compress_pos_data_schema, pyf.PandasUDFType.GROUPED_MAP)
def compress_pos_data(df):
    return pd.DataFrame(
        [[
            df['RETAILER'].unique()[0],
            df['CLIENT'].unique()[0],
            df['COUNTRY_CODE'].unique()[0],
            df['RETAILER_ITEM_ID'].unique()[0],
            universal_encoder(df, True)
        ]],
        columns=compress_pos_data_cols
    )


df_compressed = df_pos.groupby('RETAILER_ITEM_ID').apply(compress_pos_data)

# COMMAND ----------

df_distributed_input = df_compressed.join(champions, ['RETAILER_ITEM_ID'], 'inner')
print('{:,}'.format(df_distributed_input.cache().count()))

# COMMAND ----------

# MAGIC %md
# MAGIC # Inference

# COMMAND ----------

df_predictions = df_distributed_input.groupby('RETAILER_ITEM_ID').apply(distributed_predict)
print('{:,}'.format(df_predictions.cache().count()))

# COMMAND ----------

# MAGIC %md
# MAGIC # Process and export results

# COMMAND ----------

# rename columns and add additional metadata to the predictions
prediction_results_dataframe = df_predictions.selectExpr(
    'ORGANIZATION_UNIT_NUM',
    'RETAILER_ITEM_ID',
    'CURRENT_TIMESTAMP() as LOAD_TS',
    '"Dynamic.Retail.Forecast.Engine" as RECORD_SOURCE_CD',
    'exp(PREDICTIONS)-1 as BASELINE_POS_ITEM_QTY',
    'SALES_DT',
    'MODEL_ID'
)

# LOOK UP HASH KEYS
databaseName = '{}_{}_{}_dv'.format(RETAILER.lower(), CLIENT.lower(), COUNTRY_CODE.lower())
itemMasterTableName = '{}.hub_retailer_item'.format(databaseName)
storeMasterTableName = '{}.hub_organization_unit'.format(databaseName)

store_master_table = sqlContext.read.table(storeMasterTableName)
for col_name, col_type in store_master_table.dtypes:
    if col_name.casefold() == 'organization_unit_num' and 'string' in col_type:
        store_master_table = store_master_table.withColumn(
            'ORGANIZATION_UNIT_NUM',
            get_hash_org_unit_num_udf()(pyf.col('ORGANIZATION_UNIT_NUM'))
        )
store_master_table = store_master_table.alias('OUH')

prediction_results_dataframe = prediction_results_dataframe.alias('PRDF') \
    .join(store_master_table,
          pyf.col('OUH.ORGANIZATION_UNIT_NUM') == pyf.col('PRDF.ORGANIZATION_UNIT_NUM'), 'inner') \
    .join(sqlContext.read.table(itemMasterTableName).alias('RIH'),
          pyf.col('RIH.RETAILER_ITEM_ID') == pyf.col('PRDF.RETAILER_ITEM_ID'), 'inner') \
    .select('PRDF.*', 'OUH.HUB_ORGANIZATION_UNIT_HK', 'RIH.HUB_RETAILER_ITEM_HK')

# COMMAND ----------

# Insert into table rather than blob
# The Hive table definition will determine format, partitioning, and location of the data file...
# so we don't have to worry about those low-level details
insertDatabaseName = databaseName
if ENVIRONMENT == 'dev':
    insertDatabaseName = 'RETAIL_FORECAST_ENGINE'
elif ENVIRONMENT == 'prod':
    insertDatabaseName = '{}_{}_{}_retail_alert_im'.format(RETAILER.lower(), CLIENT.lower(), COUNTRY_CODE.lower())

insertTableName = '{}.DRFE_FORECAST_BASELINE_UNIT'.format(insertDatabaseName)

# COMMAND ----------

print('Confirm non-zero count {:,}'.format(prediction_results_dataframe.cache().count()))

# COMMAND ----------

# prediction_results_dataframe \
#     .select('HUB_ORGANIZATION_UNIT_HK', 'HUB_RETAILER_ITEM_HK', 'LOAD_TS', 'RECORD_SOURCE_CD', 'BASELINE_POS_ITEM_QTY',
#             'MODEL_ID', 'SALES_DT') \
#     .write.mode('overwrite').insertInto(insertTableName, overwrite=True)
