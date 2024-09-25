# Databricks notebook source
import uuid
import numpy as np
import pandas as pd

from time import time

import seaborn as sns
import matplotlib.pyplot as graph

from pyspark.sql import functions as pyf
from pyspark.sql import types as pyt

from datetime import datetime as dtm
from datetime import date

# Models
from catboost import CatBoostRegressor

# Acosta.Alerting package imports
from acosta.alerting.helpers import universal_encoder, universal_decoder, check_path_exists
from acosta.alerting.helpers.features import get_lag_column_name
from acosta.alerting.training import distributed_model_fit, get_partial_distributed_train_func, \
    training_schema_col_name_list, TRAINING_SCHEMA_LIST

import acosta
import pyarrow

print(acosta.__version__)
print(pyarrow.__version__)

# COMMAND ----------

dbutils.widgets.text('retailer', 'walmart', 'Retailer')
dbutils.widgets.text('client', 'clorox', 'Client')
dbutils.widgets.text('countrycode', 'us', 'Country Code')

dbutils.widgets.text('store', '', 'Organization Unit Num')
dbutils.widgets.text('item', '', 'Retailer Item ID')
dbutils.widgets.text('runid', '', 'Run ID')

RETAILER = dbutils.widgets.get('retailer').strip().lower()
CLIENT = dbutils.widgets.get('client').strip().lower()
COUNTRY_CODE  = dbutils.widgets.get('countrycode').strip().lower()

RUN_ID = dbutils.widgets.get('runid').strip()

try:
    STORE = int(dbutils.widgets.get('store').strip())
except ValueError:
    STORE = None
try:
    ITEM = str(dbutils.widgets.get('item').strip())
except ValueError:
    ITEM = None

if RETAILER == '':
    raise ValueError('\'retailer\' is a required parameter.  Please provide a value.')

if CLIENT == '':
    raise ValueError('\'client\' is a required parameter.  Please provide a value.')

if COUNTRY_CODE == '':
    raise ValueError('\'countrycode\' is a required parameter. Please provide a value.')

if RUN_ID == '':
    RUN_ID = str(uuid.uuid4())

# PATHS (new!)
PATH_RESULTS_OUTPUT = '/mnt/artifacts/country_code/training_results/retailer={retailer}/client={client}/country_code={country_code}/'.format(
    retailer=RETAILER,
    client=CLIENT,
    country_code=COUNTRY_CODE
)

PATH_ENGINEERED_FEATURES_OUTPUT = '/mnt/processed/training/{run_id}/engineered/'.format(run_id=RUN_ID)
print(RUN_ID)

# COMMAND ----------

check_path_exists(PATH_ENGINEERED_FEATURES_OUTPUT, file_format='delta', errors='raise')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Setup

# COMMAND ----------

# Filtering of data set
loaded_data = spark.read.format('delta').load(PATH_ENGINEERED_FEATURES_OUTPUT)

if STORE:
    loaded_data = loaded_data.filter('ORGANIZATION_UNIT_NUM == "{}"'.format(STORE))
if ITEM:
    loaded_data = loaded_data.filter('RETAILER_ITEM_ID == "{}"'.format(ITEM))

# This filter requires at least 84 days of non-zero sales in the entire dataset
subset_meets_threshold = loaded_data.select('RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM',
                                            'POS_ITEM_QTY') \
    .filter('POS_ITEM_QTY > 0') \
    .groupBy('RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM') \
    .count() \
    .filter('count >= 84') \
    .drop('count')

data_over_threshold = loaded_data.join(
    pyf.broadcast(subset_meets_threshold),
    ['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'],
    'leftsemi'
)
print(data_over_threshold.rdd.getNumPartitions())
print('N = {:,}'.format(data_over_threshold.count()))

# COMMAND ----------

# Select columns to be included in data frame
lags_to_include = get_lag_column_name(x=range(1, 8))
lags_quesi_loess = get_lag_column_name(x=[i * 7 for i in range(1, 4)])
dynamic_intercepts = [c for c in data_over_threshold.columns if 'TIME_OF_YEAR' in c]
holidays_only = [c for c in data_over_threshold.columns if 'HOLIDAY' in c and '_LAG_' not in c and '_LEAD_' not in c]

# Columns for the model to use
predictor_cols = ['RECENT_ON_HAND_INVENTORY_QTY', 'PRICE', 'SNAPINDEX', 'NONSNAPINDEX']
predictor_cols += ['ORGANIZATION_UNIT_NUM']
predictor_cols += lags_quesi_loess
predictor_cols += dynamic_intercepts
predictor_cols += ['WEEK_SEASONALITY', 'YEAR_SEASONALITY', 'DOW']
# predictor_cols += holidays_only
predictor_cols += lags_to_include[:-1]

mandatory_fields = ['POS_ITEM_QTY', 'SALES_DT', 'RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID']
select_columns = mandatory_fields + predictor_cols

data_over_threshold = data_over_threshold.fillna({
    'ON_HAND_INVENTORY_QTY':0, 
    'RECENT_ON_HAND_INVENTORY_QTY':'0',  # TODO SWAP THESE STRING ZEROS WITH NUMBERS
    'RECENT_ON_HAND_INVENTORY_DIFF':'0'
})
data_with_final_features = data_over_threshold.select(*select_columns).dropna()

# Adding log log to non binary columns
# Note: series is un-log-transformed before performing calculations in mase.py in the package
#       POS_ITEM_QTY is un-log-transformed as BASELINE_POS_ITEM_QTY in 03.1 - Inference Driver
columns_to_be_log_transformed = ['POS_ITEM_QTY', 'RECENT_ON_HAND_INVENTORY_QTY'] \
                                + ['WEEK_SEASONALITY', 'YEAR_SEASONALITY'] \
                                + lags_to_include \
                                + lags_quesi_loess

for column_name in columns_to_be_log_transformed:
    data_with_final_features = data_with_final_features.withColumn(
        column_name,
        pyf.signum(column_name) * pyf.log(pyf.abs(pyf.col(column_name)) + pyf.lit(1))
    )

# Convert long (integers) to some kind of float
for col_name, col_type in data_with_final_features.dtypes:
    if (col_type == 'bigint' or col_type == 'long') and col_name != 'ORGANIZATION_UNIT_NUM':
        data_with_final_features = data_with_final_features.withColumn(
            col_name,
            data_with_final_features[col_name].cast('float')
        )

# Repartition N_cpus x N_Workers =
data_partitioned = data_with_final_features.repartition(64 * 16, 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM')
print('N Partitions', data_partitioned.rdd.getNumPartitions())

# COMMAND ----------

# TODO Add a good comment for this cells
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
            universal_encoder(df)
        ]],
        columns=compress_pos_data_cols
    )

# Try this function out
df_compressed = data_partitioned.groupby('RETAILER_ITEM_ID').apply(compress_pos_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading Champion Models

# COMMAND ----------

champions_path = '/mnt{mod}/artifacts/country_code/champion_models/retailer={retailer}/client={client}/country_code={country_code}'.format(
    mod='/prod-ro',
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

champions.count(), df_compressed.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Inference

# COMMAND ----------

inference_schema = pyt.StructType([
    pyt.StructField('CLIENT', pyt.StringType()),
    pyt.StructField('RETAILER', pyt.StringType()),
    pyt.StructField('COUNTRY_CODE', pyt.StringType()),
    pyt.StructField('MODEL_ID', pyt.StringType()),
    pyt.StructField('RETAILER_ITEM_ID', pyt.StringType()),
    pyt.StructField('ORGANIZATION_UNIT_NUM', pyt.IntegerType()),
    pyt.StructField('PREDICTIONS', pyt.FloatType()),
    pyt.StructField('OBSERVED', pyt.FloatType()),
    pyt.StructField('SALES_DT', pyt.DateType())
])
inference_schema_col_names = [col.name for col in inference_schema]
@pyf.pandas_udf(inference_schema, pyf.PandasUDFType.GROUPED_MAP)
def distributed_inference(payload):
    """
    This function queries the models for it's historical data and
    therefore assumes the input data X are fully specified

    :param payload:
    :return:
    """
    print('N rows received {:,}'.format(len(payload)))
    payload = payload.loc[0, :]  # type: pd.Series

    # Unpacking
    model = universal_decoder(payload['MODEL_OBJECT'])
    df = universal_decoder(payload['DATAFRAME_PICKLE'])  # type: pd.DataFrame
    x_names = payload['COLUMN_NAMES'].split(',')  # type: list[str]

    # Run predict in the special way we need to for lag features to work correctly
    # This mean we need to run this foreach organization unit num
    df_results = []  # type: list[pd.DataFrame]
    for store_id, df_store in df.groupby('ORGANIZATION_UNIT_NUM'):
        tic = time()

        y_hat = model.predict(df_store.loc[:, x_names].values)

        df_results_i = df_store[['SALES_DT']]
        df_results_i['PREDICTIONS'] = y_hat
        df_results_i['OBSERVED'] = df_store['POS_ITEM_QTY'].values
        df_results_i['ORGANIZATION_UNIT_NUM'] = df_store['ORGANIZATION_UNIT_NUM'].unique()[0]

        df_results.append(df_results_i)

        toc = time()
        print('Inference @ Organization Unit Num = {} -> {:.2f}s'.format(store_id, toc - tic))
    df_results = pd.concat(df_results)  # type: pd.DataFrame
    df_results['RETAILER_ITEM_ID'] = payload['RETAILER_ITEM_ID']
    df_results['RETAILER'] = payload['RETAILER']
    df_results['CLIENT'] = payload['CLIENT']
    df_results['COUNTRY_CODE'] = payload['COUNTRY_CODE']
    df_results['MODEL_ID'] = payload['MODEL_ID']
    print('=== Done ===\n')

    return df_results[inference_schema_col_names]


# COMMAND ----------

df_distributed_input = df_compressed.join(champions, ['RETAILER_ITEM_ID'], 'inner')
print('{:,}'.format(df_distributed_input.cache().count()))

# COMMAND ----------

df_inferred = df_distributed_input.groupby('RETAILER_ITEM_ID').apply(distributed_inference)
print('{:,}'.format(df_inferred.cache().count()))

# COMMAND ----------

# display(data_partitioned)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare results

# COMMAND ----------

df_inferred_filtered = df_inferred.filter((df_inferred.SALES_DT >= date(2019, 1, 1)) & (df_inferred.SALES_DT <= date(2020, 5, 14)))
df_inferred_filtered = df_inferred_filtered.selectExpr(
                                          'CLIENT',
                                          'RETAILER',
                                          'COUNTRY_CODE',
                                          'RETAILER_ITEM_ID',
                                          'ORGANIZATION_UNIT_NUM',
                                          'SALES_DT',
                                          'round(exp(PREDICTIONS)-1, 2) as BASELINE_POS_ITEM_QTY',
                                          'round(exp(OBSERVED)-1, 2) as POS_ITEM_QTY')

# COMMAND ----------

display(df_inferred_filtered)

# COMMAND ----------

display(df_inferred_filtered.selectExpr('max(SALES_DT)', 'min(SALES_DT)'))


# COMMAND ----------

print('{:,}'.format(df_inferred_filtered.count()))

# COMMAND ----------

# # Save the data set
# df_inferred_filtered.coalesce(1) \
#     .write.mode('overwrite').format('csv').option('header', 'true') \
#     .save('/mnt/artifacts/hugh/kraftheinz-sainsbury')

# COMMAND ----------

# MAGIC %md
# MAGIC # Results Summary
# MAGIC
# MAGIC (TODO modify if you want)

# COMMAND ----------

# print('Trained Model Count: {:,}'.format(train_results.count()))
# print('Output Model Count: {:,}'.format(train_results.filter('MODEL_ID is not null').count()))

# COMMAND ----------

# df = train_results.filter('MODEL_ID is not null') \
#     .select('RETAILER_ITEM_ID', 'METRICS_R2_TEST', 'METRICS_R2_TRAIN', 'METRICS_RMSE_TEST', 'METRICS_RMSE_TRAIN') \
#     .toPandas()

# graph.title('Training Performance: {:,} Models'.format(len(df)))
# sns.distplot(df['METRICS_R2_TRAIN'], kde=False, color='seagreen')
# graph.xlabel('$R^2$')
# display(graph.show())
# graph.close()

# COMMAND ----------

# graph.title('Testing Performance: {:,} Models'.format(len(df)))
# sns.distplot(df['METRICS_R2_TEST'], kde=False)
# graph.xlabel('$R^2$')
# display(graph.show())
# graph.close()
