# Databricks notebook source
import uuid
import numpy as np
import pandas as pd

import seaborn as sns
import matplotlib.pyplot as graph

from pyspark.sql import functions as pyf
from pyspark.sql import types as pyt

from datetime import datetime as dtm

# Models
from catboost import CatBoostRegressor

# Acosta.Alerting package imports
from acosta.alerting.helpers.features import get_lag_column_name
from acosta.alerting.training import distributed_model_fit, ALERT_TRAINING_SCHEMA_LIST

import acosta

print(acosta.__version__)

# COMMAND ----------

from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_squared_error
from pyspark.sql import Row

ALERT_TRAINING_SCHEMA_LIST = [
    pyt.StructField("ORGANIZATION_UNIT_NUM", pyt.IntegerType(), True),
    pyt.StructField("RETAILER_ITEM_ID", pyt.StringType(), True),
    pyt.StructField("MODEL_METADATA", pyt.StringType(), True),
    pyt.StructField("COLUMN_NAMES", pyt.ArrayType(pyt.StringType(), True), True),
    pyt.StructField("DATASETS", pyt.MapType(pyt.StringType(), pyt.ArrayType(pyt.FloatType(),True), True), True),
    pyt.StructField("DATES", pyt.MapType(pyt.StringType(), pyt.ArrayType(pyt.DateType(),True), True), True),
    pyt.StructField("METRICS", pyt.MapType(pyt.StringType(), pyt.FloatType(),True), True),
    pyt.StructField("TRAIN_NON_ZERO_DAYS", pyt.IntegerType(), True),
    pyt.StructField("TRAIN_DAYS_COUNT", pyt.IntegerType(), True),
    pyt.StructField("TEST_NON_ZERO_DAYS", pyt.IntegerType(), True),
    pyt.StructField("TEST_DAYS_COUNT", pyt.IntegerType(), True),
    pyt.StructField("OBSERVED_DATE_RANGE_MIN", pyt.DateType(), True),
    pyt.StructField("OBSERVED_DATE_RANGE_MAX", pyt.DateType(), True),
    pyt.StructField("MODEL_ID", pyt.StringType(), True),
    pyt.StructField("MODEL_OBJECT", pyt.BinaryType(), True)
]


TRAINING_SCHEMA_LIST = [
    pyt.StructField('ORGANIZATION_UNIT_NUM', pyt.IntegerType()),
    pyt.StructField('RETAILER_ITEM_ID', pyt.StringType()),
    pyt.StructField('MODEL_METADATA', pyt.StringType()),
#     pyt.StructField('COLUMN_NAMES', pyt.ArrayType(pyt.StringType())),
#     pyt.StructField('DATES_TRAIN', pyt.ArrayType(pyt.DateType())),
#     pyt.StructField('DATES_TEST', pyt.ArrayType(pyt.DateType())),
    pyt.StructField('METRICS_RMSE_TRAIN', pyt.FloatType()),
    pyt.StructField('METRICS_RMSE_TEST', pyt.FloatType()),
    pyt.StructField('METRICS_MSE_TRAIN', pyt.FloatType()),
    pyt.StructField('METRICS_MSE_TEST', pyt.FloatType()),
    pyt.StructField('METRICS_R2_TRAIN', pyt.FloatType()),
    pyt.StructField('METRICS_R2_TEST', pyt.FloatType()),
    pyt.StructField('TRAIN_NON_ZERO_DAYS', pyt.IntegerType()),
    pyt.StructField('TRAIN_DAYS_COUNT', pyt.IntegerType()),
    pyt.StructField('TEST_NON_ZERO_DAYS', pyt.IntegerType()),
    pyt.StructField('TEST_DAYS_COUNT', pyt.IntegerType()),
#     pyt.StructField('OBSERVED_DATE_RANGE_MIN', pyt.DateType()),
#     pyt.StructField('OBSERVED_DATE_RANGE_MAX', pyt.DateType()),
    pyt.StructField('MODEL_ID', pyt.StringType()),
    pyt.StructField('MODEL_OBJECT', pyt.BinaryType())
]

_training_schema_col_name_list = [col.name for col in TRAINING_SCHEMA_LIST]
print(_training_schema_col_name_list)


def safe_float(x, else_return=9999):
    return float(else_return) if np.isnan(x) or np.isinf(x) else x
  

def distributed_model_fit(x_names, y_name, df, test_size, algorithm_list, **kwargs):
    """
    Wrapper for fitting models in the training_unit_list.

    :param x_names: Predictor columns names from the df
    :param y_name: Target variable to predict
    :param df: list of data that can be turned in a DataFrame
    :param test_size: proportion of data that should be used to for validation and model selection
    :param algorithm_list: list of initialised ML models. (Must have Sklearn API)
    :return:
    """
    import pickle

    # Processing
    ITEM_ID = str(df.loc[0, 'RETAILER_ITEM_ID'])
    STORE_IDS = df['ORGANIZATION_UNIT_NUM'].unique().tolist()

    DATE_RANGE = df['SALES_DT'].min(), df['SALES_DT'].max()

    df_without_nulls = df.loc[:, [y_name] + x_names].dropna(axis=0)

    dates_used = (pd.to_datetime(df.SALES_DT[df.index]).values - np.datetime64('1970-01-01T00:00:00Z'))
    dates_used = dates_used / np.timedelta64(1, 's')
    dates_used = [datetime.utcfromtimestamp(int(d)) for d in dates_used.tolist()]

    # Create X and y data
    y = df_without_nulls.pop(y_name).astype('float').values
    x = df_without_nulls
    x_train, x_test, y_train, y_test, DATES_USED_TRAIN, DATES_USED_TEST = train_test_split(
        x, y, dates_used,
        test_size=test_size
    )

    # Fit models and select the best one
    best_score = 9999.9
    results_history = {}
    best_key = 0
    best_model = None
    best_dates = {}
    for ith, model_i in enumerate(algorithm_list):
        # Predict depending on model
        if isinstance(model_i, CatBoostRegressor):
            model_i.fit(x_train, y_train, eval_set=[(x_test, y_test)])

            # Estimate y
            y_pred_train = model_i.predict(x_train, ntree_end=model_i.best_iteration_)
            y_pred_test = model_i.predict(x_test, ntree_end=model_i.best_iteration_)
        else:
            # if the model type cannot be inferred then treat it like a normal SKLearn model
            model_i.fit(x_train, y_train)

        # Ensure all predictions are infinite
        y_pred_train[~np.isfinite(y_pred_train)] = 0
        y_pred_test[~np.isfinite(y_pred_test)] = 0

        # Output variables
        try:
            TESTSET_METRIC = np.sqrt(mean_squared_error(y_test, y_pred_test))
        except ValueError:
            TESTSET_METRIC = 10e3
            print('Item {} {} model predicts NaNs'.format(ITEM_ID, model_i))

        TRAIN_NON_ZERO_DAYS = np.sum(y_train > 0)
        TRAIN_DAYS_COUNT = len(y_train)
        TEST_NON_ZERO_DAYS = np.sum(y_test > 0)
        TEST_DAYS_COUNT = len(y_test)

        # Append to results history
        # Save model for each store ID
        given_model_results = []
        for store_id in STORE_IDS:
            given_model_results.append({
                'ORGANIZATION_UNIT_NUM': store_id,
                'RETAILER_ITEM_ID': ITEM_ID,
                'MODEL_METADATA': str(model_i),
                'COLUMN_NAMES': x_names,
                'DATES_TRAIN': [],
                'DATES_TEST': [],
                'METRICS_RMSE_TRAIN': safe_float(float(np.sqrt(mean_squared_error(y_train, y_pred_train)))),
                'METRICS_RMSE_TEST': safe_float(float(np.sqrt(mean_squared_error(y_test, y_pred_test)))),
                'METRICS_MSE_TRAIN': safe_float(float(mean_squared_error(y_train, y_pred_train))),
                'METRICS_MSE_TEST': safe_float(float(mean_squared_error(y_test, y_pred_test))),
                'METRICS_R2_TRAIN': safe_float(float(r2_score(y_train, y_pred_train)), -9999),
                'METRICS_R2_TEST': safe_float(float(r2_score(y_test, y_pred_test)), -9999),
                'TRAIN_NON_ZERO_DAYS': int(TRAIN_NON_ZERO_DAYS),
                'TRAIN_DAYS_COUNT': int(TRAIN_DAYS_COUNT),
                'TEST_NON_ZERO_DAYS': int(TEST_NON_ZERO_DAYS),
                'TEST_DAYS_COUNT': int(TEST_DAYS_COUNT),
                'OBSERVED_DATE_RANGE_MIN': DATE_RANGE[0],
                'OBSERVED_DATE_RANGE_MAX': DATE_RANGE[1],
                'MODEL_ID': '',
                'MODEL_OBJECT': bytearray(pickle.dumps(None))
            })
        results_history[ith] = given_model_results

        if TESTSET_METRIC < best_score or ith == 0:
            best_score = TESTSET_METRIC
            best_key = ith
            best_model = model_i
            best_dates = {
                'train': DATES_USED_TRAIN,
                'test': DATES_USED_TEST
            }
    # End of training loop
    if len(algorithm_list) != len(results_history.keys()):
        raise ValueError('Programming Error. The results dictionary should be as long as the number of models')

    # Save only the best model
    # This is a list of store item dictionaries. Replace the null values with the best ones on the best index
    for i, _ in enumerate(results_history[best_key]):
        results_history[best_key][i].update({
            'MODEL_ID': str(uuid.uuid4()),
            'MODEL_OBJECT': bytearray(pickle.dumps(best_model)),
            'DATES_TRAIN': best_dates['train'],
            'DATES_TEST': best_dates['test']
        })

    # Flatten all results and convert to lists for stacking into in a DataFrame
    results = []
    for key in results_history.keys():
        for idx, result_i in enumerate(results_history[key]):
            result_row_i = [result_i[col_name] for col_name in _training_schema_col_name_list]
            results.append(result_row_i)
    results = pd.DataFrame(results, columns=_training_schema_col_name_list)
    print('\nDataFrame Created')
    print(results.head())

    # Hint garbage collection (Required do not remove)
    df = None
    x, x_train, x_test = 3*[None]
    y, y_train, y_test = 3*[None]
    model_i = None
    print('===DONE===')

    return results


def get_partial_distributed_train_func(x_names, y_name, test_size, algorithm_list, **kwargs):
    """
    Creates the UDF compatible function

    :param x_names: list
    :param y_name: target variable
    :param test_size:
    :param algorithm_list:
    :param kwargs: Extra arguments to pass to distributed_model_fit(). Never pass `df`
    :return: distributed_train_udf()
    """
    import functools
    from pyspark.sql.functions import pandas_udf, PandasUDFType

    if 'df' in kwargs:
        raise AssertionError('Do NOT pass `df` to this function!')

    if not isinstance(x_names, list):
        raise ValueError('`x_names` must be a list of column names')
    if not isinstance(y_name, str):
        raise ValueError('`y_name` must be a string for the target variable')
    if not(isinstance(test_size, float) or isinstance(test_size, int)):
        raise ValueError('`test_size` must be a number')
    if not isinstance(algorithm_list, list):
        raise ValueError('`algorithm_list` must be a list')

    partial_function = functools.partial(
        distributed_model_fit,
        x_names=x_names, y_name=y_name,
        test_size=test_size,
        algorithm_list=algorithm_list,
        **kwargs
    )
    return partial_function

# COMMAND ----------

dbutils.widgets.text('retailer', 'WALMART', 'Retailer')
dbutils.widgets.text('client', 'CLOROX', 'Client')
dbutils.widgets.text('store', '', 'Organization Unit Num')
dbutils.widgets.text('item', '', 'Retailer Item ID')
dbutils.widgets.text('runid', '', 'Run ID')

RETAILER = dbutils.widgets.get('retailer').strip().upper()
CLIENT = dbutils.widgets.get('client').strip().upper()
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

if RUN_ID == '':
    RUN_ID = str(uuid.uuid4())

# PATHS
PATH_RESULTS_OUTPUT = '/mnt/artifacts/training_results/retailer={retailer}/client={client}/'.format(
    retailer=RETAILER,
    client=CLIENT
)
PATH_ENGINEERED_FEATURES_OUTPUT = '/mnt/processed/training/{run_id}/engineered/'.format(run_id=RUN_ID)
print(RUN_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Setup

# COMMAND ----------

# Filtering of data set
loaded_data = spark.read.format('delta').load(PATH_ENGINEERED_FEATURES_OUTPUT)

if STORE:
    loaded_data = loaded_data.filter('ORGANIZATION_UNIT_NUM == "{}"'.format(STORE))
if ITEM:
    loaded_data = loaded_data.filter('RETAILER_ITEM_ID == "{}"'.format(ITEM))

# This filter requires at least 84 days of non-zero sales in the entire dataset
subset_meets_threshold = loaded_data.select('RETAILER', 'CLIENT', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM',
                                            'POS_ITEM_QTY') \
    .filter('POS_ITEM_QTY > 0') \
    .groupBy('RETAILER', 'CLIENT', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM') \
    .count() \
    .filter('count >= 84') \
    .drop('count')

data_over_threshold = loaded_data.join(
    subset_meets_threshold,
    ['RETAILER', 'CLIENT', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'],
    'inner'
)
print(data_over_threshold.rdd.getNumPartitions())

# COMMAND ----------

display(dbutils.fs.ls(PATH_ENGINEERED_FEATURES_OUTPUT))

# COMMAND ----------

data_over_threshold.count()

# COMMAND ----------

# Select columns to be included in data frame
lags_to_include = get_lag_column_name(x=range(1, 8))
lags_quesi_loess = get_lag_column_name(x=[i*7 for i in range(1, 4)])
dynamic_intercepts = [c for c in data_over_threshold.columns if 'TIME_OF_YEAR' in c]
holidays_only = [c for c in data_over_threshold.columns if 'HOLIDAY' in c and "_LAG_" not in c and "_LEAD_" not in c]

# Columns for the model to use
predictor_cols = ['RECENT_ON_HAND_INVENTORY_QTY', 'PRICE', 'SNAPINDEX', 'NONSNAPINDEX']
predictor_cols += ['ORGANIZATION_UNIT_NUM']
predictor_cols += lags_quesi_loess
predictor_cols += dynamic_intercepts
predictor_cols += ['WEEK_SEASONALITY', 'YEAR_SEASONALITY', 'DOW']
# predictor_cols += holidays_only
predictor_cols += lags_to_include[:-1]

mandatory_fields = ["POS_ITEM_QTY", "SALES_DT", "RETAILER", "CLIENT", "RETAILER_ITEM_ID"]
select_columns = mandatory_fields + predictor_cols

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

# Repartition N_cpus x N_Workers = 
data_partitioned = data_with_final_features.repartition(64*16, 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM')
print('N Partitions', data_partitioned.rdd.getNumPartitions())
# display(data_partitioned.groupby('RETAILER_ITEM_ID').count())

# COMMAND ----------

print(data_partitioned.select('RETAILER_ITEM_ID').distinct().show())

# COMMAND ----------

data_partitioned.createOrReplaceTempView("data_partitioned")

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.table("data_partitioned").schema.toDDL

# COMMAND ----------

print('{:,}'.format(data_partitioned.count()))

# COMMAND ----------

# Create instances of training units
algorithm_list = [
    CatBoostRegressor(
        iterations=10000,
        learning_rate=0.1,
        loss_function='RMSE',
        cat_features=['DOW', 'ORGANIZATION_UNIT_NUM'],
        use_best_model=True,
        early_stopping_rounds=25,
        verbose=True
    ),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Training

# COMMAND ----------

train_func = get_partial_distributed_train_func(
    x_names=predictor_cols, 
    y_name='POS_ITEM_QTY', 
    test_size=0.1, 
    algorithm_list=algorithm_list
)


@pyf.pandas_udf(pyt.StructType(TRAINING_SCHEMA_LIST), pyf.PandasUDFType.GROUPED_MAP)
def train_udf(df):
    return train_func(df=df)

# COMMAND ----------

df = spark.range(1,1000)

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df.count()

# COMMAND ----------

df.cache().count()

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df.count()

# COMMAND ----------

data_partitioned.cache().count()

# COMMAND ----------

# Apply with UDF
train_results = data_partitioned.groupby('RETAILER_ITEM_ID').apply(train_udf)
display(train_results)

# COMMAND ----------

train_results.explain(True)

# COMMAND ----------

# Apply with UDF
# train_results = data_partitioned.groupby('RETAILER_ITEM_ID').apply(train_udf)
# display(train_results)

# COMMAND ----------

print('Training initiated at {}'.format(dtm.now().strftime('%Y%m%d:%H:%M')))

#  A basic overview of trained_models_rdd construction:
# .map: Change RDD of rows into K,V pairs of identifiers and features as dict (same row count)
# .reduceByKey: For common Key (store,UPC): append daily features as a list of dicts (fewer rows, 1 per store, item)
# .mapValues: Passing list of dictionaries (features) to a function that will be constructed into a Pandas DF
#     ^-- This now is doing work on a single node for an individual item, store.
#
# The end result is an rdd containing the _results_history list of dictionaries containing
# all training units' results and the identifier and model object for the best training unit.

trained_models_rdd = data_partitioned.rdd \
    .map(lambda r: ((r.RETAILER_ITEM_ID), [r.asDict()])) \
    .reduceByKey(lambda accum, r: accum + r) \
    .mapValues(lambda x: distributed_model_fit(
        x_names=predictor_cols,
        y_name='POS_ITEM_QTY',
        df=x,
        test_size=0.1,
        training_unit_list=algorithm_list
    ))

# .flatMapValue is taking a 3D object - stack of training results, and flattens to a list of individual runs
# with repeating key entries
# Having an extraction function will simplify the extraction of tuple elements + dictionary values
# into the results columns.
# This would be equivalent to having a json result written and parsing it in.
model_performance = trained_models_rdd \
    .flatMapValues(lambda entry: entry) \
    .map(lambda entry: entry[1])

# Jump of out rdd's and back into Spark Dataframes
# The columns must be sorted because the distributed_train function unpacks 
# the dictionary into a row object.  When a dictionary is unpacked into a 
# Row it sorts the keys for backward compatibility with older verions of python
model_performance = spark.createDataFrame(
    model_performance,
    pyt.StructType(sorted(ALERT_TRAINING_SCHEMA_LIST, key=lambda x: x.name))
)

# Write out the dataframe to parquet for reference
display(model_performance.select(*[c.name for c in ALERT_TRAINING_SCHEMA_LIST]))
# model_performance \
#     .select(*[c.name for c in ALERT_TRAINING_SCHEMA_LIST]) \
#     .withColumn('DATE_MODEL_TRAINED', pyf.current_timestamp()) \
#     .withColumn('TRAINING_ID', pyf.lit(RUN_ID)) \
#     .write \
#     .mode('append') \
#     .format('parquet') \
#     .save(PATH_RESULTS_OUTPUT)

# COMMAND ----------

model_runs_meta = spark.read.parquet(PATH_RESULTS_OUTPUT) \
    .filter('TRAINING_ID == "{run_id}"'.format(run_id=RUN_ID))

# COMMAND ----------

print('Trained Model Count: {:,}'.format(model_runs_meta.count()))
print('Output Model Count: {:,}'.format(model_runs_meta.filter('MODEL_ID is not null').count()))

# COMMAND ----------

df = model_runs_meta.filter('MODEL_ID is not null') \
    .select('METRICS', 'ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID') \
    .withColumn('R2_train', pyf.col('METRICS.r2_train')) \
    .withColumn('R2_test', pyf.col('METRICS.r2_test')) \
    .toPandas()

graph.title('Training Performance: {:,} Models'.format(len(df)))
sns.distplot(df['R2_train'], color='seagreen')
graph.xlabel('$R^2$')
display(graph.show())
graph.close()

# COMMAND ----------

graph.title('Testing Performance: {:,} Models'.format(len(df)))
sns.distplot(df['R2_test'])
graph.xlabel('$R^2$')
display(graph.show())
graph.close()

# COMMAND ----------

display(
    model_runs_meta.select(
        'TRAINING_ID', 'ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID', 'MODEL_ID', 'MODEL_METADATA',
        pyf.size(pyf.col('COLUMN_NAMES')).alias('COLUMN_COUNT'),
        'TEST_NON_ZERO_DAYS', 'TEST_DAYS_COUNT', 'METRICS'
    )
)
