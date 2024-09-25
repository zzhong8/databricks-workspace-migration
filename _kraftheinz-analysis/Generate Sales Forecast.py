# Databricks notebook source
# MAGIC %md
# MAGIC # Inference Driver

# COMMAND ----------

import pyspark.sql.types as pyt
import pyspark.sql.functions as pyf
from pyspark.sql.types import *
import numpy as np
import pandas as pd
import datetime

import pickle

# Possible Models
from sklearn.linear_model import LinearRegression, LassoCV, RidgeCV, BayesianRidge, Ridge, ElasticNet, Lasso
from catboost import CatBoostRegressor
from xgboost import XGBRegressor

# Use grid search for each of the models and use either 5 or 10 fold CV
# (if grid search is taking too long then use fewer params and/or fewer param values)
from sklearn.model_selection import GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

# Acosta.Alerting package imports
from acosta.alerting.preprocessing import pos_to_training_data, read_pos_data
from acosta.alerting.forecast import distributed_predict, prediction_schema
from acosta.alerting.helpers.features import get_lag_column_name
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
RETAILER = dbutils.widgets.get('RETAILER_PARAM').strip().lower()
CLIENT = dbutils.widgets.get('CLIENT_PARAM').strip().lower()
COUNTRY_CODE = dbutils.widgets.get('COUNTRY_CODE_PARAM').strip().lower()

if len(RETAILER) == 0 or len(CLIENT) == 0 or len(COUNTRY_CODE) == 0:
    raise ValueError('Client, Retailer and Country Code must be filled in.')

LAST_PREDICTED_DATE = datetime.datetime.strptime(dbutils.widgets.get('LAST_DAY_TO_PREDICT_PARAM'), '%Y%m%d')
DAYS_TO_PREDICT = int(dbutils.widgets.get('NUM_DAYS_TO_PREDICT_PARAM'))
MINIMUM_DAYS_TO_LAG = int(dbutils.widgets.get('MAX_LAG'))
ENVIRONMENT = dbutils.widgets.get('ENVIRONMENT').upper()
MODEL_SOURCE = dbutils.widgets.get('MODEL_SOURCE').upper()

ENVIRONMENT = 'PROD' if ENVIRONMENT.startswith('PROD') else 'DEV'
MODEL_SOURCE = 'LOCAL' if MODEL_SOURCE.startswith('LOCAL') else 'PROD'

input_discount_string = dbutils.widgets.get('INCLUDE_DISCOUNT_FEATURES').strip().lower()
INCLUDE_DISCOUNT_FEATURES = 'y' in input_discount_string or 't' in input_discount_string

# Constants TODO make this useless
DATE_FIELD = 'SALES_DT'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and preprocess data

# COMMAND ----------

champions_path = '/mnt{mod}/artifacts/country_code/champion_models/retailer={retailer}/client={client}/country_code={country_code}'.format(
    mod='' if MODEL_SOURCE == 'LOCAL' else '/prod-ro',
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
# MAGIC ##### Check to see if the cross-join table exists

# COMMAND ----------

date_reference_path = '/mnt{mod}/artifacts/country_code/reference/store_item_dates/retailer={retailer}/client={client}/country_code={country_code}/'.format(
    mod='' if MODEL_SOURCE == 'LOCAL' else '/prod-ro',
    retailer=RETAILER,
    client=CLIENT,
    country_code=COUNTRY_CODE
)
print(date_reference_path)

cross_join_table_exists_flag = check_path_exists(date_reference_path, 'delta', 'ignore')
if cross_join_table_exists_flag is False:
    # Note: we would like there to be a way for the notebook to notify us (i.e. the Advanced Analytics team) if this happens since it should not happen
    print('Cross-join Table not found for client')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Cross-join table Exists, so load it

# COMMAND ----------

# Retrieve the store_item_dates table from the 'reference folder' in the storage account
if cross_join_table_exists_flag:
    df_store_item_dates = spark.read.format('delta').load(date_reference_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Check to see if the cross-join table encompasses the dates that we are using for our forecast

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
# MAGIC ##### Cross-join table encompasses the dates that we are using for our forecast, so use it

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

# TODO: Read from post-processed Delta table
current_time = datetime.datetime.now()  # TODO NOTE Why is this line here?

# TODO rename this variable
processed_v2 = pos_to_training_data(
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

processed_v2 = processed_v2 \
    .withColumn('RETAILER', pyf.lit(RETAILER)) \
    .withColumn('CLIENT', pyf.lit(CLIENT)) \
    .withColumn('COUNTRY_CODE', pyf.lit(COUNTRY_CODE))

# Add all the correct log transformations now!
columns_to_log = ['POS_ITEM_QTY', 'RECENT_ON_HAND_INVENTORY_QTY', 'RECENT_ON_HAND_INVENTORY_DIFF'] \
                 + ['WEEK_SEASONALITY', 'YEAR_SEASONALITY'] \
                 + [col for col in processed_v2.columns if 'LAG_UNITS' in col]

for column_name in columns_to_log:
    if column_name not in processed_v2.columns:
        continue  # Skip this column

    processed_v2 = processed_v2.withColumn(
        column_name,
        pyf.signum(column_name) * pyf.log(pyf.abs(pyf.col(column_name)) + pyf.lit(1))
    )
    print(column_name, 'done')

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
            universal_encoder(df, True)
#             universal_encoder(df)
        ]],
        columns=compress_pos_data_cols
    )

# Try this function out
df_compressed = processed_v2.groupby('RETAILER_ITEM_ID').apply(compress_pos_data)

# COMMAND ----------

df_distributed_input = df_compressed.join(champions, ['RETAILER_ITEM_ID'], 'inner')
print('{:,}'.format(df_distributed_input.cache().count()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inference

# COMMAND ----------

# display(df_distributed_input)

# COMMAND ----------

df_distributed_input.count()

# COMMAND ----------

df_distributed_input.columns

# COMMAND ----------

df_distributed_input_temp = df_distributed_input.drop('MODEL_OBJECT')

# COMMAND ----------

display(df_distributed_input_temp)

# COMMAND ----------

import warnings
import inspect
from functools import wraps


deprecated_please_use_this_func_string = 'Please use {}() instead!'


def check_path_exists(path, file_format, errors='raise'):
    """
    Gets a path to a file with the file format and return boolean
    :param path: a path to the file
    :param file_format: format of the file
    :param errors: must be either 'raise' or 'ignore'
    :return: boolean
    """
    from pyspark.sql.utils import AnalysisException
    from pyspark.sql.session import SparkSession

    spark = SparkSession.builder.getOrCreate()

    try:
        _ = spark.read.format(file_format).load(path)
        does_exist = True
    except AnalysisException:
        does_exist = False

    if 'ra' in errors.lower() and does_exist is False:
        raise ValueError('The `{}` does not exist'.format(path))

    return does_exist


def check_continuous_lag_units(lags, errors='raise'):
    """
    Checks whether all lag units in a dataframe is continuous or not.

    :param lags: List of a data frame columns' names
    :param errors: Whether to raise an error or just return a bool
    :return:
    """
    import numpy as np

    lags = np.array([int(s.replace('LAG_UNITS_', '')) for s in lags])
    lags.sort()

    lag_diff = np.diff(lags)
    is_continuous = np.all(lag_diff == 1)

    if 'ra' in errors and is_continuous:
        raise AssertionError('LAG_UNITS are missing columns! Check existing lag unit columns!')

    return is_continuous


class Deprecated(object):
    def __init__(self, reason):
        if inspect.isclass(reason) or inspect.isfunction(reason):
            reason = 'Do not use this!'
        self.reason = reason

    def __call__(self, cls_or_func):
        if inspect.isfunction(cls_or_func):
            if hasattr(cls_or_func, 'func_code'):
                _code = cls_or_func.func_code
            else:
                _code = cls_or_func.__code__
            fmt = '{name} is a deprecated function! ({reason}).'
            filename = _code.co_filename
            line_number = _code.co_firstlineno + 1

        elif inspect.isclass(cls_or_func):
            fmt = '{name} is a deprecated class! ({reason}).'
            filename = cls_or_func.__module__
            line_number = 1

        else:
            raise TypeError(type(cls_or_func))

        message = fmt.format(name=cls_or_func.__name__, reason=self.reason)

        @wraps(cls_or_func)
        def new_func(*args, **kwargs):
            # Ensure that the warning is seen
            warnings.simplefilter('always', DeprecationWarning)
            warnings.warn_explicit(message, category=DeprecationWarning, filename=filename, lineno=line_number)
            warnings.simplefilter('default', DeprecationWarning)
            return cls_or_func(*args, **kwargs)

        return new_func


# COMMAND ----------

from sklearn.model_selection import GridSearchCV


class TrainingUnit:
    """
    A abstract of model fit that runs GridSearchCV for hyper parameter optimisation.
    """

    def __init__(self, algorithm, param_grid=None, cv=5):
        self.algorithm = algorithm
        self.cv = cv

        if param_grid is None:
            self.param_grid = None
            
        elif isinstance(param_grid, dict):
            if len(param_grid) > 0:
                self.param_grid = param_grid
            else:
                self.param_grid = None
        else:
            raise AttributeError("param_grid must be a dictionary")

    def train(self, X_train, y_train):
        if self.param_grid:
            # Do Grid Search
            grid_parameterized = GridSearchCV(
                self.algorithm,
                self.param_grid,
                scoring="neg_mean_squared_error",
                n_jobs=1,
                cv=self.cv
            )
            trained_model = grid_parameterized.fit(X_train, y_train)
        else:
            # Model without 
            trained_model = self.algorithm.fit(X_train, y_train)

        return trained_model


# COMMAND ----------

import numpy as np
from sklearn.metrics import mean_squared_error, make_scorer
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.linear_model import Lasso
# from errors import Deprecated, deprecated_please_use_this_func_string


# noinspection PyPep8Naming
class PrioritizedFeatureSelection:
    def __init__(self, priorities_list, model=Lasso, param_grid=None):
        self.best_level_ = len(priorities_list) - 1
        self.best_score_ = np.inf
        self.priorities = priorities_list
        self.sub_model_ = Lasso
        self.grid_model = None

        if isinstance(model(), Lasso) and param_grid is None:
            self.param_grid = {'alpha': np.logspace(-3, 3, 10)}
        elif param_grid is not None:
            self.param_grid = param_grid
        else:
            self.param_grid = {}

    @staticmethod
    def _check_is_not_numpy(array):
        if isinstance(array, np.ndarray):
            raise ValueError('X cannot be a numpy array')

    def subset_by_priority(self, level=None):
        from itertools import chain
        if level is None:
            level = len(self.priorities)
        assert 0 <= level <= len(self.priorities), 'Invalid priority level'

        columns_names = list(chain.from_iterable(self.priorities[:level]))
        return columns_names

    def fit(self, X, y, cv=5, scoring=make_scorer(mean_squared_error), greater_is_better=False):
        from sklearn.model_selection import GridSearchCV
        self._check_is_not_numpy(X)

        for level in range(1, len(self.priorities)+1):
            col_subset = self.subset_by_priority(level)
            self.grid_model = GridSearchCV(
                self.sub_model_(),
                cv=cv,
                param_grid=self.param_grid,
                scoring=scoring
            )
            self.grid_model.fit(X.loc[:, col_subset], y)

            if greater_is_better:
                if self.grid_model.best_score_ > self.best_score_:
                    self.best_score_, self.best_level_ = self.grid_model.best_score_, level
            else:
                if self.grid_model.best_score_ < self.best_score_:
                    self.best_score_, self.best_level_ = self.grid_model.best_score_, level

        return self

    def predict(self, X):
        self._check_is_not_numpy(X)
        return self.grid_model.predict(X.loc[:, self.subset_by_priority(self.best_level_)])

    def predict_proba(self, X):
        self._check_is_not_numpy(X)
        return self.grid_model.predict_proba(X.loc[:, self.subset_by_priority(self.best_level_)])


@Deprecated(deprecated_please_use_this_func_string.format(PrioritizedFeatureSelection.__name__))
class RecipeFile:
    def __init__(self, priorities_list):
        self.priority = {}
        for i in range(len(priorities_list)):
            self.priority[i] = priorities_list[i]

    def cume_priority(self, depth=None):
        from itertools import chain
        print('!!Print stop using `RecipeFile` ASAP!!')

        depth = len(self.priority) - 1 if depth is None else depth
        priorities = []
        for i in range(depth + 1):
            priorities.extend(self.priority[i])

        # Flatten lists
        priorities = list(chain.from_iterable(priorities))
        return priorities

    def select_feature_set(self, X, y, depth, test_size=0.33, random_state=None):
        best_depth = len(self.priority) - 1
        best_score = 9999.9
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=random_state)

        for i in range(len(self.priority)):
            subset_of_cols = self.cume_priority(i)
            m = Lasso()
            g = GridSearchCV(m, {"alpha": np.logspace(-3, -1.5, 7)})

            fitted = g.fit(X_train.loc[:, subset_of_cols], y_train)

            pred = fitted.predict(X_test.loc[:, subset_of_cols])
            score = mean_squared_error(y_true=y_test, y_pred=pred)

            if score < best_score:
                best_score = score
                best_depth = i

        return self.cume_priority(best_depth)


# COMMAND ----------

# from .trainingunit import TrainingUnit
# from .recipefile import RecipeFile, PrioritizedFeatureSelection
# from .errors import *


def universal_encoder(obj, compress=False):
    import zlib
    import base64
    import pickle
    obj = pickle.dumps(obj)
    if compress:
        obj = zlib.compress(obj, zlib.Z_BEST_COMPRESSION)
    return base64.b64encode(obj).decode()
#     return base64.b64encode(obj)


def universal_decoder(string, decompress=False):
    import zlib
    import base64
    import pickle
    obj = base64.b64decode(string)
    if decompress:
        obj = zlib.decompress(obj)
    return pickle.loads(obj)


# COMMAND ----------

# TODO (6/5/2019) move all contents to a new top level file, distributed.py
from time import time

import numpy as np
import pandas as pd
import pyspark.sql.functions as pyf
import pyspark.sql.types as pyt
from acosta.alerting.helpers import universal_decoder
from acosta.alerting.helpers.features import get_lag_column_name
from catboost import CatBoostRegressor


prediction_schema = pyt.StructType([
    pyt.StructField('CLIENT', pyt.StringType()),
    pyt.StructField('RETAILER', pyt.StringType()),
    pyt.StructField('COUNTRY_CODE', pyt.StringType()),
    pyt.StructField('MODEL_ID', pyt.StringType()),
    pyt.StructField('RETAILER_ITEM_ID', pyt.StringType()),
    pyt.StructField('ORGANIZATION_UNIT_NUM', pyt.IntegerType()),
    pyt.StructField('PREDICTIONS', pyt.FloatType()),
    pyt.StructField('SALES_DT', pyt.DateType())
])
prediction_schema_col_names = [col.name for col in prediction_schema]


def process_store_dataframe(df_store: pd.DataFrame, model: CatBoostRegressor, x_names: list):
    """
    Runs inference on a particular store to ensure simulating lagging logic occurs correctly

    :param df_store:
    :param model:
    :param x_names:
    :return:
    """
    is_recent_on_hand_inventory_diff_used = 'RECENT_ON_HAND_INVENTORY_DIFF' in df_store.columns

    possible_lag_unit_col_names = get_lag_column_name(range(1, 31))
    is_lag_units_used = any([(col in possible_lag_unit_col_names) for col in df_store.columns])

    # Sort the data. This is required to ensure temporal coherence of our predictions
    df_store = df_store.sort_values(by='SALES_DT').reset_index(drop=True).copy()

    # TODO (6/10/2019) check if all lags between [1, 30] are present in df_store

    # Call .predict() one row at a time
    predictions = []
    for i_row, _ in df_store.iterrows():
        prediction_i = model.predict(df_store.loc[i_row, x_names].values.reshape(1, -1))[0]
        predictions.append(prediction_i)

        # Update the DataFrame with the new data
        if i_row != len(df_store)-1:
            # Update recent on hand inventory difference assuming the predicted sales if the inventory loss
            # TODO (6/8/2019) you could have a model that predicts this number instead?
            if is_recent_on_hand_inventory_diff_used:
                # Check if need to update values for the next row
                df_store.loc[i_row+1, 'RECENT_ON_HAND_INVENTORY_DIFF'] = -prediction_i

            if is_lag_units_used:
                # Simulate lagging
                lags = df_store.loc[i_row, possible_lag_unit_col_names].values.copy()  # type: np.ndarray
                lags = np.roll(lags, shift=1)
                lags[0] = prediction_i
                df_store.loc[i_row+1, possible_lag_unit_col_names] = lags.copy()

    # Construct and return the prediction results.
    result_df_i = pd.concat([df_store['SALES_DT'], pd.Series(predictions, name='PREDICTIONS')], axis=1)
    result_df_i['ORGANIZATION_UNIT_NUM'] = df_store['ORGANIZATION_UNIT_NUM'].unique()[0]
    return result_df_i  # This only contains [SALES_DT, PREDICTIONS, ORGANIZATION_UNIT_NUM]


@pyf.pandas_udf(prediction_schema, pyf.PandasUDFType.GROUPED_MAP)
def distributed_predict(payload):
    print('N rows received {:,}'.format(len(payload)))
    payload = payload.loc[0, :]  # type: pd.Series

    # Unpacking
    model = universal_decoder(payload['MODEL_OBJECT'])
    df = universal_decoder(payload['DATAFRAME_PICKLE'], True)  # type: pd.DataFrame
#     df = universal_decoder(payload['DATAFRAME_PICKLE'])  # type: pd.DataFrame
    x_names = payload['COLUMN_NAMES'].split(',') # type: list[str]

    # Run predict in the special way we need to for lag features to work correctly
    # This mean we need to run this foreach organization unit num
    df_results = []  # type: list[pd.DataFrame]
    for store_id, df_store in df.groupby('ORGANIZATION_UNIT_NUM'):
        tic = time()
        df_results.append(process_store_dataframe(df_store, model, x_names))
        toc = time()
        print('Inference @ Organization Unit Num = {} -> {:.2f}s'.format(store_id, toc-tic))
    df_results = pd.concat(df_results)  # type: pd.DataFrame
    df_results['RETAILER_ITEM_ID'] = payload['RETAILER_ITEM_ID']
    df_results['RETAILER'] = payload['RETAILER']
    df_results['CLIENT'] = payload['CLIENT']
    df_results['COUNTRY_CODE'] = payload['COUNTRY_CODE']
    df_results['MODEL_ID'] = payload['MODEL_ID']
    print('=== Done ===\n')

    # Guarantee column order and return the dataframe
    return df_results[prediction_schema_col_names]


# COMMAND ----------

df_predictions = df_distributed_input.groupby('RETAILER_ITEM_ID').apply(distributed_predict)

# COMMAND ----------

# df_predictions.count()

# COMMAND ----------

print('{:,}'.format(df_predictions.cache().count()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process and export results

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

# # LOOK UP HASH KEYS
# databaseName = '{}_{}_{}_dv'.format(RETAILER.lower(), CLIENT.lower(), COUNTRY_CODE.lower())
# itemMasterTableName = '{}.hub_retailer_item'.format(databaseName)
# storeMasterTableName = '{}.hub_organization_unit'.format(databaseName)

# prediction_results_dataframe = prediction_results_dataframe.alias('PRDF') \
#     .join(sqlContext.read.table(storeMasterTableName).alias('OUH'),
#           pyf.col('OUH.ORGANIZATION_UNIT_NUM') == pyf.col('PRDF.ORGANIZATION_UNIT_NUM'), 'inner') \
#     .join(sqlContext.read.table(itemMasterTableName).alias('RIH'),
#           pyf.col('RIH.RETAILER_ITEM_ID') == pyf.col('PRDF.RETAILER_ITEM_ID'), 'inner') \
#     .select('PRDF.*', 'OUH.HUB_ORGANIZATION_UNIT_HK', 'RIH.HUB_RETAILER_ITEM_HK')

# COMMAND ----------

prediction_results_dataframe.columns

# COMMAND ----------

display(prediction_results_dataframe)

# COMMAND ----------

# PATH_RESULTS_OUTPUT = '/mnt/artifacts/hugh/kraftheinz-morrisons-forecast'

# prediction_results_dataframe.coalesce(1)\
#     .write.format('com.databricks.spark.csv')\
#     .option('header', 'true')\
#     .mode('overwrite')\
#     .save(PATH_RESULTS_OUTPUT)

# COMMAND ----------

# # Insert into table rather than blob
# # The Hive table definition will determine format, partitioning, and location of the data file...
# # so we don't have to worry about those low-level details
# insertDatabaseName = databaseName
# if ENVIRONMENT == 'DEV':
#     insertDatabaseName = 'RETAIL_FORECAST_ENGINE'
# elif ENVIRONMENT == 'PROD':
#     insertDatabaseName = '{}_{}_{}_retail_alert_im'.format(RETAILER.lower(), CLIENT.lower(), COUNTRY_CODE.lower())

# insertTableName = '{}.DRFE_FORECAST_BASELINE_UNIT'.format(insertDatabaseName)

# COMMAND ----------

# prediction_results_dataframe \
#     .select('HUB_ORGANIZATION_UNIT_HK', 'HUB_RETAILER_ITEM_HK', 'LOAD_TS', 'RECORD_SOURCE_CD', 'BASELINE_POS_ITEM_QTY',
#             'MODEL_ID', 'SALES_DT') \
#     .write.mode('overwrite').insertInto(insertTableName, overwrite=True)
