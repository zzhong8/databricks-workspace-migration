# Databricks notebook source
import numpy as np
import pandas as pd
from pyspark.sql import functions as pyf
from pyspark.sql import types as pyt
import uuid

# 1 Linear Regression
# 4 Lasso Regression
# 5 Ridge Regression
# 6 ElasticNet
from sklearn.linear_model import LinearRegression, Lasso, Ridge, ElasticNet

# 3 Poisson regression
# import statsmodels.api as sm

# 7 Decision Tree
# 8 Random Forest
# from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor

# 9 XGBoost
from xgboost import XGBRegressor

# 10 Regression Splines
# from pyearth import Earth

# Use grid search for each of the models and use either 5 or 10 fold CV
# (if grid search is taking too long then use fewer params and/or fewer param values)
from sklearn.model_selection import GridSearchCV

from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.metrics import make_scorer

from sklearn.externals import joblib

from datetime import datetime as dtm
from datetime import timedelta

# Acosta.Alerting package imports
from acosta.alerting.helpers import TrainingUnit, PrioritizedFeatureSelection
from acosta.alerting.helpers.features import get_day_of_week_column_names, get_lag_column_name, \
    get_day_of_month_column_names
from acosta.alerting.training import distributed_training, ALERT_TRAINING_SCHEMA_LIST
from acosta.alerting.preprocessing import pos_to_training_data, read_pos_data

import acosta

print(acosta.__version__)

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime
import uuid

from pyspark.sql import types as pyt
from pyspark.sql import Row
from sklearn.metrics import r2_score, mean_squared_error

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

def distributed_training(df, test_size, y_col, training_unit_list, recipe, days_to_forecast=7):
    """
    :param days_to_forecast:
    :param pandas.DataFrame df: Required Field: SALES_DT, RETAILER, CLIENT, LAG_UNITS_1-7
    :param float test_size: The proportion of the training data to be marked as test set
    :param str y_col: Column inside of pandas dataframe representing the y value.
    :param list[TrainingUnit] training_unit_list: 
    :param sklearn.evaluator evaluator: Used to determine the best training unit outcome
    :param RecipeFile recipe: Contains the columns to be used as independent variables
    :param dict param_grid:
    :param int num_folds
    """  
    import pickle
    
    def train_test_backtest_split(X, y, test_size):
        """
        Splits the X and y data into training and testing set.  The test_size
            will split the X and y based on that percentage.  The bottom /
            later portion of the data will be considered the test set.
        :param numpy.array/pandas.DataFrame X: a multidim array or dataframe
            containing the independnet variables to be split.
        :param numpy.array/pandas.Series y: a one dimensional array or series
            containing the dependnet variable to be split.
        :param float test_size: The percent (between 0 and 1) of the data
            to be set for the test set.
        """
        rows = X.shape[0]
        rows_test = int(rows * test_size)
        rows_train = rows - rows_test
        
        x_train = X[:rows_train, :]
        x_test = X[rows_train:, :]
        y_train = y[:rows_train]
        y_test = y[rows_train:]
        
        return x_train, x_test, y_train, y_test
    
    # Make sure data frame is sorted
    df = pd.DataFrame(df).sort_values("SALES_DT")

    ITEM_ID = str(df.loc[0, "RETAILER_ITEM_ID"])
    STORE_ID = int(df.loc[0, "ORGANIZATION_UNIT_NUM"])

    # NOTE: This returns a Timestamp rather than a datetime in pandas 0.23 
    # but 0.19 (DBR installed) returns Datetime
    OBSERVED_DATE_RANGE_MIN = df["SALES_DT"].min()
    OBSERVED_DATE_RANGE_MAX = df["SALES_DT"].max()
    
    # Select only columns provided and drop SALES_DT
    independent_variables = recipe.subset_by_priority()

    df_without_nulls = df.loc[:, [y_col] + independent_variables].dropna(axis=0)

    # Get the dates involved after dropping NAs
    # Need to convert the series to datetime and then convert to UTC by
    # subtracting from 1970-01-01
    dates_used = [
        datetime.utcfromtimestamp(int(i)) for i in (
            (pd.to_datetime(df.SALES_DT[df_without_nulls.index]).values - np.datetime64('1970-01-01T00:00:00Z')) / np.timedelta64(1, 's')
            ).tolist()
        ]

    # Get only the independent variables in a dataframe
    df_independents = df_without_nulls.drop([y_col], axis=1)

    y = df_without_nulls.loc[:, y_col].astype("float64").values
    
    # Identify the important features to include based on the recipe file
    recipe.fit(df_independents, y)
    prioritized_columns = recipe.subset_by_priority(level=recipe.best_level_)

    # When moving between pyspark and pandas, some columns might be Decimal() type
    # (which has a dtype of object and that doesn't play well with numpy floats or 
    # ints, so force it to be float64
    for c in prioritized_columns:
        if df_independents[c].dtype == object:
            df_independents[c] = df_independents[c].astype("float64")
    
    # Select the final columns to be included in the model
    X = df_independents.loc[:, prioritized_columns].values
    # Split df to training and testing
    X_train, X_test, y_train, y_test = train_test_backtest_split(X, y, test_size=test_size)
    
    DATES_TRAIN = dates_used[:y_train.shape[0]]
    DATES_TEST = dates_used[y_train.shape[0]:]

    # Run grid search
    _best_score = 9999.9
    results_history = []
    _best_index = 0
    _best_model = None
    _best_dataset = {}

    # Loop over every training unit and update the _best_score, _best_index
    # if the evaluator score is less than the current _best_score
    for training_iteration, training_unit in enumerate(training_unit_list):
        
        # Convert to a 1d array for RandomForestRegressor
        grid_result = training_unit.train(X_train, y_train)

        train_predictions = [float(x) for x in list(grid_result.predict(X_train))]
        test_predictions = [float(x) for x in list(grid_result.predict(X_test))]
            
        # Output variables
        TEST_SET_MSE_PERFORMANCE = float(mean_squared_error(y_true = y_test, y_pred = test_predictions))
        TRAIN_NON_ZERO_DAYS = np.sum(y_train > 0)
        TRAIN_DAYS_COUNT = len(y_train)
        TEST_NON_ZERO_DAYS = np.sum(y_test > 0)
        TEST_DAYS_COUNT = len(y_test)

        # Append to results history
        # Need to explicitly cast these columns due to pyspark not casting numpy types
        results_history.append(
            Row(**{
                "ORGANIZATION_UNIT_NUM": STORE_ID,
                "RETAILER_ITEM_ID": ITEM_ID,
                "MODEL_METADATA": str(grid_result),
                "COLUMN_NAMES": prioritized_columns,
                "DATES": {},
                "DATASETS": {},
                "METRICS": {
                    "r2_train": float(r2_score(y_true = y_train, y_pred = train_predictions)),
                    "r2_test": float(r2_score(y_true = y_test, y_pred = test_predictions)),
                    "mse_train": float(mean_squared_error(y_true = y_train, y_pred = train_predictions)),
                    "mse_test": float(TEST_SET_MSE_PERFORMANCE)
                },
                "TRAIN_NON_ZERO_DAYS": int(TRAIN_NON_ZERO_DAYS),
                "TRAIN_DAYS_COUNT": int(TRAIN_DAYS_COUNT),
                "TEST_NON_ZERO_DAYS": int(TEST_NON_ZERO_DAYS),
                "TEST_DAYS_COUNT": int(TEST_DAYS_COUNT),
                "OBSERVED_DATE_RANGE_MIN": OBSERVED_DATE_RANGE_MIN,
                "OBSERVED_DATE_RANGE_MAX": OBSERVED_DATE_RANGE_MAX,
                "MODEL_ID": None,
                "MODEL_OBJECT": bytearray(pickle.dumps(None))
            })
        )   
        
        if TEST_SET_MSE_PERFORMANCE < _best_score or training_iteration == 0:
            _best_score = TEST_SET_MSE_PERFORMANCE
            _best_index = training_iteration
            _best_model = grid_result
            _best_dates = {
                "dates_train": DATES_TRAIN,
                "dates_test": DATES_TEST
            }
            _best_dataset = {
                "train": [float(x) for x in list(y_train)],
                "pred_train": train_predictions,
                "test": [float(x) for x in list(y_test)],
                "pred_test": test_predictions
            }

    # End Models loop
    # Write the best model to the mount point for artifacts

    # Add the model id name to the best iteration
    best_row_object_as_dict = results_history[_best_index].asDict()
    best_row_object_as_dict.update({
            "MODEL_ID": str(uuid.uuid4()),
            "MODEL_OBJECT": bytearray(pickle.dumps(_best_model)),
            "DATASETS": _best_dataset,
            "DATES": _best_dates
        })
    results_history[_best_index] = Row(**best_row_object_as_dict)
    
    # Clean up phase to encourage garbage collection on executors
    df = None
    _df_subset_columns = None
    df_independents = None
    X = None
    y = None
    X_train = None
    X_test = None
    y_train = None
    y_test = None
    grid_result = None
    mutated_x = None
    prediction_history = None
    temp_pred = None
    prediction_actuals_tuple_list = None
    test_set_sum_predictions = None
    test_set_sum_actuals = None
    training_set_window_agg = None
    prediction = None
    
    return results_history

# COMMAND ----------

sqlContext.setConf("spark.executor.cores", "8")

# COMMAND ----------

dbutils.widgets.text("retailer", "WALMART", "Retailer")
dbutils.widgets.text("client", "CLOROX", "Client")
dbutils.widgets.text("store", "", "Organization Unit Num")
dbutils.widgets.text("item", "", "Retailer Item ID")
dbutils.widgets.text("runid", "", "Run ID")

RETAILER = dbutils.widgets.get("retailer").strip().upper()
CLIENT = dbutils.widgets.get("client").strip().upper()
RUN_ID = dbutils.widgets.get("runid").strip()

try:
    STORE = int(dbutils.widgets.get("store").strip())
except ValueError:
    STORE = None
try:
    ITEM = str(dbutils.widgets.get("item").strip())
except ValueError:
    ITEM = None

if RETAILER == "":
    raise ValueError("\"retailer\" is a required parameter.  Please provide a value.")

if CLIENT == "":
    raise ValueError("\"client\" is a required parameter.  Please provide a value.")

if RUN_ID == "":
    RUN_ID = str(uuid.uuid4())

# PATHS
PATH_RESULTS_OUTPUT = "/mnt/artifacts/training_results/retailer={retailer}/client={client}/".format(retailer=RETAILER,
                                                                                                    client=CLIENT)
PATH_ENGINEERED_FEATURES_OUTPUT = "/mnt/processed/training/{run_id}/engineered/".format(run_id=RUN_ID)
print(RUN_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Setup

# COMMAND ----------

# Filtering of data set
loaded_data = spark.read.parquet(PATH_ENGINEERED_FEATURES_OUTPUT)

if STORE:
    loaded_data = loaded_data.filter("ORGANIZATION_UNIT_NUM == '{}'".format(STORE))
if ITEM:
    loaded_data = loaded_data.filter("RETAILER_ITEM_ID == '{}'".format(ITEM))

# This filter requires at least 84 days of non-zero sales in the entire datset
subset_meets_threshold = loaded_data.select("RETAILER", "CLIENT", "RETAILER_ITEM_ID", "ORGANIZATION_UNIT_NUM",
                                            "POS_ITEM_QTY") \
    .filter("POS_ITEM_QTY > 0") \
    .groupBy("RETAILER", "CLIENT", "RETAILER_ITEM_ID", "ORGANIZATION_UNIT_NUM") \
    .count() \
    .filter("count >= 84") \
    .drop("count")

data_over_threshold = loaded_data.join(subset_meets_threshold,
                                       ["RETAILER", "CLIENT", "RETAILER_ITEM_ID", "ORGANIZATION_UNIT_NUM"], "inner")

# COMMAND ----------

# Select columns to be included in dataframe

# Create 'Recipe' of columns to be added in priority order
list_of_cols = [
    ['RECENT_ON_HAND_INVENTORY_QTY', 'REG_PRICE', 'DISCOUNT_PERCENT', 'DAYS_DISCOUNTED', 'SNAPINDEX', 'NONSNAPINDEX'],
    get_day_of_week_column_names(),
    get_day_of_month_column_names(),
    [c for c in data_over_threshold.columns if c.startswith("HOLIDAY") and "_LAG_" not in c and "_LEAD_" not in c]
]

recipe = PrioritizedFeatureSelection(list_of_cols)
priority_list = recipe.subset_by_priority()

mandatory_fields = ["POS_ITEM_QTY", "SALES_DT", "RETAILER", "CLIENT", "ORGANIZATION_UNIT_NUM", "RETAILER_ITEM_ID"]
lags_to_include = get_lag_column_name(x=range(1, 8))

_select_columns = mandatory_fields + priority_list + lags_to_include

data_with_final_features = data_over_threshold.select(*_select_columns).dropna()

# Adding log log to non binary columns
# Note: series is un-log-transformed before performing calculations in mase.py in the package
#       POS_ITEM_QTY is un-log-transformed as BASELINE_POS_ITEM_QTY in 03.1 - Inference Driver
non_neg_columns_to_be_log_transformed = ["POS_ITEM_QTY", "RECENT_ON_HAND_INVENTORY_QTY"] \
                                        + ['REG_PRICE', 'DISCOUNT_PERCENT', 'DAYS_DISCOUNTED'] \
                                        + lags_to_include

for column_name in non_neg_columns_to_be_log_transformed:
    data_with_final_features = data_with_final_features.withColumn(column_name, pyf.signum(column_name) * pyf.log(
        pyf.abs(pyf.col(column_name)) + pyf.lit(1)))
    print(column_name + " done")

data_partitioned = data_with_final_features.repartition(800, "ORGANIZATION_UNIT_NUM", "RETAILER_ITEM_ID")

# COMMAND ----------

# Create instances of training units
default_algorithms_and_grids = [
    TrainingUnit(
        algorithm=Lasso(alpha=0.004),
        param_grid={}
    ),
#     TrainingUnit(
#         algorithm=Ridge(alpha=15),
#         param_grid={}
#     ),
#     TrainingUnit(
#         algorithm=ElasticNet(alpha=0.01),
#         param_grid={}
#     ),
    TrainingUnit(
        algorithm=RandomForestRegressor(),
        param_grid={}
    )#,
#     TrainingUnit(
#         algorithm=XGBRegressor(n_estimators=200, objective='reg:linear'),
#         param_grid={}
#     )
]

# COMMAND ----------

# # Create instances of training units
# default_algorithms_and_grids = [
#     TrainingUnit(
#         algorithm = Lasso(alpha=0.004),
#         param_grid = {"alpha": np.logspace(-3, -1.5, 7)}
#     ),
#     TrainingUnit(
#         algorithm = Ridge(alpha=15),
#         param_grid = {"alpha": np.logspace(0.5, 2, 7)}
#     ),
#     TrainingUnit(
#         algorithm = ElasticNet(alpha=0.01),
#         param_grid = {"alpha": np.logspace(-3, -1, 5), "l1_ratio": [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]}
#     ),
#     TrainingUnit(
#         algorithm = RandomForestRegressor(),
#         param_grid = {"max_depth": [3, 10, None], "min_samples_split": [2, 10], "min_samples_leaf": [1, 3, 10]}
#     ),
#     TrainingUnit(
#         algorithm = XGBRegressor(n_estimators=200, objective='reg:linear'),
#         param_grid = {"max_depth": [3, 10], "learning_rate": [0.003, 0.01, 0.03, 0.1], "n_estimators": [100, 200, 300]}
#     ),
#     TrainingUnit(
#         algorithm = XGBRegressor(n_estimators=200, objective='count:poisson'),
#         param_grid = {"max_depth": [3, 5], "learning_rate": [0.003, 0.01, 0.03, 0.1], "n_estimators": [100, 200, 300]}
#     )
# ]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Training

# COMMAND ----------

print("Beginning training for at {}".format(dtm.now().strftime("%Y%m%d:%H:%M")))

#  A basic overview of trained_models_rdd construction:
# .map: Change RDD of rows into K,V pairs of identifiers and features as dict (same row count)
# .reduceByKey: For common Key (store,UPC): append daily features as a list of dicts (fewer rows, 1 per store, item)
# .mapValues: Passing list of dictionaries (features) to a function that will be constructed into a Pandas DF
#     ^-- This now is doing work on a single node for an individual item, store.
#
# The end result is an rdd containing the _results_history list of dictionaries containing all training units' results and the identifier
# and model object for the best training unit.

trained_models_rdd = data_partitioned.rdd \
    .map(lambda r: ((r.ORGANIZATION_UNIT_NUM, r.RETAILER_ITEM_ID), [r.asDict()])) \
    .reduceByKey(lambda accum, r: accum + r) \
    .mapValues(lambda x: distributed_training(
        df=x,
        test_size=0.33,
        y_col="POS_ITEM_QTY",
        recipe=recipe,
        training_unit_list=default_algorithms_and_grids
    ))

# .flatMapValue is taking a 3D object - stack of training results, and making it a long list of individual runs with repeating key entries
# Having an extraction function will simplify the extraction of tuple elements + dictionary values into the results columns.
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
model_performance \
    .select(*[c.name for c in ALERT_TRAINING_SCHEMA_LIST]) \
    .withColumn("DATE_MODEL_TRAINED", pyf.current_timestamp()) \
    .withColumn("TRAINING_ID", pyf.lit(RUN_ID)) \
    .write \
    .mode("append") \
    .format("parquet") \
    .save(PATH_RESULTS_OUTPUT)

# COMMAND ----------

model_runs_meta = spark.read.parquet(PATH_RESULTS_OUTPUT) \
    .filter("TRAINING_ID == '{run_id}'".format(run_id=RUN_ID))

# COMMAND ----------

print("Trained Model Count: {:,}".format(model_runs_meta.count()))

print("Output Model Count: {:,}".format(model_runs_meta.filter("MODEL_ID is not null").count()))

# COMMAND ----------

display(
    model_runs_meta.select(
        "TRAINING_ID", "ORGANIZATION_UNIT_NUM", "RETAILER_ITEM_ID", "MODEL_ID", "MODEL_METADATA",
        pyf.size(pyf.col("COLUMN_NAMES")).alias("COLUMN_COUNT"),
        "TEST_NON_ZERO_DAYS", "TEST_DAYS_COUNT", "METRICS"
    )
)
