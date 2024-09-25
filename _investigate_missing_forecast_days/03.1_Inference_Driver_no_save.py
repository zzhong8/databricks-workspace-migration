# Databricks notebook source
# MAGIC %md
# MAGIC # Prediction Engine

# COMMAND ----------

from pyspark.sql import functions as pyf
from pyspark.sql.types import *
import pandas as pd
import datetime

import pickle

# 1 Linear Regression
# 4 Lasso Regression
# 5 Ridge Regression
# 6 ElasticNet
from sklearn.linear_model import LinearRegression, LassoCV, RidgeCV, BayesianRidge, Ridge, ElasticNet, Lasso

# 3 Poisson regression
# import statsmodels.api as sm

# 7 Decision Tree
# 8 Random Forest
from sklearn.ensemble import RandomForestRegressor

# 9 XGBoost
from xgboost import XGBRegressor

# 10 Regression Splines
# from pyearth import Earth

# Use grid search for each of the models and use either 5 or 10 fold CV
# (if grid search is taking too long then use fewer params and/or fewer param values)
from sklearn.model_selection import GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
# Acosta.Alerting package imports
from acosta.alerting.preprocessing import pos_to_training_data, read_pos_data
from acosta.alerting.forecast import distributed_predict, ALERT_PREDICTION_SCHEMA_LIST
from acosta.alerting.helpers.features import get_lag_column_name

import acosta

print(acosta.__version__)

# COMMAND ----------

sqlContext.setConf("spark.sql.shuffle.partitions", "800")
# sqlContext.setConf("spark.default.parallelism", "800")

# COMMAND ----------

spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

# COMMAND ----------

# Loading all variables and constants
dbutils.widgets.text("RETAILER_PARAM", "WALMART", "Retailer")
dbutils.widgets.text("CLIENT_PARAM", "", "Client")
dbutils.widgets.text("LAST_DAY_TO_PREDICT_PARAM", "", "Last Day to Predict (YYYYMMDD)")
dbutils.widgets.text("NUM_DAYS_TO_PREDICT_PARAM", "7", "Number of Days to Predict")
dbutils.widgets.text("MAX_LAG", "30", "Maximum Days to Lag")
dbutils.widgets.dropdown("ENVIRONMENT", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.dropdown("MODEL_SOURCE", "local", ["local", "prod"], "Model Source")
dbutils.widgets.dropdown("INCLUDE_DISCOUNT_FEATURES", "Yes", ["Yes", "No"], "Include Discount Features")

# COMMAND ----------

# Parameters
RETAILER = dbutils.widgets.get("RETAILER_PARAM").upper()
CLIENT = dbutils.widgets.get("CLIENT_PARAM").upper()

if len(RETAILER) == 0 or len(CLIENT) == 0:
    raise ValueError("Client and Retailer must be filled in.")

LAST_PREDICTED_DATE = datetime.datetime.strptime(dbutils.widgets.get("LAST_DAY_TO_PREDICT_PARAM"), "%Y%m%d")
DAYS_TO_PREDICT = int(dbutils.widgets.get("NUM_DAYS_TO_PREDICT_PARAM"))
MINIMUM_DAYS_TO_LAG = int(dbutils.widgets.get("MAX_LAG"))
ENVIRONMENT = dbutils.widgets.get("ENVIRONMENT").upper()
MODEL_SOURCE = dbutils.widgets.get('MODEL_SOURCE').upper()

ENVIRONMENT = 'PROD' if ENVIRONMENT.startswith('PROD') else 'DEV'
MODEL_SOURCE = 'LOCAL' if MODEL_SOURCE.startswith('LOCAL') else 'PROD'

input_discount_string = dbutils.widgets.get("INCLUDE_DISCOUNT_FEATURES").strip().lower()
INCLUDE_DISCOUNT_FEATURES = 'y' in input_discount_string or 't' in input_discount_string

# Constants
DATE_FIELD = "SALES_DT"

# COMMAND ----------

# TODO: Check whether this is as fast as reading directly from the partitioned files
champions_path = '/mnt{mod}/artifacts/champion_models/'.format(mod='' if MODEL_SOURCE == 'LOCAL' else '/prod-ro')
champions = spark.read.parquet(champions_path) \
    .filter("RETAILER == '{retailer}' and CLIENT == '{client}'".format(retailer=RETAILER, client=CLIENT))\
    .drop("RETAILER", "CLIENT")

# COMMAND ----------

# Load in the non-TSV files from the pre-processed data location
sales_data_subset = read_pos_data(RETAILER, CLIENT, sqlContext).filter("POS_ITEM_QTY > 0")

LAST_N_DAYS_FILTER = "SALES_DT >= '{startDateHyphen}' and SALES_DT < '{endDateHyphen}'".format(
    startDateHyphen=(LAST_PREDICTED_DATE - datetime.timedelta(MINIMUM_DAYS_TO_LAG + DAYS_TO_PREDICT)).strftime(
        '%Y-%m-%d'
    ),
    endDateHyphen=(LAST_PREDICTED_DATE - datetime.timedelta(DAYS_TO_PREDICT)).strftime("%Y-%m-%d")
)

print(LAST_N_DAYS_FILTER)
last_n_days = sales_data_subset.filter(LAST_N_DAYS_FILTER)

# COMMAND ----------

# Dole retailer_item_id = 9237492, organization_unit_num = 860
sales_data_subset = sales_data_subset.filter("RETAILER_ITEM_ID == '9237492' AND ORGANIZATION_UNIT_NUM == 860")

# COMMAND ----------

display(sales_data_subset)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Check to see if the cross-join table exists

# COMMAND ----------

from pyspark.sql.utils import AnalysisException

def path_exists(path):
    try:
        x = spark.read.format("delta").load(path)
        return True
      
    except AnalysisException as e:
        return False

# COMMAND ----------

date_reference_path = '/mnt{mod}/artifacts/reference/store_item_dates/retailer={retailer}/client={client}/'

print(date_reference_path.format(mod='' if MODEL_SOURCE == 'LOCAL' else '/prod-ro', retailer=RETAILER, client=CLIENT))

# If the approved product list exists for this client
if path_exists(date_reference_path.format(mod='' if MODEL_SOURCE == 'LOCAL' else '/prod-ro', retailer=RETAILER, client=CLIENT)): 
    cross_join_table_exists_flag = True
    
else:
    cross_join_table_exists_flag = False
    
    # Note: we would like there to be a way for the notebook to notify us (i.e. the Advanced Analytics team) if this happens since it should not happen
    print("Cross-join Table not found for client")

# COMMAND ----------

# %fs
# mounts

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Cross-join table Exists, so load it

# COMMAND ----------

# Retrieve the store_item_dates table from the 'reference folder' in the storage account
if cross_join_table_exists_flag:
    df_store_item_dates = spark.read.format("delta").load(date_reference_path.format(mod='' if MODEL_SOURCE == 'LOCAL' else '/prod-ro', retailer=RETAILER, client=CLIENT))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Check to see if the cross-join table encompasses the dates that we are using for our forecast

# COMMAND ----------

if cross_join_table_exists_flag:
    df_store_item_date_min_max = df_store_item_dates.select(pyf.min("SALES_DT"), pyf.max("SALES_DT")).collect()

    df_store_item_date_min = datetime.datetime.combine(df_store_item_date_min_max[0][0], datetime.datetime.min.time())
    df_store_item_date_max = datetime.datetime.combine(df_store_item_date_min_max[0][1], datetime.datetime.min.time())

# COMMAND ----------

# Dates that we care about
if cross_join_table_exists_flag:
    LAST_PREDICTED_DATE # Directly From Widget
    FIRST_PREDICTED_DATE = LAST_PREDICTED_DATE - datetime.timedelta(DAYS_TO_PREDICT - 1)
    FIRST_DAY_OF_LAGS = LAST_PREDICTED_DATE - datetime.timedelta(MINIMUM_DAYS_TO_LAG + DAYS_TO_PREDICT - 1)
    
    # If the first day of lags is earlier than the earliest date that we have in the cross-join table
    if FIRST_DAY_OF_LAGS < df_store_item_date_min:
        cross_join_table_exists_flag = False
          
        print("The first day of lags is earlier than the earliest date that we have in the cross-join table")

    # If the last day to predict is later than the latest date that we have in the cross-join table
    elif LAST_PREDICTED_DATE > df_store_item_date_max:
        cross_join_table_exists_flag = False
          
        print("The last day to predict is later than the latest date that we have in the cross-join table")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Cross-join table encompasses the dates that we are using for our forecast, so use it

# COMMAND ----------

if cross_join_table_exists_flag:
    df_store_item_last_n_days_filter = " (SALES_DT >= '{startDateHyphen}' and SALES_DT <= '{endDateHyphen}')".format(
        startDateHyphen=FIRST_DAY_OF_LAGS.strftime('%Y-%m-%d'),
        endDateHyphen=(LAST_PREDICTED_DATE).strftime("%Y-%m-%d")
    )

    print(df_store_item_last_n_days_filter)

# COMMAND ----------

if cross_join_table_exists_flag:
    df_store_item_last_n_days = df_store_item_dates.filter(df_store_item_last_n_days_filter)\
        .select(["SALES_DT", "RETAILER_ITEM_ID", "ORGANIZATION_UNIT_NUM"])\
        .withColumn("IS_PREDICTION_ROW", pyf.col("SALES_DT") >= FIRST_PREDICTED_DATE.strftime("%Y-%m-%d"))

    print(df_store_item_last_n_days.explain())

# COMMAND ----------

display(df_store_item_last_n_days)

# COMMAND ----------

if cross_join_table_exists_flag:
  
    # Get distinct values and combine them
    unique_org_item = last_n_days.select("ORGANIZATION_UNIT_NUM", "RETAILER_ITEM_ID").distinct()

    # Get all combinations of the orgs + items and the dates selected
    df_store_item_last_n_days = df_store_item_last_n_days.join(unique_org_item, ["ORGANIZATION_UNIT_NUM", "RETAILER_ITEM_ID"])

    # Add columns from the loaded sales data
    input_dataset = df_store_item_last_n_days.join(
        last_n_days,
        ["SALES_DT", "ORGANIZATION_UNIT_NUM", "RETAILER_ITEM_ID"],
        "left"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Cross-join table either does not exist or does not encompass the dates required, so create it on the fly

# COMMAND ----------

if not cross_join_table_exists_flag:
  
    # Setup the list of days needed to collect and flag those within the days to predict range
    days_to_collect = [(LAST_PREDICTED_DATE - datetime.timedelta(days=i), False)
                       for i in range(0, MINIMUM_DAYS_TO_LAG + DAYS_TO_PREDICT)]
    
    days_flagged = [[day.date(), day > (LAST_PREDICTED_DATE - datetime.timedelta(days=DAYS_TO_PREDICT))] 
                    for day, _ in days_to_collect]
    
    dates_and_predict_flag = sc.parallelize(days_flagged).toDF(["SALES_DT", "IS_PREDICTION_ROW"])

# COMMAND ----------

if not cross_join_table_exists_flag:
  
    # Get all combinations of the orgs + items and the dates selected
    all_rows_of_loaded_and_dates = dates_and_predict_flag.crossJoin(unique_org_item)
    
    input_dataset = all_rows_of_loaded_and_dates.join(
        last_n_days,
        ["SALES_DT", "ORGANIZATION_UNIT_NUM", "RETAILER_ITEM_ID"],
        "left"
    )

# COMMAND ----------

input_dataset.printSchema()

# COMMAND ----------

# missing days:
# - dole 2019-03-02 t0 2019-03-08
# - Campbellssnack 2019-03-09 to 2019-03-15

# and the Minutemaid one has only one day forecast. which means you can pick any days you like

# COMMAND ----------

# Dole retailer_item_id = 9237492, organization_unit_num = 860
input_dataset = input_dataset.filter("RETAILER_ITEM_ID == '9237492' AND ORGANIZATION_UNIT_NUM == 860")

# COMMAND ----------

# Campbellssnack retailer_item_id = 553042010, organization_unit_num = 2569
input_dataset = input_dataset.filter("RETAILER_ITEM_ID == '553042010' AND ORGANIZATION_UNIT_NUM == 2569") 

# COMMAND ----------

# Minutemaid retailer_item_id = 552183147, organization_unit_num = 3482
input_dataset = input_dataset.filter("RETAILER_ITEM_ID == '552183147' AND ORGANIZATION_UNIT_NUM == 3482") 

# COMMAND ----------

display(input_dataset)

# COMMAND ----------

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

    # This should be moved into a Dataframe rather than RDD to increase the chance of pipelining
    explode_dates = min_max_date.rdd \
        .map(lambda r: (r.ORGANIZATION_UNIT_NUM, r.RETAILER_ITEM_ID, _list_days(r.startDate, r.endDate))) \
        .toDF(['ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID', 'dayList']) \
        .select('ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID', pyf.explode(pyf.col('dayList')).alias(date_field))

    return explode_dates.join(df, [date_field] + join_fields_list, 'left')

# COMMAND ----------

def pos_to_training_data(df, retailer, client, spark, spark_context, include_discount_features=False, item_store_cols=None):
    """
    Takes in a POS DataFrame and:
    - Explodes data to include all days between min and max of original dataframe.
    - Computes Price and a lag/leading price.
    - Joins global tables:
      - holidays_usa
    - Relies on a global DATE_FIELD being defined.
  
    :param DataFrame df: A POS fact table containing <item_store_cols>, POS_ITEM_QTY, POS_AMTS
      and the field defined in a constant <DATE_FIELD>.
    :param string retailer: A string containing a valid RETAILER.
    :param string client: A string containing a valid CLIENT for the given RETAILER.
    :param SparkSession spark: The current spark session.
    :param SparkContext spark_context: In databricks you get this for free in the `sc` object
    :param boolean include_discount_features: Whether or not the advanced discount features should be
        calculated.  Setting to true provides
    :param list(string) item_store_cols: The list of column names that represent the RETAILER_ITEM_ID
      and stores and are used in joining to NARS datasets (Note: deprecated).
  
    :return DataFrame:
    """
    # HIGH LEVEL COMMENT: The idea of setting the variables to None was done with the hope
    #   of keeping the driver memory low.  However, since that is no longer necessary,
    #   consider overwriting the variables for cleanliness or stick with the unique
    #   variables to keep it scala-esque.

    # Necessary to handle the Pricing logic
    # @Stephen Rose for more details
    df = cast_decimal_to_number(df, cast_to='float')

    df010 = _all_possible_days(df=df, date_field='SALES_DT', join_fields_list=['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'])

    # Fill in the zeros for On hand inventory quantity
    # Could be merged with the _replace_negative_and_null_with
    # Should verify that the optimizer is not combining this already
    df110 = df010.withColumn(
        'ON_HAND_INVENTORY_QTY',
        pyf.when(pyf.col('ON_HAND_INVENTORY_QTY') < 0, pyf.lit(0)).otherwise(
            pyf.col('ON_HAND_INVENTORY_QTY'))
        )

    df010 = None
    df020 = _replace_negative_and_null_with(df=df110, cols=['POS_ITEM_QTY', 'POS_AMT'], replacement_constant=0)

    # Very important to get all of the window functions using this partitioning to be done at the same time
    # This will minimize the amount of shuffling being done and allow for pipelining
    window_item_store = Window.partitionBy(['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM']).orderBy('SALES_DT')

    # Lag for 12 weeks to match the logic from the LOESS baseline model
    # TODO: Increase lag to a year once we have more data
    # TODO: replace with for loop of the acosta_features.get_lag_days()
    lag_units = list(range(1, 31)) + [35, 42, 49, 56, 63, 70, 77, 84]
    for lag in lag_units:

        # 2019-03-19: We are not currently using lag units, so this code may be removed in the future
        df020 = df020.withColumn(
            'LAG_UNITS_{}'.format(lag),
            pyf.lag(pyf.col('POS_ITEM_QTY'), lag).over(window_item_store)
        )
        if lag <= 14:

            # Lag inventory units are created to generate the recent_on_hand_inventory_diff feature
            df020 = df020.withColumn(
                'LAG_INV_{}'.format(lag),
                pyf.lag(pyf.col('POS_ITEM_QTY'), lag).over(window_item_store) # This should actually be 'ON_HAND_INVENTORY_QTY'
            )

    # Include the lags for pricing
    # This note is still very much important to avoid a second shuffle
    # NOTE: Consider moving this out of a function and embedding into pos_to_training_data
    # NOTE: (continued) the DAG shows two window functions being used instead of one.
    df020 = compute_price_and_lag_lead_price(df020)

    # Use the Lag_INV columns to generate RECENT_ON_HAND_INVENTORY_QTY
    # The Lag_INV columns are dropped after this command and are no longer referenced
    df020 = df020.withColumn('RECENT_ON_HAND_INVENTORY_QTY', pyf.coalesce(
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
            )).drop(*['LAG_INV_{}'.format(i) for i in range(1, 15)])
    
    # The RECENT_ON_HAND_INVENTORY_DIFF is the prior day end's inventory minus two days ago
    # RECENT_ON_HAND_INVENTORY_QTY is at least the prior day's ending inventory
    df020 = df020.withColumn('RECENT_ON_HAND_INVENTORY_DIFF', pyf.col('RECENT_ON_HAND_INVENTORY_QTY') \
                            - pyf.lag(pyf.col('RECENT_ON_HAND_INVENTORY_QTY'), 1).over(window_item_store))

    df110 = None
    loaded_df01 = None
    
    # Reference data that contains Holidays, Days of Week, and Days of Month
    master_dates_table = spark.sql("SELECT * FROM retail_forecast_engine.country_dates WHERE COUNTRY = 'US'")

    # NOTE!  The Country/Date dimension contains some columns we're not yet ready to use in our training process.
    # This includes the new "RELATIVE" class of columns
    # So let's filter out those columns
    master_dates_table = master_dates_table.drop(*["COUNTRY", "DAY_OF_WEEK"])
    master_dates_table = master_dates_table.drop(*filter(lambda x: x.endswith("_RELATIVE"), master_dates_table.schema.names))

    # This is all @Stephen
    # Add Seasonality Columns
    master_dates_table = master_dates_table.withColumn('dow', pyf.dayofweek('SALES_DT'))
    master_dates_table = master_dates_table.withColumn('doy', pyf.dayofyear('SALES_DT'))

    week_df = convert_dict_to_spark_df(week_seasonality_spark_map, ['dow', 'WEEK_SEASONALITY'], spark_context)
    year_df = convert_dict_to_spark_df(year_seasonality_map, ['doy', 'YEAR_SEASONALITY'], spark_context)

    master_dates_table = master_dates_table.join(week_df, week_df['dow'] == master_dates_table['dow'])
    master_dates_table = master_dates_table.drop('dow')

    master_dates_table = master_dates_table.join(year_df, year_df['doy'] == master_dates_table['doy'])
    master_dates_table = master_dates_table.drop('doy')

    # Add dynamic intercepts
    days_of_year = np.arange(366) + 1
    dynamic_intercepts = pd.DataFrame(days_of_year, columns=['doy'])
    for month_i in range(12):
        dynamic_intercepts['TIME_OF_YEAR_{}'.format(month_i)] = repeated_rbf(
            dynamic_intercepts['doy'].values,
            center=15 + (month_i * 30),
            period=days_of_year.max(),
            std=15
        )

    dynamic_intercepts = spark.createDataFrame(dynamic_intercepts)
    master_dates_table = master_dates_table.withColumn('doy', pyf.dayofyear('SALES_DT'))
    master_dates_table = master_dates_table.join(
        dynamic_intercepts, dynamic_intercepts['doy'] == master_dates_table['doy']
    )
    master_dates_table = master_dates_table.drop('doy')

    # Add weekly amplitude features
    master_dates_table = master_dates_table.withColumn('doy', pyf.dayofyear('SALES_DT'))
    result_df = master_dates_table.toPandas()
    amplitude = ['WEEK_AMPLITUDE_{}'.format(i) for i in range(15)]
    for i, week_i in enumerate(amplitude):
        result_df[week_i] = repeated_rbf(
            result_df['doy'].values,
            i * (366 / len(amplitude)),
            period=366,
            std=10
        )
        result_df[week_i] = result_df[week_i] * result_df['WEEK_SEASONALITY']
    master_dates_table = spark.createDataFrame(result_df)

    # Load reference data for Snap and Paycycles
    # Joins to the data set based on day of month and organization_unit_num (store #)
    snap_pay_cycle = spark.read.parquet(
        '/mnt/prod-ro/artifacts/reference/snap_paycycle/retailer={retailer}/client={client}/'.format(
            retailer=retailer.lower(),
            client=client.lower()
        )
    ).withColumnRenamed("DAYOFMONTH", "DAY_OF_MONTH")

    # Join to the list of holidays, day_of_week, and day_of_month
    # This join is needed before joining snap_paycycle due to the presence of day_of_month
    df_with_dates = df020.join(master_dates_table, ["SALES_DT"], "inner")

    df_with_snap = df_with_dates \
        .join(snap_pay_cycle, ['ORGANIZATION_UNIT_NUM', 'DAY_OF_MONTH'], 'inner') \
        .drop('STATE')
    
    df_penultimate = df_with_snap
    df_with_dates = None
    master_dates_table = None
    snap_pay_cycle = None
    loaded_df02 = None

    if include_discount_features:
        df_out = compute_reg_price_and_discount_features(df_penultimate, threshold=0.025, max_discount_run=49)
    else:
        df_out = df_penultimate

    df_penultimate = None
    
    return df_out

# COMMAND ----------

# TODO: Read from post-processed Delta table
current_time = datetime.datetime.now()
processed_v2 = pos_to_training_data(
    df=input_dataset,
    retailer=RETAILER,
    client=CLIENT,
    spark=spark,
    spark_context=sc,
    include_discount_features = INCLUDE_DISCOUNT_FEATURES,
    item_store_cols=['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM']
).filter("IS_PREDICTION_ROW == True")

# COMMAND ----------

models_assigned = processed_v2.join(champions, ["ORGANIZATION_UNIT_NUM", "RETAILER_ITEM_ID"], "inner").filter(
    "IS_PREDICTION_ROW == True"
)

# Log the columns to be consistent with training
# TODO: Add this into the recipe file
columns_to_log = ['POS_ITEM_QTY', 'RECENT_ON_HAND_INVENTORY_QTY', 'RECENT_ON_HAND_INVENTORY_DIFF']
columns_to_log += get_lag_column_name()

for column_name in columns_to_log:
    models_assigned = models_assigned.withColumn(
        column_name,
        pyf.signum(column_name) * pyf.log(pyf.abs(pyf.col(column_name)) + pyf.lit(1))
    )
    print(column_name, "done")

# COMMAND ----------

# Must be in alphabetical order due to the way unpacking dictionaries work
pred_schema = StructType(fields=ALERT_PREDICTION_SCHEMA_LIST)

# COMMAND ----------

# Repartition if you need to

prediction_results_dataframe = models_assigned.rdd \
    .map(lambda x: ((x["ORGANIZATION_UNIT_NUM"], x["RETAILER_ITEM_ID"]), [x.asDict()])) \
    .reduceByKey(lambda accum, r: accum + r) \
    .mapValues(lambda x: distributed_predict(pd.DataFrame(x))) \
    .flatMap(lambda x: x[1]) \
    .toDF(schema=pred_schema)

# COMMAND ----------

# rename columns and add additional metadata to the predictions
prediction_results_dataframe = prediction_results_dataframe.selectExpr(
    "ORGANIZATION_UNIT_NUM",
    "RETAILER_ITEM_ID",
    "CURRENT_TIMESTAMP() as LOAD_TS",
    "'Dynamic.Retail.Forecast.Engine' as RECORD_SOURCE_CD",
    "exp(PREDICTIONS)-1 as BASELINE_POS_ITEM_QTY",
    "SALES_DT",
    "MODEL_ID"
)

# LOOK UP HASH KEYS
databaseName = "{}_{}_dv".format(RETAILER.lower(), CLIENT.lower())
itemMasterTableName = "{}.hub_retailer_item".format(databaseName)
storeMasterTableName = "{}.hub_organization_unit".format(databaseName)

prediction_results_dataframe = prediction_results_dataframe.alias("PRDF") \
    .join(sqlContext.read.table(storeMasterTableName).alias("OUH"),
          pyf.col("OUH.ORGANIZATION_UNIT_NUM") == pyf.col("PRDF.ORGANIZATION_UNIT_NUM"), "inner") \
    .join(sqlContext.read.table(itemMasterTableName).alias("RIH"),
          pyf.col("RIH.RETAILER_ITEM_ID") == pyf.col("PRDF.RETAILER_ITEM_ID"), "inner") \
    .select("PRDF.*", "OUH.HUB_ORGANIZATION_UNIT_HK", "RIH.HUB_RETAILER_ITEM_HK")

# COMMAND ----------

# Insert into table rather than blob
# The Hive table definition will determine format, partitioning, and location of the data file...
# so we don't have to worry about those low-level details

insertDatabaseName = databaseName
if ENVIRONMENT == "DEV":
    insertDatabaseName = "RETAIL_FORECAST_ENGINE"

elif ENVIRONMENT == "PROD":
    insertDatabaseName = "{}_{}_retail_alert_im".format(RETAILER.lower(), CLIENT.lower())

insertTableName = "{}.DRFE_FORECAST_BASELINE_UNIT".format(insertDatabaseName)

# COMMAND ----------

# prediction_results_dataframe \
#     .select("HUB_ORGANIZATION_UNIT_HK", "HUB_RETAILER_ITEM_HK", "LOAD_TS", "RECORD_SOURCE_CD", "BASELINE_POS_ITEM_QTY",
#             "MODEL_ID", "SALES_DT") \
#     .write.mode("overwrite").insertInto(insertTableName, overwrite = True)

# COMMAND ----------

# display(prediction_results_dataframe)
