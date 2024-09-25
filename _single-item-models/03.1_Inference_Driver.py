# Databricks notebook source
# MAGIC %md
# MAGIC # Prediction Engine

# COMMAND ----------

from pyspark.sql import functions as pyf
from pyspark.sql.types import *
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
from acosta.alerting.forecast import distributed_predict, ALERT_PREDICTION_SCHEMA_LIST
from acosta.alerting.helpers.features import get_lag_column_name
from acosta.alerting.helpers import check_path_exists

import acosta

print(acosta.__version__)

# COMMAND ----------

# sqlContext.setConf("spark.sql.shuffle.partitions", "800")
# sqlContext.setConf("spark.default.parallelism", "800")

# COMMAND ----------

# spark.conf.set("hive.exec.dynamic.partition", "true")
# spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
# spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

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
# TODO: Check if the path exists after the format has changed to delta
champions_path = '/mnt{mod}/artifacts/champion_models_v2/'.format(mod='' if MODEL_SOURCE == 'LOCAL' else '/prod-ro')
champions = spark.read.parquet(champions_path) \
    .filter("RETAILER == '{retailer}' and CLIENT == '{client}'".format(retailer=RETAILER, client=CLIENT))\
    .drop("RETAILER", "CLIENT")

# COMMAND ----------

print('{:,}'.format(champions.count()))

# COMMAND ----------

# Read POS data
sales_data_subset = read_pos_data(RETAILER, CLIENT, sqlContext)#.repartition('SALES_DT')

# COMMAND ----------

# Replace negative POS_ITEM_QTY and POS_AMT values with 0
sales_data_subset = sales_data_subset.withColumn("POS_ITEM_QTY", pyf.when(pyf.col("POS_ITEM_QTY") >= 0, pyf.col("POS_ITEM_QTY")).otherwise(0))
sales_data_subset = sales_data_subset.withColumn("POS_AMT", pyf.when(pyf.col("POS_AMT") >= 0, pyf.col("POS_AMT")).otherwise(0))

print('{:,}'.format(sales_data_subset.count()))

# COMMAND ----------

LAST_N_DAYS_FILTER = "SALES_DT >= '{startDateHyphen}' and SALES_DT < '{endDateHyphen}'".format(
    startDateHyphen=(LAST_PREDICTED_DATE - datetime.timedelta(MINIMUM_DAYS_TO_LAG + DAYS_TO_PREDICT)).strftime(
        '%Y-%m-%d'
    ),
    endDateHyphen=(LAST_PREDICTED_DATE - datetime.timedelta(DAYS_TO_PREDICT)).strftime("%Y-%m-%d")
)

print(LAST_N_DAYS_FILTER)
last_n_days = sales_data_subset.filter(LAST_N_DAYS_FILTER)

# Get distinct values and combine them
unique_org_item = last_n_days.select('ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID').distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Check to see if the cross-join table exists

# COMMAND ----------

date_reference_path = '/mnt{mod}/artifacts/reference/store_item_dates/retailer={retailer}/client={client}/'\
    .format(mod='' if MODEL_SOURCE == 'LOCAL' else '/prod-ro',
            retailer=RETAILER,
            client=CLIENT
           )
print(date_reference_path)

# Check if the date_reference_path exists.
# TODO: replace with one-liner
if check_path_exists(date_reference_path, 'delta', 'ignore'):
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
    df_store_item_dates = spark.read.format("delta").load(date_reference_path)

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

if cross_join_table_exists_flag:
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
print('{:,}'.format(processed_v2.count()))

# COMMAND ----------

models_assigned = processed_v2.join(champions, ["RETAILER_ITEM_ID"], "inner").filter(
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
    
print('{:,}'.format(models_assigned.count()))

# COMMAND ----------

# Must be in alphabetical order due to the way unpacking dictionaries work
pred_schema = StructType(fields=ALERT_PREDICTION_SCHEMA_LIST)
print(pred_schema)

# COMMAND ----------

print(models_assigned.columns)

# COMMAND ----------

display(models_assigned.groupby('RETAILER_ITEM_ID').count())

# COMMAND ----------

# Repartition if you need to
prediction_results_dataframe = models_assigned.groupby('RETAILER_ITEM_ID')
# prediction_results_dataframe = models_assigned.rdd \
#     .map(lambda x: ((x["ORGANIZATION_UNIT_NUM"], x["RETAILER_ITEM_ID"]), [x.asDict()])) \
#     .reduceByKey(lambda accum, r: accum + r) \
#     .mapValues(lambda x: distributed_predict(pd.DataFrame(x))) \
#     .flatMap(lambda x: x[1]) \
#     .toDF(schema=pred_schema)

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

prediction_results_dataframe \
    .select("HUB_ORGANIZATION_UNIT_HK", "HUB_RETAILER_ITEM_HK", "LOAD_TS", "RECORD_SOURCE_CD", "BASELINE_POS_ITEM_QTY",
            "MODEL_ID", "SALES_DT") \
    .write.mode("overwrite").insertInto(insertTableName, overwrite = True)

# COMMAND ----------

# display(prediction_results_dataframe)
