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
from sklearn.linear_model import LinearRegression, LassoCV, Ridge, ElasticNet, Lasso, BayesianRidge

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
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
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

# TODO: When writing out all the rows make sure the timestamp is consistent on them

# Setup the list of days needed to collect and flag those within the days to predict range
days_to_collect = [(LAST_PREDICTED_DATE - datetime.timedelta(days=i), False)
                   for i in range(0, MINIMUM_DAYS_TO_LAG + DAYS_TO_PREDICT)]

days_flagged = [[day.date(), day > (LAST_PREDICTED_DATE - datetime.timedelta(days=DAYS_TO_PREDICT))]
                for day, _ in days_to_collect]

dates_and_predict_flag = sc.parallelize(days_flagged).toDF(["SALES_DT", "IS_PREDICTION_ROW"])

# Get distinct values and combine them
unique_org_item = last_n_days.select("ORGANIZATION_UNIT_NUM", "RETAILER_ITEM_ID").distinct()

# Get all combinations of the orgs + items and the dates selected
all_rows_of_loaded_and_dates = dates_and_predict_flag.crossJoin(unique_org_item)

# Add columns from the loaded sales data
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

prediction_results_dataframe \
    .select("HUB_ORGANIZATION_UNIT_HK", "HUB_RETAILER_ITEM_HK", "LOAD_TS", "RECORD_SOURCE_CD", "BASELINE_POS_ITEM_QTY",
            "MODEL_ID", "SALES_DT") \
    .write.mode("overwrite").insertInto(insertTableName, overwrite = True)

# COMMAND ----------

display(prediction_results_dataframe)
