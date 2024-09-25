# Databricks notebook source
from pyspark.sql import functions as pyf
from pyspark.sql import types as pyt
# 1 Linear Regression
# 4 Lasso Regression
# 5 Ridge Regression
# 6 ElasticNet
from sklearn.linear_model import LinearRegression, Lasso, Ridge, ElasticNet

# 3 Poisson regression
#import statsmodels.api as sm

# 7 Decision Tree
# 8 Random Forest
#from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor

# 9 XGBoost
#from xgboost import XGBRegressor

# 10 Regression Splines
#from pyearth import Earth

# Use grid search for each of the models and use either 5 or 10 fold CV
# (if grid search is taking too long then use fewer params and/or fewer param values)
from sklearn.model_selection import GridSearchCV

from datetime import datetime as dtm
from datetime import timedelta

# Acosta.Alerting package imports
from acosta.alerting.algorithms import MASE
from acosta.alerting.training import distributed_training, ALERT_TRAINING_SCHEMA_LIST

import acosta
print(acosta.__version__)

# COMMAND ----------

dbutils.widgets.text("retailer","WALMART", "Retailer")
dbutils.widgets.text("client","CLOROX", "Client")
dbutils.widgets.text("store", "", "Organization Unit Num")
dbutils.widgets.text("item","", "Retailer Item ID")
dbutils.widgets.text("runid","", "Run ID")

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
 
# PATHS
PATH_RESULTS_OUTPUT = "/mnt/artifacts/training_results/retailer={retailer}/client={client}/".format(retailer=RETAILER, client=CLIENT)
print(RUN_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Training Results

# COMMAND ----------

training_results = spark.read.parquet("/mnt/artifacts/training_results/")\
    .filter("RETAILER == '{retailer}' and CLIENT == '{client}' and TRAINING_ID == '{run_id}'".format(
        retailer = RETAILER, client = CLIENT, run_id = RUN_ID
    ))

# COMMAND ----------

print("Trained Model Count: {:,}".format(training_results.count()))

print("Output Model Count: {:,}".format(training_results.filter("MODEL_ID is not null").count()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analysis Examples

# COMMAND ----------

# Summary of training results
from pyspark.ml.feature import QuantileDiscretizer

filtered_results = training_results.filter("MODEL_ID IS NOT NULL").withColumn("MASE_TEST", pyf.col("METRICS").mase_test)

discretizer = QuantileDiscretizer(numBuckets=10, inputCol="MASE_TEST", outputCol="decile")
bucketed_results = discretizer.fit(filtered_results).transform(filtered_results)

display(bucketed_results.groupBy("decile")\
        .agg(
          pyf.count("RETAILER_ITEM_ID").alias("records"),
          pyf.mean("TRAIN_NON_ZERO_DAYS"),
          pyf.mean("TRAIN_DAYS_COUNT"),
          pyf.mean("TEST_NON_ZERO_DAYS"),
          pyf.mean("TEST_DAYS_COUNT"),
          pyf.min("mase_test").alias("min_mase"),
          pyf.mean("mase_test").alias("mean_mase"),
          pyf.max("mase_test").alias("max_mase"),
          pyf.mean("METRICS.r2_train").alias("r2_train_mean"),
          pyf.mean("METRICS.r2_test").alias("r2_test_mean"),
          pyf.mean("METRICS.mse_train").alias("mse_train_mean"),
          pyf.mean("METRICS.mse_test").alias("mse_test_mean")
          )\
        .orderBy("decile")
       )

# COMMAND ----------

# Sort the models based on a metric
display(
  training_results\
  .select("TRAINING_ID","ORGANIZATION_UNIT_NUM","RETAILER_ITEM_ID","MODEL_ID",
          pyf.size(pyf.col("COLUMN_NAMES")).alias("COLUMN_COUNT"),
          "TEST_NON_ZERO_DAYS","TEST_DAYS_COUNT","METRICS")\
  .orderBy("METRICS.r2_test", ascending=False)  
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analysis of single store upc

# COMMAND ----------

SINGLE_ITEM = '562965810'
SINGLE_STORE = 2210

# COMMAND ----------

# TODO: Understand why bad dates are being created
display(training_results\
    .filter("MODEL_ID is not NULL")\
    .filter("RETAILER_ITEM_ID == '{}' AND ORGANIZATION_UNIT_NUM == {}".format(SINGLE_ITEM, SINGLE_STORE))\
    .select("DATASETS","DATES")\
    .withColumn("dates", pyf.col("DATES.dates_train") )\
    .withColumn("actual", pyf.col("DATASETS.train") )\
    .withColumn("pred", pyf.col("DATASETS.pred_train"))\
    .withColumn("zippeddata", pyf.arrays_zip("dates","actual", "pred"))\
    .withColumn("exploder", pyf.explode("zippeddata"))\
    .select(
      pyf.col("exploder.dates"), "exploder.actual", "exploder.pred", (pyf.col("exploder.actual") - pyf.col("exploder.pred")).alias("residuals") )\
      .orderBy("dates")
       )

# COMMAND ----------

# TODO: Understand why bad dates are being created
display(training_results\
    .filter("MODEL_ID is not NULL")\
    .filter("RETAILER_ITEM_ID == '{}' AND ORGANIZATION_UNIT_NUM == {}".format(SINGLE_ITEM, SINGLE_STORE))\
    .select("DATASETS","DATES")\
    .withColumn("dates", pyf.col("DATES.dates_test") )\
    .withColumn("actual", pyf.col("DATASETS.test") )\
    .withColumn("pred", pyf.col("DATASETS.pred_test"))\
    .withColumn("zippeddata", pyf.arrays_zip("dates","actual", "pred"))\
    .withColumn("exploder", pyf.explode("zippeddata"))\
    .select(
      pyf.col("exploder.dates"), "exploder.actual", "exploder.pred", (pyf.col("exploder.actual") - pyf.col("exploder.pred")).alias("residuals") )\
      .orderBy("dates")
       )

# COMMAND ----------


