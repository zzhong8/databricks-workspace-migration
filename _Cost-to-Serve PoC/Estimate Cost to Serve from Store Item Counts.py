# Databricks notebook source
import datetime
import warnings
import mlflow
from dateutil.relativedelta import relativedelta

import pyspark.sql.functions as pyf
import pyspark.sql.types as pyt

import pyspark.mllib.stat as stat

# from pyspark.mllib.stat.Statistics import corr 
# import pyspark.mllib.stat.Statistics.corr

from pyspark.sql.window import Window

# COMMAND ----------

# Interface
# dbutils.widgets.text('RETAILER', 'walmart', 'Number of Stores')
# dbutils.widgets.text('RETAILER', 'walmart', 'Number of Items')
# dbutils.widgets.text('RETAILER', 'walmart', 'Retailer')

dbutils.widgets.dropdown('Retailer', 'walmart', ['asda', 'boots', 'kroger', 'meijer', 'morrisons', 'sainsburys', 'target', 'tesco', 'walmart'])
dbutils.widgets.dropdown('Country', 'us', ['us', 'ca', 'uk'])
dbutils.widgets.dropdown('Team', 'drt', ['drt', 'syn'])

dbutils.widgets.text('Num_Stores', '')
dbutils.widgets.text('Num_Items', '')

# Process input parameters
retailer = dbutils.widgets.get('Retailer').strip().casefold()
country = dbutils.widgets.get('Country').strip().casefold()
team = dbutils.widgets.get('Team').strip().casefold()

num_stores_P3M = int(dbutils.widgets.get('Num_Stores').strip().casefold())
num_items_P3M = int(dbutils.widgets.get('Num_Items').strip().casefold())
num_stores_items_P3M = num_stores_P3M * num_items_P3M

# COMMAND ----------

import_path = '/mnt/artifacts/reference/cost_to_serve_poc.csv'

# Load tables with business rules and join it to the main table
df_cost_to_serve = spark.read.format('csv') \
    .options(header='true', inferSchema='true') \
    .load(import_path)

display(df_cost_to_serve)

# COMMAND ----------

df_cost_to_serve_filtered = df_cost_to_serve.where(pyf.col('num_stores_P3M') > 0)
df_cost_to_serve_filtered = df_cost_to_serve.where(pyf.col('total_num_alerts') > 0)

display(df_cost_to_serve_filtered)

# COMMAND ----------

# Get average number of stores by retailer
df_cost_to_serve_filtered_agg = (
    df_cost_to_serve_filtered.groupBy(["country", "retailer"])
    .agg(pyf.avg("num_stores_P3M").alias("average_num_stores"))
    .orderBy(["country", "retailer"], descending=True)
)

display(df_cost_to_serve_filtered_agg)

# COMMAND ----------

# Let's do some scatterplots
df_cost_by_store_item_counts = df_cost_to_serve_filtered.select("num_stores_P3M", "num_items_P3M", "total_cost")
df_cost_by_store_counts = df_cost_to_serve_filtered.select("num_stores_P3M", "total_cost")
df_cost_by_item_counts = df_cost_to_serve_filtered.select("num_items_P3M", "total_cost")

df_cost_by_store_x_item_counts = df_cost_by_store_item_counts
df_cost_by_store_x_item_counts = df_cost_by_store_x_item_counts.withColumn("num_stores_items_P3M", pyf.col('num_stores_P3M') * pyf.col('num_items_P3M'))
df_cost_by_store_x_item_counts = df_cost_by_store_x_item_counts.select("num_stores_items_P3M", "total_cost")

df_cost_by_log_store_x_item_counts = df_cost_by_store_x_item_counts
df_cost_by_log_store_x_item_counts = df_cost_by_log_store_x_item_counts.withColumn("log_num_stores_items_P3M", pyf.log(pyf.col('num_stores_items_P3M')))
df_cost_by_log_store_x_item_counts = df_cost_by_log_store_x_item_counts.select("log_num_stores_items_P3M", "total_cost")

# COMMAND ----------

display(df_cost_by_store_counts)

# COMMAND ----------

display(df_cost_by_item_counts)

# COMMAND ----------

display(df_cost_by_store_x_item_counts)

# COMMAND ----------

df_cost_by_store_x_item_counts.stat.corr("num_stores_items_P3M", "total_cost")

# COMMAND ----------

display(df_cost_by_log_store_x_item_counts)

# COMMAND ----------

df_cost_by_log_store_x_item_counts.stat.corr("log_num_stores_items_P3M", "total_cost")

# COMMAND ----------

# Let's build a model
df_cost_by_store_variables = df_cost_to_serve_filtered.select("retailer", "country", "team", "num_stores_P3M", "num_items_P3M", "total_cost")

display(df_cost_by_store_variables)

# COMMAND ----------

# Import libraries for building a Gradient Boosted Tree Regression Model
# We will follow the example in this link: https://docs.databricks.com/en/_extras/notebooks/source/gbt-regression.html

from pyspark.ml import Pipeline
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, VectorIndexer

from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator

# COMMAND ----------

# StringIndexer for each categorical column
retailer_indexer = StringIndexer(inputCol="retailer", outputCol="retailer_index")
country_indexer = StringIndexer(inputCol="country", outputCol="country_index")
team_indexer = StringIndexer(inputCol="team", outputCol="team_index")

# OneHotEncoderEstimator
encoder = OneHotEncoder(inputCols=["retailer_index", "country_index", "team_index"],
                        outputCols=["retailer_vec", "country_vec", "team_vec"])

# Create a pipeline
pipeline = Pipeline(stages=[retailer_indexer, country_indexer, team_indexer, encoder])

# Fit the pipeline and transform the DataFrame
df_cost_by_store_variables_transformed = pipeline.fit(df_cost_by_store_variables).transform(df_cost_by_store_variables)

cols = ["retailer_index", "country_index", "team_index", "retailer_vec", "country_vec", "team_vec", "num_stores_P3M", "num_items_P3M", "total_cost"]

df_cost_by_store_variables_transformed = df_cost_by_store_variables_transformed.select(cols)

# Show the transformed DataFrame
display(df_cost_by_store_variables_transformed)

# COMMAND ----------

# Split the dataset randomly into 70% for training and 30% for testing. Passing a seed for deterministic behavior
train, test = df_cost_by_store_variables_transformed.randomSplit([0.8, 0.2], seed = 0)
print("There are %d training examples and %d test examples." % (train.count(), test.count()))

# COMMAND ----------

# The first step is to create the VectorAssembler and VectorIndexer steps.

# Remove the target column from the input feature set.
featuresCols = df_cost_by_store_variables_transformed.columns
featuresCols.remove('total_cost')
 
# vectorAssembler combines all feature columns into a single feature vector column, "rawFeatures".
vectorAssembler = VectorAssembler(inputCols=featuresCols, outputCol="rawFeatures")

# vectorIndexer identifies categorical features and indexes them, and creates a new column "features". 
vectorIndexer = VectorIndexer(inputCol="rawFeatures", outputCol="features", maxCategories=9, handleInvalid="keep")

# COMMAND ----------

# Next, define the model

# The next step is to define the model training stage of the pipeline. 
# The following command defines a GBTRegressor model that takes an input column "features" by default and learns to predict the values in the "total_cost" column. 
gbt = GBTRegressor(labelCol="total_cost")

# COMMAND ----------

# Define a grid of hyperparameters to test:
#  - maxDepth: maximum depth of each decision tree 
#  - maxIter: iterations, or the total number of trees 
paramGrid = ParamGridBuilder()\
  .addGrid(gbt.maxDepth, [2, 5])\
  .addGrid(gbt.maxIter, [10, 100])\
  .build()
 
# Define an evaluation metric.  The CrossValidator compares the true labels with predicted values for each combination of parameters, and calculates this value to determine the best model.
evaluator = RegressionEvaluator(metricName="rmse", labelCol=gbt.getLabelCol(), predictionCol=gbt.getPredictionCol())
 
# Declare the CrossValidator, which performs the model tuning.
cv = CrossValidator(estimator=gbt, evaluator=evaluator, estimatorParamMaps=paramGrid)

# COMMAND ----------

# Create the pipeline.
pipeline = Pipeline(stages=[vectorAssembler, vectorIndexer, cv])

# COMMAND ----------

# Train the pipeline
pipelineModel = pipeline.fit(train)

# COMMAND ----------

# Make predictions and evaluate results
predictions = pipelineModel.transform(test)

display(predictions.select("total_cost", "prediction", *featuresCols))

# COMMAND ----------

# A common way to evaluate the performance of a regression model is the calculate the root mean squared error (RMSE). The value is not very informative on its own, but you can use it to compare different models. CrossValidator determines the best model by selecting the one that minimizes RMSE.

rmse = evaluator.evaluate(predictions)
print("RMSE on our test set: %g" % rmse)

# COMMAND ----------

predictions_with_residuals = predictions.withColumn("residual", (pyf.col("total_cost") - pyf.col("prediction")))

display(predictions_with_residuals.agg({'residual': 'mean'}))

# COMMAND ----------

display(predictions_with_residuals.select("retailer_index", "residual"))

# COMMAND ----------

display(predictions_with_residuals.select("country_index", "residual"))

# COMMAND ----------

test_data = [{"retailer": retailer, "country": country, "team": team, "num_stores_P3M": num_stores_P3M, "num_items_P3M": num_items_P3M, "num_stores_items_P3M": num_stores_items_P3M}]

test_df = spark.createDataFrame(test_data).select("retailer", "country", "team", "num_stores_P3M", "num_items_P3M", "num_stores_items_P3M")

display(test_df)

# COMMAND ----------

# Manually assign numeric indices
test_df_transformed = test_df

test_df_transformed = test_df_transformed.withColumn("retailer_index",
    pyf.when(test_df_transformed["retailer"] == "asda", 1)
    .when(test_df_transformed["retailer"] == "morrisons", 2)
    .when(test_df_transformed["retailer"] == "tesco", 3)
    .when(test_df_transformed["retailer"] == "sainsburys", 4)
    .when(test_df_transformed["retailer"] == "kroger", 5)
    .when(test_df_transformed["retailer"] == "target", 6)
    .when(test_df_transformed["retailer"] == "boots", 7)
    .when(test_df_transformed["retailer"] == "meijer", 8)
    .when(test_df_transformed["retailer"] == "walmart", 0)
    .otherwise(None)  # Handle other cases (if any)
)

test_df_transformed = test_df_transformed.withColumn("country_index",
    pyf.when(test_df_transformed["country"] == "us", 1)
    .when(test_df_transformed["country"] == "ca", 2)
    .when(test_df_transformed["country"] == "uk", 0)
    .otherwise(None)  # Handle other cases (if any)
)

test_df_transformed = test_df_transformed.withColumn("team_index",
    pyf.when(test_df_transformed["team"] == "syn", 1)
    .when(test_df_transformed["team"] == "drt", 0)
    .otherwise(None)  # Handle other cases (if any)
)

test_cols = ["retailer_index", "country_index", "team_index", "num_stores_P3M", "num_items_P3M"]

test_df_transformed = test_df_transformed.select(test_cols)

# Show the transformed DataFrame
display(test_df_transformed)

# COMMAND ----------

# Make predictions and evaluate results
test_predictions = pipelineModel.transform(test_df_transformed)

display(test_predictions.select("prediction", *featuresCols))

# COMMAND ----------

model_uri = f"runs:/886e4de176894a709449c5c11498952f/model"

# model = mlflow.pyfunc.load_model(model_uri=model_uri)
# model.predict(input_X)

# Prepare the test dataset
predict_udf = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri)

display(test_df.withColumn("total_costs_predicted", predict_udf()))

# COMMAND ----------

# # Import only recent data (currently set to most recent 6 months)
# today_date = datetime.date.today()

# ###################### TEMP CODE ######################
# today_date = datetime.datetime(2023, 12, 8, 0, 0)
# ###################### TEMP CODE ######################

# min_date = today_date - relativedelta(months=3)
# max_date = today_date - relativedelta(days=1)

# min_date_filter = min_date.strftime(format="%Y-%m-%d")
# max_date_filter = max_date.strftime(format="%Y-%m-%d")

# print(min_date_filter)
# print(max_date_filter)

# COMMAND ----------


