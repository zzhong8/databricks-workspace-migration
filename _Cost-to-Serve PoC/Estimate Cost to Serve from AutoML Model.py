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

test_data = [{"retailer": retailer, "country": country, "team": team, "num_stores_P3M": num_stores_P3M, "num_items_P3M": num_items_P3M, "num_stores_items_P3M": num_stores_items_P3M}]

test_df = spark.createDataFrame(test_data).select("retailer", "country", "team", "num_stores_P3M", "num_items_P3M", "num_stores_items_P3M")

display(test_df)

# COMMAND ----------

model_uri = f"runs:/886e4de176894a709449c5c11498952f/model"

# # model = mlflow.pyfunc.load_model(model_uri=model_uri)
# # model.predict(input_X)

# # Prepare the test dataset
# predict_udf = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri)

# display(test_df.withColumn("total_costs_predicted", predict_udf()))

# COMMAND ----------

X_test = test_df.toPandas()

# Convert int64 columns to int32 using a loop
for column in X_test.columns:
    if X_test[column].dtype == 'int64':
        X_test[column] = X_test[column].astype('int32')

# COMMAND ----------

# Run inference using the best model

# Option 1 - Infer directly from the total_cost model
X_test1 = X_test

model = mlflow.pyfunc.load_model(model_uri)
# mlflow.pyfunc.get_model_dependencies(model_uri)

predictions = model.predict(X_test1)
X_test1["total_cost_predicted"] = predictions

display(X_test1)

# COMMAND ----------

# Option 2 - Infer from each cost model separately and sum up the results
X_test2 = X_test

model_uri1 = f"runs:/09727cf371574bcd861b979ee6f0375e/model"

model1 = mlflow.pyfunc.load_model(model_uri1)
# mlflow.pyfunc.get_model_dependencies(model_uri)

predictions1 = model1.predict(X_test)
X_test2["DRFE_forecast_cost_predicted"] = predictions1

model_uri2 = f"runs:/902260bcee234f6f853498a55893175c/model"

model2 = mlflow.pyfunc.load_model(model_uri2)
# mlflow.pyfunc.get_model_dependencies(model_uri)

predictions2 = model2.predict(X_test)
X_test2["value_measurement_cost_predicted"] = predictions2

model_uri3 = f"runs:/f8252cbaafa34089bc747576d9c54739/model"

model3 = mlflow.pyfunc.load_model(model_uri3)
# mlflow.pyfunc.get_model_dependencies(model_uri)

predictions3 = model3.predict(X_test)
X_test2["DLA_alert_gen_cost_predicted"] = predictions3

model_uri4 = f"runs:/40e6e8df9094473eb9e11a31d79d83fe/model"

model4 = mlflow.pyfunc.load_model(model_uri4)
# mlflow.pyfunc.get_model_dependencies(model_uri)

predictions4 = model4.predict(X_test)
X_test2["team_alerts_cost_predicted"] = predictions4

model_uri5 = f"runs:/2a05f936d40e4caeaf0147d60e4b3b1e/model"

model5 = mlflow.pyfunc.load_model(model_uri5)
# mlflow.pyfunc.get_model_dependencies(model_uri)

predictions5 = model5.predict(X_test)
X_test2["data_ingestion_cost_predicted"] = predictions5

X_test2["total_cost_predicted"] = X_test2["DRFE_forecast_cost_predicted"] + X_test2["value_measurement_cost_predicted"] + X_test2["DLA_alert_gen_cost_predicted"] + X_test2["team_alerts_cost_predicted"] + X_test2["data_ingestion_cost_predicted"]

display(X_test2)

# COMMAND ----------


