# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Define Custom Flavor (DTW) For Managed Model Logging/Loading
# MAGIC #### Cluster Requirements
# MAGIC
# MAGIC In order to run this notebook, the following dependencies must be installed on the cluster:
# MAGIC
# MAGIC - mlflow (latest version)
# MAGIC
# MAGIC - dill
# MAGIC
# MAGIC - Python 3

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 1 - Create directory in local directories with model logging/loading dependencies

# COMMAND ----------

# MAGIC %sh mkdir -p /tmp/custom_model

# COMMAND ----------

# MAGIC %fs cp /FileStore/tables/custom_flavor.py file:/tmp/custom_model/custom_flavor.py

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 2 - Add Dependencies to PYTHON sys path

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Finally, we will add the directory containing the dependencies to the Python system path so that we can import them.

# COMMAND ----------

import sys
sys.path.append('/tmp/custom_model')

# COMMAND ----------

# MAGIC %fs 
# MAGIC
# MAGIC cp /FileStore/tables/Sales_Transactions_Dataset_Weekly.csv file:/databricks/driver/sales.csv

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 3 - Test Model Logging

# COMMAND ----------

import mlflow
import custom_flavor as mlflow_custom_flavor
import tempfile
import os

mlflow.set_experiment("/Users/ricardo.portilla@databricks.com/ricardo.portilla@databricks.com/Unsupervised Learning/Test_DTW_Log_Model")

with mlflow.start_run() as run:
  dtw_model = {'stretch_factor' : 0.50, 'pattern' : [12.5, 13, 13.5, 14, 17, 10, 15, 16, 17, 16, 19, 14, 14, 15, 11, 20, 18, 17, 19, 21, 19, 15, 17, 17, 16, 15, 19, 19, 20, 21, 19, 18, 17, 16, 20, 19, 19, 18, 17, 18, 26, 25, 17, 23, 21, 19, 28, 18, 19, 16, 21, 23]}
  artifact_path = "pyfunc_dtw_model"
  mlflow_custom_flavor.log_model(dtw_model, artifact_path=artifact_path)
  print(run.info.run_uuid)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 4 - Test Model Loading (which uses the custom flavor wrapper defined in our flavor module)

# COMMAND ----------

loaded_model = mlflow_custom_flavor.load_model(artifact_path, run_id = '1617fd83a3664defbff6dab4bfa62f84')

# COMMAND ----------

import random
import pandas as pd

# Use same schema from sales DF
sales_cols = pd.read_csv("/databricks/driver/sales.csv", header='infer').columns
new_prod_1 = ['New Product 1'] + random.sample(range(1, 107), 106)
new_prod_2 = ['New Product 2'] + random.sample(range(1, 107), 106)
new_sales_units = pd.DataFrame([new_prod_1, new_prod_2], columns = sales_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Testing our DTW model against Optimal Trend!

# COMMAND ----------

outputs = loaded_model.predict(new_sales_units)
print(outputs)
