# Databricks notebook source
# MAGIC %md #Notebook to test the execution of the notebooks in the workshop
# MAGIC Please execute this notebook in advance of the workshop, it runs some prep that ensures all notebooks execute
# MAGIC
# MAGIC This notebook runs on Databricks ML Runtime 7.0 with %pip/%conda magic commands enabled as of this writing.

# COMMAND ----------

#Run 01
try:
  dbutils.notebook.run("../01 Intro to DS on Databricks/01 DS Features",0)
except:
  print("Error Running 01")

# COMMAND ----------

# DBTITLE 0,Run all notebooks
#Run 02
try:
  dbutils.notebook.run("../02 MLflow/01-MLflow-Overview",0)
  dbutils.notebook.run("../02 MLflow/02-Experiment-Tracking",0)
#   dbutils.notebook.run("../02 MLflow/03-Packaging-ML-Projects",0)
  dbutils.notebook.run("../02 MLflow/04-Multistep-Workflows",0)
  dbutils.notebook.run("../02 MLflow/05-Model-Management",0)
  dbutils.notebook.run("../02 MLflow/06-Model-Registry",0)
  dbutils.notebook.run("../02 MLflow/97_MLflow_SparkML_Detailed",0)
  dbutils.notebook.run("../02 MLflow/98_MLflow_SparkML_Tutorial",0)
except:
  print("Error Running 02")

# COMMAND ----------

#Run 05
try:
  print("Skipping Koalas test (notebooks contain intentional errors)")
  #dbutils.notebook.run("../05 Koalas/01 Koalas Working Demo",0)
  #dbutils.notebook.run("../05 Koalas/02 Alternative Simple Koalas Demo",0)
except:
  print("Error Running 05")

# COMMAND ----------

#Run 06
# dbutils.notebook.run("../02a MLflow Intro/00 mlflow_quick_start_tracking_server", 0)
# dbutils.notebook.run("../03 Parallelising ML/01 Parallelise Feature Engineering with Spark", 0)
# dbutils.notebook.run("../03 Parallelising ML/02 Random Forest Distributed HyperOpt", 0) # Note that this takes around 14 minutes to run
# dbutils.notebook.run("../03 Parallelising ML/03 Parallelise Single Model Training", 0)
# dbutils.notebook.run("../03 Parallelising ML/04 Lots of Models", 0)
# dbutils.notebook.run("../04 Model Deployment/01 Model Deploy - Batch and Stream", 180) 
# dbutils.notebook.run("../05 Koalas/01 Alternative_Simple_Koalas_Demo", 0)

# COMMAND ----------

# DBTITLE 1,Run XGBoost end-to-end example (single node and distributed)
# #dbutils.notebook.run("./_prepData",0)
# dbutils.notebook.run(".../Dev/BoostingMethods/")

# COMMAND ----------

# DBTITLE 1,Run LightGBM end-to-end example (single node and distributed)

