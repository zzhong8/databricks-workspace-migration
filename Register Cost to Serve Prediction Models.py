# Databricks notebook source
import mlflow

# COMMAND ----------

# total_cost model

model_uri = f"runs:/886e4de176894a709449c5c11498952f/model"
model_name = "Total cost model"

registered_model_version = mlflow.register_model(model_uri, model_name)

# COMMAND ----------

# DRFE_forecast_cost model

model_uri = f"runs:/09727cf371574bcd861b979ee6f0375e/model"
model_name = "DRFE forecast cost model"

registered_model_version = mlflow.register_model(model_uri, model_name)

# COMMAND ----------

# value_measurement_cost model

model_uri = f"runs:/902260bcee234f6f853498a55893175c/model"
model_name = "Value measurement cost model"

registered_model_version = mlflow.register_model(model_uri, model_name)

# COMMAND ----------

# DLA_alert_gen_cost model

model_uri = f"runs:/f8252cbaafa34089bc747576d9c54739/model"
model_name = "DLA alert gen cost model"

registered_model_version = mlflow.register_model(model_uri, model_name)

# COMMAND ----------

# team_alerts_cost model

model_uri = f"runs:/40e6e8df9094473eb9e11a31d79d83fe/model"
model_name = "Team alerts cost model"

registered_model_version = mlflow.register_model(model_uri, model_name)

# COMMAND ----------

# data_ingestion_cost model

model_uri = f"runs:/2a05f936d40e4caeaf0147d60e4b3b1e/model"
model_name = "Data ingestion cost model"

registered_model_version = mlflow.register_model(model_uri, model_name)
