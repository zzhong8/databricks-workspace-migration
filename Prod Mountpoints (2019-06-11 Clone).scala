// Databricks notebook source
dbutils.fs.mkdirs("/mnt/prod-ro/")
dbutils.fs.mkdirs("/mnt/prod-ro/artifacts")
dbutils.fs.mkdirs("/mnt/prod-ro/processed")

// COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://artifacts@azueus2prdsadsretfceng.blob.core.windows.net",
  mountPoint = "/mnt/prod-ro/artifacts",
  extraConfigs = Map("fs.azure.sas.artifacts.azueus2prdsadsretfceng.blob.core.windows.net" -> "?sv=2018-03-28&ss=bq&srt=sco&sp=rl&st=2019-01-15T16%3A26%3A45Z&se=2025-01-16T16%3A26%3A00Z&sig=Om6EKzuaDCFyVSd9GVUr4iv0waAIHHaP8EtITpeFzYc%3D")
)

// COMMAND ----------

display(dbutils.fs.ls("/mnt/prod-ro/artifacts/"))

// COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://processed@azueus2prdsadsretfceng.blob.core.windows.net",
  mountPoint = "/mnt/prod-ro/processed",
  extraConfigs = Map("fs.azure.sas.processed.azueus2prdsadsretfceng.blob.core.windows.net" -> "?sv=2018-03-28&ss=bq&srt=sco&sp=rl&st=2019-01-15T16%3A26%3A45Z&se=2025-01-16T16%3A26%3A00Z&sig=Om6EKzuaDCFyVSd9GVUr4iv0waAIHHaP8EtITpeFzYc%3D" )
)

// COMMAND ----------

display(dbutils.fs.ls("/mnt/prod-ro/processed/"))

// COMMAND ----------

display(dbutils.fs.mounts())

// COMMAND ----------

display(dbutils.fs.ls("/mnt/prod-ro/artifacts/reference/snap_paycycle/retailer=test/"))

// COMMAND ----------


