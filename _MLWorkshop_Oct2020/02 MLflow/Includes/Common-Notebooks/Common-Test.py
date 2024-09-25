# Databricks notebook source
# MAGIC %md
# MAGIC # Integration Tests
# MAGIC The purpose of this notebook is to faciliate testing of our systems.

# COMMAND ----------

spark.conf.set("com.databricks.training.module-name", "common-notebooks")

# COMMAND ----------

# MAGIC %run ./Common

# COMMAND ----------

allDone(courseAdvertisements)
