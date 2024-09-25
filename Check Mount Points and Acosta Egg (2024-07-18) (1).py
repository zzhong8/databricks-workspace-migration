# Databricks notebook source
import mlflow
import mlflow.keras
from hyperopt import fmin, tpe, hp, STATUS_OK

# COMMAND ----------

import acosta

print(acosta.__version__)

# COMMAND ----------

import acosta
import expectation

print(acosta.__version__)
print(expectation.__version__)

# COMMAND ----------

# MAGIC %scala
# MAGIC display(dbutils.fs.mounts)

# COMMAND ----------

# MAGIC %scala
# MAGIC display(dbutils.fs.ls("dbfs:/mnt/artifacts/packages/"))

# COMMAND ----------

# MAGIC %scala
# MAGIC display(dbutils.fs.ls("dbfs:/databricks/egg/acosta_alerting-gen2-migration-latest.egg"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc retaillink_walmart_danoneusllc_us_dv.vw_latest_sat_epos_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from retaillink_walmart_danoneusllc_us_dv.vw_latest_sat_epos_summary
# MAGIC limit 10

# COMMAND ----------


