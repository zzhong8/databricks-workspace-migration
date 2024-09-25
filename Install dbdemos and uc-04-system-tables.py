# Databricks notebook source
# MAGIC %pip install dbdemos

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import dbdemos

# COMMAND ----------

dbdemos.help()

# COMMAND ----------

dbdemos.list_demos()

# COMMAND ----------

dbdemos.install('uc-04-system-tables')

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false $catalog=main $schema=billing_forecast

# COMMAND ----------


