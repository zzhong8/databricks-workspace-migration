# Databricks notebook source
from pyspark.sql import functions as pyf
from pyspark.sql import types as pyt

# COMMAND ----------

drfe_forecast_baseline_unit_table = spark.sql("SELECT * FROM retail_forecast_engine.drfe_forecast_baseline_unit")

# COMMAND ----------

drfe_forecast_baseline_unit_table.printSchema()

# COMMAND ----------

drfe_forecast_baseline_unit_table.count()

# COMMAND ----------

sales_dates = drfe_forecast_baseline_unit_table.select('SALES_DT').distinct().orderBy('SALES_DT', ascending=False)

# COMMAND ----------

display(sales_dates)

# COMMAND ----------

drfe_forecast_baseline_unit_table_2019_06_18 = drfe_forecast_baseline_unit_table.filter(drfe_forecast_baseline_unit_table.SALES_DT == '2019-06-18')

# COMMAND ----------

drfe_forecast_baseline_unit_table_2019_06_18.count()

# COMMAND ----------

display(drfe_forecast_baseline_unit_table_2019_06_18)

# COMMAND ----------


