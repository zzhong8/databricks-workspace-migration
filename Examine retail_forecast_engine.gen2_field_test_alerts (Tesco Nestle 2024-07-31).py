# Databricks notebook source
import datetime
import pyspark.sql.functions as pyf

# COMMAND ----------

# MAGIC %sql
# MAGIC desc RETAIL_FORECAST_ENGINE.GEN2_FIELD_TEST_ALERTS

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from RETAIL_FORECAST_ENGINE.GEN2_FIELD_TEST_ALERTS

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct RETAILER, CLIENT, COUNTRY_CODE, LAST_DATE_INCLUDED from RETAIL_FORECAST_ENGINE.GEN2_FIELD_TEST_ALERTS

# COMMAND ----------

# MAGIC %sql
# MAGIC select min(LAST_DATE_INCLUDED), max(LAST_DATE_INCLUDED) from RETAIL_FORECAST_ENGINE.gen2_field_test_alerts

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from RETAIL_FORECAST_ENGINE.gen2_field_test_alerts

# COMMAND ----------

# MAGIC %sql
# MAGIC select last_date_included, date_generated, count(*) from RETAIL_FORECAST_ENGINE.gen2_field_test_alerts
# MAGIC group by last_date_included, date_generated
# MAGIC order by last_date_included, date_generated

# COMMAND ----------

# MAGIC %sql
# MAGIC select last_date_included, date_generated, count(*) from RETAIL_FORECAST_ENGINE.gen2_field_test_alerts_osa_only
# MAGIC group by last_date_included, date_generated
# MAGIC order by last_date_included, date_generated

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from RETAIL_FORECAST_ENGINE.GEN2_FIELD_TEST_ALERTS
# MAGIC limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from RETAIL_FORECAST_ENGINE.GEN2_FIELD_TEST_ALERTS

# COMMAND ----------


