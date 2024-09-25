# Databricks notebook source
import uuid
import warnings
import pandas as pd
import numpy as np
import datetime

import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.metrics import r2_score

from pyspark.sql import functions as pyf

# COMMAND ----------

# MAGIC %md
# MAGIC Given a list of retailer item id, we want to comperate the performance of loess vs drfe

# COMMAND ----------

sql_statement = """
SELECT CLIENT,
      ORGANIZATION_UNIT_NUM,
      RETAILER_ITEM_ID,
      RETAILER_ITEM_DESC,
      SALES_DT,
      POS_ITEM_QTY,
      DRFE_POS_ITEM_QTY,
      LEGACY_POS_ITEM_QTY,
      DRFE_ERROR,
      LEGACY_ERROR
FROM RETAIL_FORECAST_ENGINE.RESULTS_SUMMARY
WHERE CLIENT = 'CLOROX'
AND SALES_DT <= '2019-02-19'
UNION
SELECT CLIENT,
      ORGANIZATION_UNIT_NUM,
      RETAILER_ITEM_ID,
      RETAILER_ITEM_DESC,
      SALES_DT,
      POS_ITEM_QTY,
      DRFE_POS_ITEM_QTY,
      0 AS LEGACY_POS_ITEM_QTY,
      DRFE_ERROR,
      0 AS LEGACY_ERROR
FROM RETAIL_FORECAST_ENGINE.PERFORMANCE
WHERE CLIENT = 'CLOROX'
AND SALES_DT > '2019-02-19'
"""
df = spark.sql(sql_statement)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DRFE_LOAD_TS
# MAGIC FROM RETAIL_FORECAST_ENGINE.PERFORMANCE
# MAGIC WHERE CLIENT = 'CAMPBELLSSNACK'
# MAGIC GROUP BY DRFE_LOAD_TS
# MAGIC ORDER BY DRFE_LOAD_TS DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DRFE_LOAD_TS
# MAGIC FROM RETAIL_FORECAST_ENGINE.PERFORMANCE
# MAGIC WHERE CLIENT = 'campbellssnack'
# MAGIC GROUP BY DRFE_LOAD_TS
# MAGIC ORDER BY DRFE_LOAD_TS DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM RETAIL_FORECAST_ENGINE.PERFORMANCE
# MAGIC WHERE UPPER(CLIENT) = 'CAMPBELLSSNACK'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SALES_DT, DRFE_LOAD_TS
# MAGIC FROM RETAIL_FORECAST_ENGINE.PERFORMANCE
# MAGIC WHERE UPPER(CLIENT) = 'CAMPBELLSSNACK'
# MAGIC GROUP BY SALES_DT, DRFE_LOAD_TS
# MAGIC ORDER BY DRFE_LOAD_TS DESC, SALES_DT DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DRFE_LOAD_TS
# MAGIC FROM RETAIL_FORECAST_ENGINE.PERFORMANCE
# MAGIC WHERE UPPER(CLIENT) = 'CLOROX'
# MAGIC GROUP BY DRFE_LOAD_TS
# MAGIC ORDER BY DRFE_LOAD_TS DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DRFE_LOAD_TS
# MAGIC FROM RETAIL_FORECAST_ENGINE.PERFORMANCE
# MAGIC WHERE UPPER(CLIENT) = 'TYSONHILLSHIRE'
# MAGIC GROUP BY DRFE_LOAD_TS
# MAGIC ORDER BY DRFE_LOAD_TS DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CLIENT
# MAGIC FROM RETAIL_FORECAST_ENGINE.PERFORMANCE
# MAGIC GROUP BY CLIENT

# COMMAND ----------

display(df.orderBy('SALES_DT', ascending=False))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT LOAD_TS
# MAGIC FROM walmart_campbellssnack_us_retail_alert_im.drfe_forecast_baseline_unit
# MAGIC GROUP BY LOAD_TS
# MAGIC ORDER BY LOAD_TS DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT LOAD_TS
# MAGIC FROM walmart_clorox_us_retail_alert_im.drfe_forecast_baseline_unit
# MAGIC GROUP BY LOAD_TS
# MAGIC ORDER BY LOAD_TS DESC

# COMMAND ----------

item_id = '550646751'
store = '2210'
df.filter((df.RETAILER_ITEM_ID==item_id) & (df.ORGANIZATION_UNIT_NUM==store)).select(pyf.max('SALES_DT'), pyf.min('SALES_DT')).show()

# COMMAND ----------

display(df.filter('DRFE_POS_ITEM_QTY <=200'))

# COMMAND ----------

display(df.filter((df.RETAILER_ITEM_ID==item_id) & (df.ORGANIZATION_UNIT_NUM==store)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC select model_type from retail_forecast_engine.performance
# MAGIC group by model_type

# COMMAND ----------

df = spark.sql("""select * from  RETAIL_FORECAST_ENGINE.PERFORMANCE where model_type in ('CatBoostRegressor', 'LassoCV', 'BayesianRidge') """)

# COMMAND ----------

# MAGIC %md
# MAGIC #### High dollar sale items

# COMMAND ----------

high_dollar = df.filter('high_dollar_sales==1').filter(df['MODEL_TYPE']=='CatBoostRegressor')

high_dollar = high_dollar.withColumn('SQR_ERROR', high_dollar['DRFE_POS_ITEM_QTY']*high_dollar['DRFE_POS_ITEM_QTY'])

r2_df = high_dollar.groupBy(['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM']).agg((pyf.variance(high_dollar['POS_ITEM_QTY'])/pyf.count(high_dollar['POS_ITEM_QTY'])).alias('Stotal'), pyf.sum(high_dollar['SQR_ERROR']).alias('Sreg'))

r2_df = r2_df.withColumn('r2', 1-(r2_df['Sreg']/r2_df['Stotal']))

# COMMAND ----------

r2_pd = r2_df.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC Sample some data

# COMMAND ----------

display(df.select(['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM']).distinct())

# COMMAND ----------

display(df.filter('HIGH_DOLLAR_SALES==1').select(['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM']).distinct())
high_dollar_df = df.filter('HIGH_DOLLAR_SALES==1')
high_dollar_df = high_dollar_df.filter(high_dollar_df['RETAILER_ITEM_ID'].isin(['551033404', '551089196', '556978160', '4673278', '1309982' '566050927', '551214100', '562965811', '552100479', '9261391']))

# COMMAND ----------

print(high_dollar_df.count())

# COMMAND ----------

high_dollar_df.coalesce(1)\
  .write.format('com.databricks.spark.csv')\
  .option('header', 'true')\
  .mode('overwrite')\
  .save("/mnt/artifacts/Monitoring/clorox_high_dollar.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### High frequency

# COMMAND ----------

high_frequency = df.filter('HIGH_FREQUENCY==1')
# display(high_frequency.select(['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM']).distinct())
high_frequency = high_frequency.filter(high_frequency['RETAILER_ITEM_ID'].isin(['1390254', '1312862', '550646457', '1345144', '550646457' '1345144', '550646455', '1375162', '9261293', '417132']))

# COMMAND ----------

high_frequency.count()

# COMMAND ----------

high_frequency.coalesce(1)\
  .write.format('com.databricks.spark.csv')\
  .option('header', 'true')\
  .mode('overwrite')\
  .save("/mnt/artifacts/Monitoring/clorox_high_frequency.csv")

# COMMAND ----------

abs_df = df.withColumn("abs drfe", pyf.abs(df.DRFE_ERROR)).withColumn("abs legacy", pyf.abs(df.LEGACY_ERROR))
display(abs_df.filter((abs_df.RETAILER_ITEM_ID=='557188137') & (abs_df.ORGANIZATION_UNIT_NUM=='2210')))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Low frequency

# COMMAND ----------

low_frequency = df.filter('LOW_FREQUENCY==1')
item_list = low_frequency.filter(low_frequency['MODEL_TYPE']=='CatBoostRegressor').select(['RETAILER_ITEM_ID']).distinct().take(10)
low_frequency = low_frequency.filter(low_frequency['RETAILER_ITEM_ID'].isin([item[0] for item in item_list]))

# COMMAND ----------

display(low_frequency.select(['RETAILER_ITEM_ID', 'MODEL_TYPE']).distinct())

# COMMAND ----------

low_frequency.coalesce(1)\
  .write.format('com.databricks.spark.csv')\
  .option('header', 'true')\
  .mode('overwrite')\
  .save("/mnt/artifacts/Monitoring/clorox_low_frequency.csv")

# COMMAND ----------


