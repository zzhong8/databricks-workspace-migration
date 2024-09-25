# Databricks notebook source
import uuid
import warnings
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import datetime

from pyspark.sql import functions as pyf
import pyspark.sql.types as pyt

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MAX(SALES_DT),
# MAGIC       MIN(SALES_DT),
# MAGIC       COUNT(DISTINCT(SALES_DT)),
# MAGIC       LOAD_TS
# MAGIC FROM walmart_clorox_retail_alert_im.DRFE_FORECAST_BASELINE_UNIT
# MAGIC GROUP BY LOAD_TS
# MAGIC ORDER BY LOAD_TS DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MODEL_TYPE,
# MAGIC       CLIENT,
# MAGIC       ORGANIZATION_UNIT_NUM,
# MAGIC       SALES_DT,
# MAGIC       RETAILER_ITEM_ID,
# MAGIC       RETAILER_ITEM_DESC,
# MAGIC       POS_ITEM_QTY,
# MAGIC       DRFE_POS_ITEM_QTY
# MAGIC FROM
# MAGIC   (SELECT *,
# MAGIC           ROW_NUMBER() OVER(PARTITION BY CLIENT, ORGANIZATION_UNIT_NUM, SALES_DT ORDER BY DRFE_POS_ITEM_QTY DESC) AS ROW_NUMBER
# MAGIC   FROM RETAIL_FORECAST_ENGINE.RESULTS_SUMMARY
# MAGIC           )
# MAGIC WHERE ROW_NUMBER = 1
# MAGIC AND SALES_DT >= '2019-02-09'
# MAGIC AND MODEL_TYPE IN ('RidgeCV', 'LassoCV', 'BayesianRidge')
# MAGIC AND DRFE_POS_ITEM_QTY > 250
# MAGIC ORDER BY DRFE_POS_ITEM_QTY DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MODEL_TYPE,
# MAGIC       CLIENT,
# MAGIC       RETAILER_ITEM_ID,
# MAGIC       RETAILER_ITEM_DESC
# MAGIC FROM
# MAGIC   (SELECT *,
# MAGIC           ROW_NUMBER() OVER(PARTITION BY CLIENT, ORGANIZATION_UNIT_NUM, SALES_DT ORDER BY DRFE_POS_ITEM_QTY DESC) AS ROW_NUMBER
# MAGIC   FROM RETAIL_FORECAST_ENGINE.RESULTS_SUMMARY
# MAGIC   WHERE DRFE_POS_ITEM_QTY > 250
# MAGIC           )
# MAGIC WHERE ROW_NUMBER = 1
# MAGIC AND SALES_DT >= '2019-02-09'
# MAGIC AND MODEL_TYPE IN ('RidgeCV', 'LassoCV', 'BayesianRidge')
# MAGIC GROUP BY MODEL_TYPE,
# MAGIC         CLIENT,
# MAGIC         RETAILER_ITEM_ID,
# MAGIC         RETAILER_ITEM_DESC
# MAGIC ORDER BY CLIENT, RETAILER_ITEM_ID

# COMMAND ----------

df = spark.sql("""SELECT * FROM RETAIL_FORECAST_ENGINE.RESULTS_SUMMARY """)

# COMMAND ----------

grouped_df_new = df.filter(df.SALES_DT >= datetime.date(2019, 2, 9))\
                       .groupby("MODEL_TYPE").agg(pyf.sum("POS_ITEM_QTY").alias("Total POS actual"), 
                                          pyf.count("RETAILER_ITEM_ID").alias("Total count"), 
                                          pyf.round(pyf.mean("TEST_SET_MSE_PERFORMANCE"), 2).alias("Mean_Test_MSE")
                                         ).orderBy(pyf.desc("Total count")).toPandas()
display(grouped_df_new)

# COMMAND ----------

# MAGIC %md
# MAGIC ### explorating the residuals
# MAGIC We only look into residuals from 2019-02-09 and assume that the champions are rewriten.

# COMMAND ----------

df = spark.sql("""SELECT * FROM RETAIL_FORECAST_ENGINE.RESULTS_SUMMARY""")

# COMMAND ----------

recent_df = df.filter(df.SALES_DT >= datetime.date(2019, 2, 9)).filter(df.MODEL_TYPE.isin('RidgeCV', 'LassoCV', 'BayesianRidge'))

# COMMAND ----------

display(recent_df.groupby('SALES_DT').agg(pyf.sum('DRFE_POS_ITEM_QTY').alias('Total prediction QTY')))

# COMMAND ----------

display(recent_df.groupby('SALES_DT').agg(pyf.avg('DRFE_POS_ITEM_QTY').alias("Mean prediction QTY")))

# COMMAND ----------

display(recent_df.groupby('SALES_DT').agg(pyf.avg('DRFE_ERROR').alias('Error')))

# COMMAND ----------

display(recent_df)

# COMMAND ----------

display(recent_df.select("RETAILER_ITEM_DESC", "CLIENT", "SALES_DT", "POS_ITEM_QTY", "DRFE_POS_ITEM_QTY", "DRFE_ERROR").sort("DRFE_ERROR"))

# COMMAND ----------

display(recent_df.groupby("ORGANIZATION_UNIT_NUM", "SALES_DT").agg(pyf.avg("DRFE_ERROR").alias("Mean_Error")).sort("Mean_Error"))

# COMMAND ----------

display(recent_df.selectExpr("max(SALES_DT)"))

# COMMAND ----------


