# Databricks notebook source
import uuid
import warnings
import pandas as pd
import numpy as np
import datetime

from pyspark.sql import functions as pyf
import pyspark.sql.types as pyt

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set a threshold to filter the prediction outliers
# MAGIC The drfe predictions include outliers. For now if set a threshold to filter out the outliers

# COMMAND ----------

drfe_threshold = 100

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Create a view to produce deciles at a client-retailer's level */
# MAGIC CREATE OR REPLACE VIEW retail_forecast_engine.VW_MONITORING_RETAILER_CLIENT_POS_DECILE_A_HZ
# MAGIC AS SELECT 'ACTUAL' AS SOURCE,
# MAGIC             RETAILER,
# MAGIC             CLIENT,
# MAGIC             POS_ITEM_QTY AS POS_ITEM_QTY,
# MAGIC             NTILE(10) OVER (PARTITION BY RETAILER, CLIENT ORDER BY POS_ITEM_QTY) AS DECILE
# MAGIC    FROM retail_forecast_engine.RESULTS_SUMMARY
# MAGIC    
# MAGIC    UNION ALL
# MAGIC    SELECT 'DRFE' AS SOURCE,
# MAGIC           RETAILER,
# MAGIC           CLIENT,
# MAGIC           DRFE_POS_ITEM_QTY AS POS_ITEM_QTY,
# MAGIC           NTILE(10) OVER (PARTITION BY RETAILER, CLIENT ORDER BY DRFE_POS_ITEM_QTY) AS DECILE
# MAGIC    FROM retail_forecast_engine.RESULTS_SUMMARY
# MAGIC    WHERE DRFE_POS_ITEM_QTY <= 100
# MAGIC    
# MAGIC    UNION ALL
# MAGIC    SELECT 'LEGACY' AS SOURCE,
# MAGIC           RETAILER,
# MAGIC           CLIENT,
# MAGIC           LEGACY_POS_ITEM_QTY AS POS_ITEM_QTY,
# MAGIC           NTILE(10) OVER (PARTITION BY RETAILER, CLIENT ORDER BY LEGACY_POS_ITEM_QTY) AS DECILE
# MAGIC    FROM retail_forecast_engine.RESULTS_SUMMARY
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Create a summary view to produce deciles for all retailers and clients */
# MAGIC
# MAGIC CREATE OR REPLACE VIEW retail_forecast_engine.VW_MONITORING_POS_DECILE_A_HZ
# MAGIC AS SELECT 'ACTUAL' AS SOURCE,
# MAGIC           POS_ITEM_QTY AS POS_ITEM_QTY,
# MAGIC           NTILE(10) OVER (ORDER BY POS_ITEM_QTY) AS DECILE
# MAGIC    FROM retail_forecast_engine.RESULTS_SUMMARY
# MAGIC    
# MAGIC    UNION ALL
# MAGIC    SELECT 'DRFE' AS SOURCE,
# MAGIC           DRFE_POS_ITEM_QTY AS POS_ITEM_QTY,
# MAGIC           NTILE(10) OVER (ORDER BY DRFE_POS_ITEM_QTY) AS DECILE
# MAGIC    FROM retail_forecast_engine.RESULTS_SUMMARY
# MAGIC    WHERE DRFE_POS_ITEM_QTY <= 100
# MAGIC    
# MAGIC    UNION ALL
# MAGIC    SELECT 'LEGACY' AS SOURCE,
# MAGIC           LEGACY_POS_ITEM_QTY AS POS_ITEM_QTY,
# MAGIC           NTILE(10) OVER (ORDER BY LEGACY_POS_ITEM_QTY) AS DECILE
# MAGIC    FROM retail_forecast_engine.RESULTS_SUMMARY

# COMMAND ----------


