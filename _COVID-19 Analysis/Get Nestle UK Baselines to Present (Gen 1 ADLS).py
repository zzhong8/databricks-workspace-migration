# Databricks notebook source
import uuid
import warnings
import numpy as np
import pandas as pd
from datetime import datetime
from pyspark.sql import Window
from pyspark.sql import types as pyt
from pyspark.sql import functions as pyf
from pyspark.sql import SQLContext

import acosta
from acosta.alerting.preprocessing import read_pos_data

print(acosta.__version__)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from asda_nestlecore_uk_retail_alert_im.drfe_forecast_baseline_unit

# COMMAND ----------

df_sql_query_asda_nestle = """
    select SALES_DT, BASELINE_POS_ITEM_QTY from asda_nestlecore_uk_retail_alert_im.drfe_forecast_baseline_unit
    where SALES_DT >= '2022-01-01'
    order by SALES_DT
"""

data_vault_data1 = spark.sql(df_sql_query_asda_nestle)

total_baseline_sales_by_date1 = data_vault_data1.select("SALES_DT", "BASELINE_POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_baseline_sales_by_date1)

# COMMAND ----------

df_sql_query_asda_nestleb = """
    select SALES_DT, BASELINE_POS_ITEM_QTY from asda_nestlecore_uk_retail_alert_im.drfe_forecast_baseline_unit
    where SALES_DT >= '2023-01-01'
    order by SALES_DT
"""

data_vault_data1b = spark.sql(df_sql_query_asda_nestleb)

total_baseline_sales_by_date1b = data_vault_data1b.select("SALES_DT", "BASELINE_POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_baseline_sales_by_date1b)

# COMMAND ----------

df_sql_query_asda_nestlec = """
    select SALES_DT, BASELINE_POS_ITEM_QTY from asda_nestlecore_uk_retail_alert_im.drfe_forecast_baseline_unit
    where SALES_DT >= '2023-07-29'
    order by SALES_DT
"""

data_vault_data1c = spark.sql(df_sql_query_asda_nestlec)

total_baseline_sales_by_date1c = data_vault_data1c.select("SALES_DT", "BASELINE_POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_baseline_sales_by_date1c)

# COMMAND ----------

df_sql_query_asda_nestled = """
    select SALES_DT, BASELINE_POS_ITEM_QTY from asda_nestlecore_uk_retail_alert_im.drfe_forecast_baseline_unit
    where SALES_DT >= '2023-08-03'
    order by SALES_DT
"""

data_vault_data1d = spark.sql(df_sql_query_asda_nestled)

total_baseline_sales_by_date1d = data_vault_data1d.select("SALES_DT", "BASELINE_POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_baseline_sales_by_date1d)

# COMMAND ----------

df_sql_query_morrisons_nestle = """
    select SALES_DT, BASELINE_POS_ITEM_QTY from morrisons_nestlecore_uk_retail_alert_im.drfe_forecast_baseline_unit
    where SALES_DT >= '2022-01-01'
    order by SALES_DT
"""

data_vault_data2 = spark.sql(df_sql_query_morrisons_nestle)

total_baseline_sales_by_date2 = data_vault_data2.select("SALES_DT", "BASELINE_POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_baseline_sales_by_date2)

# COMMAND ----------

df_sql_query_morrisons_nestleb = """
    select SALES_DT, BASELINE_POS_ITEM_QTY from morrisons_nestlecore_uk_retail_alert_im.drfe_forecast_baseline_unit
    where SALES_DT >= '2023-01-01'
    order by SALES_DT
"""

data_vault_data2b = spark.sql(df_sql_query_morrisons_nestleb)

total_baseline_sales_by_date2b = data_vault_data2b.select("SALES_DT", "BASELINE_POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_baseline_sales_by_date2b)

# COMMAND ----------

df_sql_query_sainsburys_nestle = """
    select SALES_DT, BASELINE_POS_ITEM_QTY from sainsburys_nestlecore_uk_retail_alert_im.drfe_forecast_baseline_unit
    where SALES_DT >= '2022-01-01'
    order by SALES_DT
"""

data_vault_data3 = spark.sql(df_sql_query_sainsburys_nestle)

total_baseline_sales_by_date3 = data_vault_data3.select("SALES_DT", "BASELINE_POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_baseline_sales_by_date3)

# COMMAND ----------

df_sql_query_tesco_nestle = """
    select SALES_DT, BASELINE_POS_ITEM_QTY from tesco_nestlecore_uk_retail_alert_im.drfe_forecast_baseline_unit
    where SALES_DT >= '2022-01-01'
    order by SALES_DT
"""

data_vault_data4 = spark.sql(df_sql_query_tesco_nestle)

total_baseline_sales_by_date4 = data_vault_data4.select("SALES_DT", "BASELINE_POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_baseline_sales_by_date4)

# COMMAND ----------


