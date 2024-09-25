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
# MAGIC select * from retail_alert_asda_premier_uk_im.drfe_forecast_baseline_unit

# COMMAND ----------

df_sql_query_asda_premier = """
    select SALES_DT, BASELINE_POS_ITEM_QTY from retail_alert_asda_premier_uk_im.drfe_forecast_baseline_unit
    where SALES_DT >= '2022-01-01'
    order by SALES_DT
"""

data_vault_data1 = spark.sql(df_sql_query_asda_premier)

total_baseline_sales_by_date1 = data_vault_data1.select("SALES_DT", "BASELINE_POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_baseline_sales_by_date1)

# COMMAND ----------

df_sql_query_morrisons_premier = """
    select SALES_DT, BASELINE_POS_ITEM_QTY from retail_alert_morrisons_premier_uk_im.drfe_forecast_baseline_unit
    where SALES_DT >= '2022-01-01'
    order by SALES_DT
"""

data_vault_data2 = spark.sql(df_sql_query_morrisons_premier)

total_baseline_sales_by_date2 = data_vault_data2.select("SALES_DT", "BASELINE_POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_baseline_sales_by_date2)

# COMMAND ----------

df_sql_query_tesco_premier = """
    select SALES_DT, BASELINE_POS_ITEM_QTY from retail_alert_tesco_premier_uk_im.drfe_forecast_baseline_unit
    where SALES_DT >= '2022-01-01'
    order by SALES_DT
"""

data_vault_data4 = spark.sql(df_sql_query_tesco_premier)

total_baseline_sales_by_date4 = data_vault_data4.select("SALES_DT", "BASELINE_POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_baseline_sales_by_date4)

# COMMAND ----------


