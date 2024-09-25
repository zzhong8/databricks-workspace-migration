# Databricks notebook source
import datetime
import pyspark.sql.functions as pyf

# COMMAND ----------

sql_query_morrisons_nestle_2024_alerts = """
    SELECT distinct FROM team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
    WHERE Retail_Client = 'morrisons_nestlecore_uk'
    AND sales_dt >= '2024-01-01'
    AND sales_dt <= '2024-06-30'
    ORDER BY sales_dt, organization_unit_num, retailer_item_id
"""

df_morrisons_nestle_2024_alerts = spark.sql(sql_query_morrisons_nestle_2024_alerts)

df_morrisons_nestle_2024_alerts.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count (distinct Retail_Client, ORGANIZATION_UNIT_NUM, OutletId) FROM team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count (distinct Retail_Client, ORGANIZATION_UNIT_NUM) FROM team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT distinct Retail_Client, ORGANIZATION_UNIT_NUM, OutletId FROM team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
# MAGIC order by Retail_Client, ORGANIZATION_UNIT_NUM, OutletId

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Retail_Client, count (distinct ORGANIZATION_UNIT_NUM, OutletId, RETAILER_ITEM_ID, SALES_DT) FROM team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
# MAGIC GROUP BY Retail_Client
# MAGIC order by Retail_Client

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Retail_Client, count (*) FROM team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
# MAGIC GROUP BY Retail_Client
# MAGIC order by Retail_Client

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Retail_Client, count (distinct ORGANIZATION_UNIT_NUM, RETAILER_ITEM_ID, SALES_DT) FROM team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
# MAGIC where sales_dt >= '2024-06-01'
# MAGIC     AND sales_dt <= '2024-07-31'
# MAGIC GROUP BY Retail_Client
# MAGIC order by Retail_Client

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Retail_Client, count (distinct ORGANIZATION_UNIT_NUM, OutletId, RETAILER_ITEM_ID, SALES_DT) FROM team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
# MAGIC where sales_dt >= '2024-06-01'
# MAGIC     AND sales_dt <= '2024-07-31'
# MAGIC GROUP BY Retail_Client
# MAGIC order by Retail_Client

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Retail_Client, count (*) FROM team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
# MAGIC where sales_dt >= '2024-06-01'
# MAGIC     AND sales_dt <= '2024-07-31'
# MAGIC GROUP BY Retail_Client
# MAGIC order by Retail_Client

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
# MAGIC where Retail_Client = 'morrisons_nestlecore_uk'
# MAGIC     AND sales_dt >= '2024-07-01'
# MAGIC     AND sales_dt <= '2024-07-31'
# MAGIC order by SALES_DT, ORGANIZATION_UNIT_NUM, OutletId, RETAILER_ITEM_ID

# COMMAND ----------


