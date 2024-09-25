# Databricks notebook source
import datetime
import pyspark.sql.functions as pyf

from expectation.functions import pivot_pos, get_pos_prod, get_price

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Asda Beiersdorf

# COMMAND ----------

database_name_asda_beiersdorf = "retaillink_asda_beiersdorf_uk_dv"

df_price_asda_beiersdorf = get_price(database_name_asda_beiersdorf, spark)

display(df_price_asda_beiersdorf)

# COMMAND ----------

sql_query_asda_beiersdorf_p12m_OSA_alerts_raw = """
    SELECT
    Retail_Client,
    ORGANIZATION_UNIT_NUM,
    RETAILER_ITEM_ID,
    ALERT_MESSAGE_DESC,
    ALERT_TYPE_NM,
    ON_HAND_INVENTORY_QTY,
    LOST_SALES_AMT,
    SALES_DT
    from team_retail_alert_beiersdorf_drt_uk_im.alert_on_shelf_availability
    WHERE Retail_Client = 'asda_beiersdorf_uk'
    AND sales_dt >= '2023-07-01'
    AND sales_dt <= '2024-06-30'
    ORDER BY sales_dt, organization_unit_num, retailer_item_id
"""

df_asda_beiersdorf_p12m_OSA_alerts_raw = spark.sql(sql_query_asda_beiersdorf_p12m_OSA_alerts_raw)

display(df_asda_beiersdorf_p12m_OSA_alerts_raw)

# COMMAND ----------

df_asda_beiersdorf_p12m_OSA_alerts = df_asda_beiersdorf_p12m_OSA_alerts_raw.join(df_price_asda_beiersdorf, on=['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM', 'sales_dt'])

display(df_asda_beiersdorf_p12m_OSA_alerts)

# COMMAND ----------

df_asda_beiersdorf_p12m_OSA_alerts_pre = df_asda_beiersdorf_p12m_OSA_alerts.filter(df_asda_beiersdorf_p12m_OSA_alerts.SALES_DT < '2024-01-23')

display(df_asda_beiersdorf_p12m_OSA_alerts_pre)

# COMMAND ----------

df_asda_beiersdorf_p12m_OSA_alerts_post = df_asda_beiersdorf_p12m_OSA_alerts.filter(df_asda_beiersdorf_p12m_OSA_alerts.SALES_DT >= '2024-01-23')

display(df_asda_beiersdorf_p12m_OSA_alerts_post)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Walmart Danone

# COMMAND ----------

database_name_walmart_danone = "retaillink_walmart_danoneusllc_us_dv"

df_price_walmart_danone = get_price(database_name_walmart_danone, spark)

display(df_price_walmart_danone)

# COMMAND ----------

sql_query_walmart_danone_p2m_OSA_alerts_raw = """
    SELECT
    Retail_Client,
    ORGANIZATION_UNIT_NUM,
    RETAILER_ITEM_ID,
    ALERT_MESSAGE_DESC,
    ALERT_TYPE_NM,
    ON_HAND_INVENTORY_QTY,
    LOST_SALES_AMT,
    SALES_DT
    from team_retail_alert_walmart_syn_us_im.alert_on_shelf_availability
    WHERE Retail_Client = 'walmart_danoneusllc_us'
    AND sales_dt >= '2024-05-01'
    AND sales_dt <= '2024-06-30'
    ORDER BY sales_dt, organization_unit_num, retailer_item_id
"""

df_walmart_danone_p2m_OSA_alerts_raw = spark.sql(sql_query_walmart_danone_p2m_OSA_alerts_raw)

display(df_walmart_danone_p2m_OSA_alerts_raw)

# COMMAND ----------

df_walmart_danone_p2m_OSA_alerts = df_walmart_danone_p2m_OSA_alerts_raw.join(df_price_walmart_danone, on=['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM', 'sales_dt'])

display(df_walmart_danone_p2m_OSA_alerts)

# COMMAND ----------

df_walmart_danone_p2m_OSA_alerts_by_pre = df_walmart_danone_p2m_OSA_alerts.filter(df_walmart_danone_p2m_OSA_alerts.SALES_DT < '2024-05-30')

display(df_walmart_danone_p2m_OSA_alerts_by_pre)

# COMMAND ----------

df_walmart_danone_p2m_OSA_alerts_by_post = df_walmart_danone_p2m_OSA_alerts.filter(df_walmart_danone_p2m_OSA_alerts.SALES_DT >= '2024-05-30')

display(df_walmart_danone_p2m_OSA_alerts_by_post)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Tesco Nestle

# COMMAND ----------

database_name_tesco_nestle = "tescopartnertoolkit_tesco_nestlecore_uk_dv"

df_price_tesco_nestle = get_price(database_name_tesco_nestle, spark)

display(df_price_tesco_nestle)

# COMMAND ----------

sql_query_tesco_nestle_p1w_legacy_OSA_alerts_raw = """
    SELECT
    Retail_Client,
    ORGANIZATION_UNIT_NUM,
    RETAILER_ITEM_ID,
    ALERT_MESSAGE_DESC,
    ALERT_TYPE_NM,
    ON_HAND_INVENTORY_QTY,
    LOST_SALES_AMT,
    SALES_DT
    from team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
    WHERE Retail_Client = 'tesco_nestlecore_uk'
    AND sales_dt >= '2024-07-29'
    AND sales_dt <= '2024-08-06'
    ORDER BY sales_dt, organization_unit_num, retailer_item_id
"""

df_tesco_nestle_p1w_legacy_OSA_alerts_raw = spark.sql(sql_query_tesco_nestle_p1w_legacy_OSA_alerts_raw)

display(df_tesco_nestle_p1w_legacy_OSA_alerts_raw)

# COMMAND ----------

df_tesco_nestle_p1w_legacy_OSA_alerts = df_tesco_nestle_p1w_legacy_OSA_alerts_raw.join(df_price_tesco_nestle, on=['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM', 'sales_dt'])

display(df_tesco_nestle_p1w_legacy_OSA_alerts)

# COMMAND ----------

sql_query_tesco_nestle_p1w_next_gen_OSA_alerts = """
    SELECT
    Retailer,
    Client,
    RETAILER_ITEM_ID,
    ORGANIZATION_UNIT_NUM,
    ALERT_ID,
    ID,
LAST_DATE_INCLUDED,
LAST_OBS,
LAST_RATE,
LAST_LOST_SALES,
N_DAYS_LAST_SALE,
PROB,
ZSCORE,
R2,
RMSE,
PRICE,
INVENTORY,
LOST_SALES_VALUE,
ISSUE,
DATE_GENERATED,
ALERT_TYPE
    from RETAIL_FORECAST_ENGINE.gen2_field_test_alerts_osa_only
    WHERE Retailer = 'tesco'
    AND Client = 'nestlecore'
    AND LAST_DATE_INCLUDED >= '2024-07-29'
    AND LAST_DATE_INCLUDED <= '2024-08-06'
    and PROB <= 0.2
    ORDER BY LAST_DATE_INCLUDED, ORGANIZATION_UNIT_NUM, ID
"""

df_tesco_nestle_p1w_next_gen_OSA_alerts = spark.sql(sql_query_tesco_nestle_p1w_next_gen_OSA_alerts)

display(df_tesco_nestle_p1w_next_gen_OSA_alerts)

# COMMAND ----------

sql_query_tesco_nestle_p1w_next_gen_OSA_alerts_alt = """
    SELECT
    Retailer,
    Client,
    RETAILER_ITEM_ID,
    ORGANIZATION_UNIT_NUM,
    ALERT_ID,
    ID,
LAST_DATE_INCLUDED,
LAST_OBS,
LAST_RATE,
LAST_LOST_SALES,
N_DAYS_LAST_SALE,
PROB,
ZSCORE,
R2,
RMSE,
PRICE,
INVENTORY,
LOST_SALES_VALUE,
ISSUE,
DATE_GENERATED,
ALERT_TYPE
    from RETAIL_FORECAST_ENGINE.gen2_field_test_alerts_osa_only
    WHERE Retailer = 'tesco'
    AND Client = 'nestlecore'
    AND LAST_DATE_INCLUDED >= '2024-07-29'
    AND LAST_DATE_INCLUDED <= '2024-08-06'
    and PROB <= 0.5
    ORDER BY LAST_DATE_INCLUDED, ORGANIZATION_UNIT_NUM, ID
"""

df_tesco_nestle_p1w_next_gen_OSA_alerts_alt = spark.sql(sql_query_tesco_nestle_p1w_next_gen_OSA_alerts_alt)

display(df_tesco_nestle_p1w_next_gen_OSA_alerts_alt)

# COMMAND ----------

sql_query_tesco_nestle_legacy_OSA_alerts_by_prob_raw_test2 = """
    SELECT
    Retail_Client,
    ORGANIZATION_UNIT_NUM,
    RETAILER_ITEM_ID,
    ALERT_MESSAGE_DESC,
    ALERT_TYPE_NM,
    ON_HAND_INVENTORY_QTY,
    LOST_SALES_AMT,
    SALES_DT
    from team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
    WHERE Retail_Client = 'tesco_nestlecore_uk'
    AND sales_dt >= '2024-08-06'
    AND sales_dt <= '2024-08-07'
    ORDER BY sales_dt, organization_unit_num, retailer_item_id
"""

df_tesco_nestle_legacy_OSA_alerts_by_prob_raw_test2 = spark.sql(sql_query_tesco_nestle_legacy_OSA_alerts_by_prob_raw_test2)

df_tesco_nestle_legacy_OSA_alerts_by_prob_raw_test2.count()

# COMMAND ----------

sql_query_tesco_nestle_legacy_OSA_alerts_by_prob_raw = """
    SELECT
    Retail_Client,
    ORGANIZATION_UNIT_NUM,
    RETAILER_ITEM_ID,
    ALERT_MESSAGE_DESC,
    ALERT_TYPE_NM,
    ON_HAND_INVENTORY_QTY,
    LOST_SALES_AMT,
    SALES_DT
    from team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
    WHERE Retail_Client = 'tesco_nestlecore_uk'
    AND sales_dt >= '2024-08-05'
    AND sales_dt <= '2024-08-08'
    ORDER BY sales_dt, organization_unit_num, retailer_item_id
"""

df_tesco_nestle_legacy_OSA_alerts_by_prob_raw = spark.sql(sql_query_tesco_nestle_legacy_OSA_alerts_by_prob_raw)

display(df_tesco_nestle_legacy_OSA_alerts_by_prob_raw)

# COMMAND ----------

df_tesco_nestle_legacy_OSA_alerts_by_prob_raw.count()

# COMMAND ----------

sql_query_tesco_nestle_legacy_OSA_alert_stats = """
    SELECT
    Retailer,
    Client,
    RETAILER_ITEM_ID,
    ORGANIZATION_UNIT_NUM,
LAST_DATE_INCLUDED as SALES_DT,
LAST_OBS,
LAST_RATE,
LAST_LOST_SALES,
N_DAYS_LAST_SALE,
PROB,
ZSCORE,
R2,
RMSE,
PRICE,
INVENTORY,
LOST_SALES_VALUE,
ISSUE,
DATE_GENERATED,
ALERT_TYPE
    from RETAIL_FORECAST_ENGINE.gen2_field_test_alerts
    WHERE Retailer = 'tesco'
    AND Client = 'nestlecore'
    AND LAST_DATE_INCLUDED >= '2024-08-05'
    AND LAST_DATE_INCLUDED <= '2024-08-07'
    and PROB <= 1
    ORDER BY LAST_DATE_INCLUDED, ORGANIZATION_UNIT_NUM, ID
"""

df_tesco_nestle_legacy_OSA_alert_stats = spark.sql(sql_query_tesco_nestle_legacy_OSA_alert_stats)

display(df_tesco_nestle_legacy_OSA_alert_stats)

# COMMAND ----------

df_tesco_nestle_legacy_OSA_alerts_by_prob = df_tesco_nestle_legacy_OSA_alerts_by_prob_raw.join(df_tesco_nestle_legacy_OSA_alert_stats, on=['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM', 'SALES_DT'])

display(df_tesco_nestle_legacy_OSA_alerts_by_prob)

# COMMAND ----------

df_tesco_nestle_legacy_OSA_alerts_by_prob.count()

# COMMAND ----------

# sql_query_tesco_nestle_test_stats = """
#     SELECT
#     Retailer,
#     Client,
#     RETAILER_ITEM_ID,
#     ORGANIZATION_UNIT_NUM,
# LAST_DATE_INCLUDED,
# LAST_OBS,
# LAST_RATE,
# LAST_LOST_SALES,
# N_DAYS_LAST_SALE,
# PROB,
# ZSCORE,
# R2,
# RMSE,
# PRICE,
# INVENTORY,
# LOST_SALES_VALUE,
# ISSUE,
# DATE_GENERATED as SALES_DT,
# ALERT_TYPE
#     from RETAIL_FORECAST_ENGINE.gen2_field_test_alerts
#     WHERE Retailer = 'tesco'
#     AND Client = 'nestlecore'
#     AND LAST_DATE_INCLUDED >= '2024-08-04'
#     AND LAST_DATE_INCLUDED <= '2024-08-09'
#     and PROB <= 1
#     ORDER BY LAST_DATE_INCLUDED, ORGANIZATION_UNIT_NUM, ID
# """

# df_tesco_nestle_test_stats = spark.sql(sql_query_tesco_nestle_test_stats)

# df_tesco_nestle_test = df_tesco_nestle_legacy_OSA_alerts_by_prob_raw.join(df_tesco_nestle_test_stats, on=['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM', 'SALES_DT'])

# df_tesco_nestle_test.count()

# COMMAND ----------


