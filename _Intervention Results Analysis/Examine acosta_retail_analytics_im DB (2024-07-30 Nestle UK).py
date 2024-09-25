# Databricks notebook source
import datetime
import pyspark.sql.functions as pyf

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Morrisons

# COMMAND ----------

sql_query_morrisons_nestle_2024_alerts = """
    SELECT * FROM team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
    WHERE Retail_Client = 'morrisons_nestlecore_uk'
    AND sales_dt >= '2024-01-01'
    AND sales_dt <= '2024-06-30'
    ORDER BY sales_dt, organization_unit_num, retailer_item_id
"""

df_morrisons_nestle_2024_alerts = spark.sql(sql_query_morrisons_nestle_2024_alerts)

df_morrisons_nestle_2024_alerts = df_morrisons_nestle_2024_alerts.withColumn('sales_dt_1', pyf.expr("date_add(sales_dt, 1)"))
df_morrisons_nestle_2024_alerts = df_morrisons_nestle_2024_alerts.withColumn('sales_dt_2', pyf.expr("date_add(sales_dt, 2)"))
df_morrisons_nestle_2024_alerts = df_morrisons_nestle_2024_alerts.withColumn('sales_dt_3', pyf.expr("date_add(sales_dt, 3)"))
df_morrisons_nestle_2024_alerts = df_morrisons_nestle_2024_alerts.withColumn('sales_dt_4', pyf.expr("date_add(sales_dt, 4)"))

display(df_morrisons_nestle_2024_alerts)

# COMMAND ----------

df_morrisons_nestle_2024_les_osa_alerts = df_morrisons_nestle_2024_alerts.filter(df_morrisons_nestle_2024_alerts.ALERT_TYPE_NM == "LowExpectedSales")

df_morrisons_nestle_2024_les_osa_alerts.count()

# COMMAND ----------

sql_query_morrisons_nestle_measured_rep_responses_to_2024_alerts = """
    select * from acosta_retail_analytics_im.ds_intervention_summary
    where
    mdm_country_id = 30 -- UK
    and
    mdm_client_id = 16320	-- Nestle UK
    and
    mdm_holding_id = 3257 -- AcostaRetailUK
    and
    mdm_banner_id = 7744 -- Morrisons
    and
    objective_typ = 'DLA'
    and
    is_complete = 'true'
    and
    call_date >= '2024-01-01'
    and
    call_date <= '2024-07-04'
    ORDER BY call_date, epos_organization_unit_num, epos_retailer_item_id
"""

df_morrisons_nestle_measured_rep_responses_to_2024_alerts = spark.sql(sql_query_morrisons_nestle_measured_rep_responses_to_2024_alerts)

display(df_morrisons_nestle_measured_rep_responses_to_2024_alerts)

# COMMAND ----------

df_morrisons_nestle_measured_rep_responses_to_2024_alerts.count()

# COMMAND ----------

df_morrisons_combined_1 = df_morrisons_nestle_2024_alerts.join(
  df_morrisons_nestle_measured_rep_responses_to_2024_alerts,
  (df_morrisons_nestle_2024_alerts.ORGANIZATION_UNIT_NUM == df_morrisons_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_morrisons_nestle_2024_alerts.RETAILER_ITEM_ID == df_morrisons_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_morrisons_nestle_2024_alerts.sales_dt_1 == df_morrisons_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_morrisons_combined_1.count())

# COMMAND ----------

df_morrisons_combined_2 = df_morrisons_nestle_2024_alerts.join(
  df_morrisons_nestle_measured_rep_responses_to_2024_alerts,
  (df_morrisons_nestle_2024_alerts.ORGANIZATION_UNIT_NUM == df_morrisons_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_morrisons_nestle_2024_alerts.RETAILER_ITEM_ID == df_morrisons_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_morrisons_nestle_2024_alerts.sales_dt_2 == df_morrisons_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_morrisons_combined_2.count())

# COMMAND ----------

df_morrisons_combined_3 = df_morrisons_nestle_2024_alerts.join(
  df_morrisons_nestle_measured_rep_responses_to_2024_alerts,
  (df_morrisons_nestle_2024_alerts.ORGANIZATION_UNIT_NUM == df_morrisons_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_morrisons_nestle_2024_alerts.RETAILER_ITEM_ID == df_morrisons_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_morrisons_nestle_2024_alerts.sales_dt_3 == df_morrisons_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_morrisons_combined_3.count())

# COMMAND ----------

df_morrisons_combined_4 = df_morrisons_nestle_2024_alerts.join(
  df_morrisons_nestle_measured_rep_responses_to_2024_alerts,
  (df_morrisons_nestle_2024_alerts.ORGANIZATION_UNIT_NUM == df_morrisons_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_morrisons_nestle_2024_alerts.RETAILER_ITEM_ID == df_morrisons_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_morrisons_nestle_2024_alerts.sales_dt_4 == df_morrisons_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_morrisons_combined_4.count())

# COMMAND ----------

df_morrisons_les_osa_combined_1 = df_morrisons_nestle_2024_les_osa_alerts.join(
  df_morrisons_nestle_measured_rep_responses_to_2024_alerts,
  (df_morrisons_nestle_2024_les_osa_alerts.ORGANIZATION_UNIT_NUM == df_morrisons_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_morrisons_nestle_2024_les_osa_alerts.RETAILER_ITEM_ID == df_morrisons_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_morrisons_nestle_2024_les_osa_alerts.sales_dt_1 == df_morrisons_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_morrisons_les_osa_combined_1.count())

# COMMAND ----------

df_morrisons_les_osa_combined_2 = df_morrisons_nestle_2024_les_osa_alerts.join(
  df_morrisons_nestle_measured_rep_responses_to_2024_alerts,
  (df_morrisons_nestle_2024_les_osa_alerts.ORGANIZATION_UNIT_NUM == df_morrisons_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_morrisons_nestle_2024_les_osa_alerts.RETAILER_ITEM_ID == df_morrisons_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_morrisons_nestle_2024_les_osa_alerts.sales_dt_2 == df_morrisons_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_morrisons_les_osa_combined_2.count())

# COMMAND ----------

df_morrisons_les_osa_combined_3 = df_morrisons_nestle_2024_les_osa_alerts.join(
  df_morrisons_nestle_measured_rep_responses_to_2024_alerts,
  (df_morrisons_nestle_2024_les_osa_alerts.ORGANIZATION_UNIT_NUM == df_morrisons_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_morrisons_nestle_2024_les_osa_alerts.RETAILER_ITEM_ID == df_morrisons_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_morrisons_nestle_2024_les_osa_alerts.sales_dt_3 == df_morrisons_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_morrisons_les_osa_combined_3.count())

# COMMAND ----------

display(df_morrisons_combined_2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Asda

# COMMAND ----------

sql_query_asda_nestle_2024_alerts = """
    SELECT * FROM team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
    WHERE Retail_Client = 'asda_nestlecore_uk'
    AND sales_dt >= '2024-01-01'
    AND sales_dt <= '2024-06-30'
    ORDER BY sales_dt, organization_unit_num, retailer_item_id
"""

df_asda_nestle_2024_alerts = spark.sql(sql_query_asda_nestle_2024_alerts)

df_asda_nestle_2024_alerts = df_asda_nestle_2024_alerts.withColumn('sales_dt_1', pyf.expr("date_add(sales_dt, 1)"))
df_asda_nestle_2024_alerts = df_asda_nestle_2024_alerts.withColumn('sales_dt_2', pyf.expr("date_add(sales_dt, 2)"))
df_asda_nestle_2024_alerts = df_asda_nestle_2024_alerts.withColumn('sales_dt_3', pyf.expr("date_add(sales_dt, 3)"))
df_asda_nestle_2024_alerts = df_asda_nestle_2024_alerts.withColumn('sales_dt_4', pyf.expr("date_add(sales_dt, 4)"))

display(df_asda_nestle_2024_alerts)

# COMMAND ----------

df_asda_nestle_2024_les_osa_alerts = df_asda_nestle_2024_alerts.filter(df_asda_nestle_2024_alerts.ALERT_TYPE_NM == "LowExpectedSales")

df_asda_nestle_2024_les_osa_alerts.count()

# COMMAND ----------

sql_query_asda_nestle_measured_rep_responses_to_2024_alerts = """
    select * from acosta_retail_analytics_im.ds_intervention_summary
    where
    mdm_country_id = 30 -- UK
    and
    mdm_client_id = 16320	-- Nestle UK
    and
    mdm_holding_id = 3257 -- AcostaRetailUK
    and
    mdm_banner_id = 7743 -- Asda
    and
    objective_typ = 'DLA'
    and
    is_complete = 'TRUE'
    and
    call_date >= '2024-01-01'
    and
    call_date <= '2024-07-04'
    ORDER BY call_date, epos_organization_unit_num, epos_retailer_item_id
"""

df_asda_nestle_measured_rep_responses_to_2024_alerts = spark.sql(sql_query_asda_nestle_measured_rep_responses_to_2024_alerts)

display(df_asda_nestle_measured_rep_responses_to_2024_alerts)

# COMMAND ----------

df_asda_nestle_measured_rep_responses_to_2024_alerts.count()

# COMMAND ----------

df_asda_combined_1 = df_asda_nestle_2024_alerts.join(
  df_asda_nestle_measured_rep_responses_to_2024_alerts,
  (df_asda_nestle_2024_alerts.ORGANIZATION_UNIT_NUM == df_asda_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_asda_nestle_2024_alerts.RETAILER_ITEM_ID == df_asda_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_asda_nestle_2024_alerts.sales_dt_1 == df_asda_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_asda_combined_1.count())

# COMMAND ----------

df_asda_combined_2 = df_asda_nestle_2024_alerts.join(
  df_asda_nestle_measured_rep_responses_to_2024_alerts,
  (df_asda_nestle_2024_alerts.ORGANIZATION_UNIT_NUM == df_asda_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_asda_nestle_2024_alerts.RETAILER_ITEM_ID == df_asda_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_asda_nestle_2024_alerts.sales_dt_2 == df_asda_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_asda_combined_2.count())

# COMMAND ----------

df_asda_combined_3 = df_asda_nestle_2024_alerts.join(
  df_asda_nestle_measured_rep_responses_to_2024_alerts,
  (df_asda_nestle_2024_alerts.ORGANIZATION_UNIT_NUM == df_asda_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_asda_nestle_2024_alerts.RETAILER_ITEM_ID == df_asda_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_asda_nestle_2024_alerts.sales_dt_3 == df_asda_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_asda_combined_3.count())

# COMMAND ----------

df_asda_combined_4 = df_asda_nestle_2024_alerts.join(
  df_asda_nestle_measured_rep_responses_to_2024_alerts,
  (df_asda_nestle_2024_alerts.ORGANIZATION_UNIT_NUM == df_asda_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_asda_nestle_2024_alerts.RETAILER_ITEM_ID == df_asda_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_asda_nestle_2024_alerts.sales_dt_4 == df_asda_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_asda_combined_4.count())

# COMMAND ----------

display(df_asda_combined_2)

# COMMAND ----------

df_asda_les_osa_combined_1 = df_asda_nestle_2024_les_osa_alerts.join(
  df_asda_nestle_measured_rep_responses_to_2024_alerts,
  (df_asda_nestle_2024_les_osa_alerts.ORGANIZATION_UNIT_NUM == df_asda_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_asda_nestle_2024_les_osa_alerts.RETAILER_ITEM_ID == df_asda_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_asda_nestle_2024_les_osa_alerts.sales_dt_1 == df_asda_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_asda_les_osa_combined_1.count())

# COMMAND ----------

df_asda_les_osa_combined_2 = df_asda_nestle_2024_les_osa_alerts.join(
  df_asda_nestle_measured_rep_responses_to_2024_alerts,
  (df_asda_nestle_2024_les_osa_alerts.ORGANIZATION_UNIT_NUM == df_asda_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_asda_nestle_2024_les_osa_alerts.RETAILER_ITEM_ID == df_asda_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_asda_nestle_2024_les_osa_alerts.sales_dt_2 == df_asda_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_asda_les_osa_combined_2.count())

# COMMAND ----------

df_asda_les_osa_combined_3 = df_asda_nestle_2024_les_osa_alerts.join(
  df_asda_nestle_measured_rep_responses_to_2024_alerts,
  (df_asda_nestle_2024_les_osa_alerts.ORGANIZATION_UNIT_NUM == df_asda_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_asda_nestle_2024_les_osa_alerts.RETAILER_ITEM_ID == df_asda_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_asda_nestle_2024_les_osa_alerts.sales_dt_3 == df_asda_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_asda_les_osa_combined_3.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Sainsburys

# COMMAND ----------

sql_query_sainsburys_nestle_2024_alerts = """
    SELECT * FROM team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
    WHERE Retail_Client = 'sainsburys_nestlecore_uk'
    AND sales_dt >= '2024-01-01'
    AND sales_dt <= '2024-06-30'
    ORDER BY sales_dt, organization_unit_num, retailer_item_id
"""

df_sainsburys_nestle_2024_alerts = spark.sql(sql_query_sainsburys_nestle_2024_alerts)

df_sainsburys_nestle_2024_alerts = df_sainsburys_nestle_2024_alerts.withColumn('sales_dt_1', pyf.expr("date_add(sales_dt, 1)"))
df_sainsburys_nestle_2024_alerts = df_sainsburys_nestle_2024_alerts.withColumn('sales_dt_2', pyf.expr("date_add(sales_dt, 2)"))
df_sainsburys_nestle_2024_alerts = df_sainsburys_nestle_2024_alerts.withColumn('sales_dt_3', pyf.expr("date_add(sales_dt, 3)"))
df_sainsburys_nestle_2024_alerts = df_sainsburys_nestle_2024_alerts.withColumn('sales_dt_4', pyf.expr("date_add(sales_dt, 4)"))
df_sainsburys_nestle_2024_alerts = df_sainsburys_nestle_2024_alerts.withColumn('sales_dt_5', pyf.expr("date_add(sales_dt, 5)"))

display(df_sainsburys_nestle_2024_alerts)

# COMMAND ----------

df_sainsburys_nestle_2024_les_osa_alerts = df_sainsburys_nestle_2024_alerts.filter(df_sainsburys_nestle_2024_alerts.ALERT_TYPE_NM == "LowExpectedSales")

df_sainsburys_nestle_2024_les_osa_alerts.count()

# COMMAND ----------

sql_query_sainsburys_nestle_measured_rep_responses_to_2024_alerts = """
    select * from acosta_retail_analytics_im.ds_intervention_summary
    where
    mdm_country_id = 30 -- UK
    and
    mdm_client_id = 16320	-- Nestle UK
    and
    mdm_holding_id = 3257 -- AcostaRetailUK
    and
    mdm_banner_id = 7745 -- Sainsburys
    and
    objective_typ = 'DLA'
    and
    is_complete = 'TRUE'
    and
    call_date >= '2024-01-01'
    and
    call_date <= '2024-07-05'
    ORDER BY call_date, epos_organization_unit_num, epos_retailer_item_id
"""

df_sainsburys_nestle_measured_rep_responses_to_2024_alerts = spark.sql(sql_query_sainsburys_nestle_measured_rep_responses_to_2024_alerts)

display(df_sainsburys_nestle_measured_rep_responses_to_2024_alerts)

# COMMAND ----------

df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.count()

# COMMAND ----------

df_sainsburys_combined_1 = df_sainsburys_nestle_2024_alerts.join(
  df_sainsburys_nestle_measured_rep_responses_to_2024_alerts,
  (df_sainsburys_nestle_2024_alerts.ORGANIZATION_UNIT_NUM == df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_sainsburys_nestle_2024_alerts.RETAILER_ITEM_ID == df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_sainsburys_nestle_2024_alerts.sales_dt_1 == df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_sainsburys_combined_1.count())

# COMMAND ----------

df_sainsburys_combined_2 = df_sainsburys_nestle_2024_alerts.join(
  df_sainsburys_nestle_measured_rep_responses_to_2024_alerts,
  (df_sainsburys_nestle_2024_alerts.ORGANIZATION_UNIT_NUM == df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_sainsburys_nestle_2024_alerts.RETAILER_ITEM_ID == df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_sainsburys_nestle_2024_alerts.sales_dt_2 == df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_sainsburys_combined_2.count())

# COMMAND ----------

df_sainsburys_combined_3 = df_sainsburys_nestle_2024_alerts.join(
  df_sainsburys_nestle_measured_rep_responses_to_2024_alerts,
  (df_sainsburys_nestle_2024_alerts.ORGANIZATION_UNIT_NUM == df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_sainsburys_nestle_2024_alerts.RETAILER_ITEM_ID == df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_sainsburys_nestle_2024_alerts.sales_dt_3 == df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_sainsburys_combined_3.count())

# COMMAND ----------

df_sainsburys_combined_4 = df_sainsburys_nestle_2024_alerts.join(
  df_sainsburys_nestle_measured_rep_responses_to_2024_alerts,
  (df_sainsburys_nestle_2024_alerts.ORGANIZATION_UNIT_NUM == df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_sainsburys_nestle_2024_alerts.RETAILER_ITEM_ID == df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_sainsburys_nestle_2024_alerts.sales_dt_4 == df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_sainsburys_combined_4.count())

# COMMAND ----------

df_sainsburys_combined_5 = df_sainsburys_nestle_2024_alerts.join(
  df_sainsburys_nestle_measured_rep_responses_to_2024_alerts,
  (df_sainsburys_nestle_2024_alerts.ORGANIZATION_UNIT_NUM == df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_sainsburys_nestle_2024_alerts.RETAILER_ITEM_ID == df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_sainsburys_nestle_2024_alerts.sales_dt_5 == df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_sainsburys_combined_5.count())

# COMMAND ----------

display(df_sainsburys_combined_4)

# COMMAND ----------

df_sainsburys_les_osa_combined_1 = df_sainsburys_nestle_2024_les_osa_alerts.join(
  df_sainsburys_nestle_measured_rep_responses_to_2024_alerts,
  (df_sainsburys_nestle_2024_les_osa_alerts.ORGANIZATION_UNIT_NUM == df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_sainsburys_nestle_2024_les_osa_alerts.RETAILER_ITEM_ID == df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_sainsburys_nestle_2024_les_osa_alerts.sales_dt_1 == df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_sainsburys_les_osa_combined_1.count())

# COMMAND ----------

df_sainsburys_les_osa_combined_2 = df_sainsburys_nestle_2024_les_osa_alerts.join(
  df_sainsburys_nestle_measured_rep_responses_to_2024_alerts,
  (df_sainsburys_nestle_2024_les_osa_alerts.ORGANIZATION_UNIT_NUM == df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_sainsburys_nestle_2024_les_osa_alerts.RETAILER_ITEM_ID == df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_sainsburys_nestle_2024_les_osa_alerts.sales_dt_2 == df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_sainsburys_les_osa_combined_2.count())

# COMMAND ----------

df_sainsburys_les_osa_combined_3 = df_sainsburys_nestle_2024_les_osa_alerts.join(
  df_sainsburys_nestle_measured_rep_responses_to_2024_alerts,
  (df_sainsburys_nestle_2024_les_osa_alerts.ORGANIZATION_UNIT_NUM == df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_sainsburys_nestle_2024_les_osa_alerts.RETAILER_ITEM_ID == df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_sainsburys_nestle_2024_les_osa_alerts.sales_dt_3 == df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_sainsburys_les_osa_combined_3.count())

# COMMAND ----------

df_sainsburys_les_osa_combined_4 = df_sainsburys_nestle_2024_les_osa_alerts.join(
  df_sainsburys_nestle_measured_rep_responses_to_2024_alerts,
  (df_sainsburys_nestle_2024_les_osa_alerts.ORGANIZATION_UNIT_NUM == df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_sainsburys_nestle_2024_les_osa_alerts.RETAILER_ITEM_ID == df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_sainsburys_nestle_2024_les_osa_alerts.sales_dt_4 == df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_sainsburys_les_osa_combined_4.count())

# COMMAND ----------

df_sainsburys_les_osa_combined_5 = df_sainsburys_nestle_2024_les_osa_alerts.join(
  df_sainsburys_nestle_measured_rep_responses_to_2024_alerts,
  (df_sainsburys_nestle_2024_les_osa_alerts.ORGANIZATION_UNIT_NUM == df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_sainsburys_nestle_2024_les_osa_alerts.RETAILER_ITEM_ID == df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_sainsburys_nestle_2024_les_osa_alerts.sales_dt_5 == df_sainsburys_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_sainsburys_les_osa_combined_5.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Tesco

# COMMAND ----------

sql_query_tesco_nestle_2024_alerts = """
    SELECT * FROM team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
    WHERE Retail_Client = 'tesco_nestlecore_uk'
    AND sales_dt >= '2024-01-01'
    AND sales_dt <= '2024-06-30'
    ORDER BY sales_dt, organization_unit_num, retailer_item_id
"""

df_tesco_nestle_2024_alerts = spark.sql(sql_query_tesco_nestle_2024_alerts)

df_tesco_nestle_2024_alerts = df_tesco_nestle_2024_alerts.withColumn('sales_dt_1', pyf.expr("date_add(sales_dt, 1)"))
df_tesco_nestle_2024_alerts = df_tesco_nestle_2024_alerts.withColumn('sales_dt_2', pyf.expr("date_add(sales_dt, 2)"))
df_tesco_nestle_2024_alerts = df_tesco_nestle_2024_alerts.withColumn('sales_dt_3', pyf.expr("date_add(sales_dt, 3)"))
df_tesco_nestle_2024_alerts = df_tesco_nestle_2024_alerts.withColumn('sales_dt_4', pyf.expr("date_add(sales_dt, 4)"))
df_tesco_nestle_2024_alerts = df_tesco_nestle_2024_alerts.withColumn('sales_dt_5', pyf.expr("date_add(sales_dt, 5)"))

display(df_sainsburys_nestle_2024_alerts)

# COMMAND ----------

df_tesco_nestle_2024_les_osa_alerts = df_tesco_nestle_2024_alerts.filter(df_tesco_nestle_2024_alerts.ALERT_TYPE_NM == "LowExpectedSales")

df_tesco_nestle_2024_les_osa_alerts.count()

# COMMAND ----------

sql_query_tesco_nestle_measured_rep_responses_to_2024_alerts_export = """
    select * from acosta_retail_analytics_im.ds_intervention_summary
    where
    mdm_country_id = 30 -- UK
    and
    mdm_client_id = 16320	-- Nestle UK
    and
    mdm_holding_id = 3257 -- AcostaRetailUK
    and
    mdm_banner_id = 7746 -- Tesco
    and
    objective_typ = 'DLA'
    and
    is_complete = 'TRUE'
    and
    call_date >= '2024-01-01'
    and
    call_date <= '2024-07-02'
    ORDER BY call_date, epos_organization_unit_num, epos_retailer_item_id
"""

df_tesco_nestle_measured_rep_responses_to_2024_alerts_export = spark.sql(sql_query_tesco_nestle_measured_rep_responses_to_2024_alerts_export)

display(df_tesco_nestle_measured_rep_responses_to_2024_alerts_export)

# COMMAND ----------

df_tesco_nestle_measured_rep_responses_to_2024_alerts_export.count()

# COMMAND ----------

sql_query_tesco_nestle_measured_rep_responses_to_2024_alerts = """
    select * from acosta_retail_analytics_im.ds_intervention_summary
    where
    mdm_country_id = 30 -- UK
    and
    mdm_client_id = 16320	-- Nestle UK
    and
    mdm_holding_id = 3257 -- AcostaRetailUK
    and
    mdm_banner_id = 7746 -- Tesco
    and
    objective_typ = 'DLA'
    and
    is_complete = 'TRUE'
    and
    call_date >= '2024-01-01'
    and
    call_date <= '2024-07-05'
    ORDER BY call_date, epos_organization_unit_num, epos_retailer_item_id
"""

df_tesco_nestle_measured_rep_responses_to_2024_alerts = spark.sql(sql_query_tesco_nestle_measured_rep_responses_to_2024_alerts)

display(df_tesco_nestle_measured_rep_responses_to_2024_alerts)

# COMMAND ----------

df_tesco_nestle_measured_rep_responses_to_2024_alerts.count()

# COMMAND ----------

df_tesco_combined_1 = df_tesco_nestle_2024_alerts.join(
  df_tesco_nestle_measured_rep_responses_to_2024_alerts,
  (df_tesco_nestle_2024_alerts.ORGANIZATION_UNIT_NUM == df_tesco_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_tesco_nestle_2024_alerts.RETAILER_ITEM_ID == df_tesco_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_tesco_nestle_2024_alerts.sales_dt_1 == df_tesco_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_tesco_combined_1.count())

# COMMAND ----------

df_tesco_combined_2 = df_tesco_nestle_2024_alerts.join(
  df_tesco_nestle_measured_rep_responses_to_2024_alerts,
  (df_tesco_nestle_2024_alerts.ORGANIZATION_UNIT_NUM == df_tesco_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_tesco_nestle_2024_alerts.RETAILER_ITEM_ID == df_tesco_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_tesco_nestle_2024_alerts.sales_dt_2 == df_tesco_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_tesco_combined_2.count())

# COMMAND ----------

df_tesco_combined_3 = df_tesco_nestle_2024_alerts.join(
  df_tesco_nestle_measured_rep_responses_to_2024_alerts,
  (df_tesco_nestle_2024_alerts.ORGANIZATION_UNIT_NUM == df_tesco_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_tesco_nestle_2024_alerts.RETAILER_ITEM_ID == df_tesco_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_tesco_nestle_2024_alerts.sales_dt_3 == df_tesco_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_tesco_combined_3.count())

# COMMAND ----------

df_tesco_combined_4 = df_tesco_nestle_2024_alerts.join(
  df_tesco_nestle_measured_rep_responses_to_2024_alerts,
  (df_tesco_nestle_2024_alerts.ORGANIZATION_UNIT_NUM == df_tesco_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_tesco_nestle_2024_alerts.RETAILER_ITEM_ID == df_tesco_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_tesco_nestle_2024_alerts.sales_dt_4 == df_tesco_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_tesco_combined_4.count())

# COMMAND ----------

df_tesco_combined_5 = df_tesco_nestle_2024_alerts.join(
  df_tesco_nestle_measured_rep_responses_to_2024_alerts,
  (df_tesco_nestle_2024_alerts.ORGANIZATION_UNIT_NUM == df_tesco_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_tesco_nestle_2024_alerts.RETAILER_ITEM_ID == df_tesco_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_tesco_nestle_2024_alerts.sales_dt_5 == df_tesco_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_tesco_combined_5.count())

# COMMAND ----------

display(df_tesco_combined_2)

# COMMAND ----------

df_tesco_les_osa_combined_1 = df_tesco_nestle_2024_les_osa_alerts.join(
  df_tesco_nestle_measured_rep_responses_to_2024_alerts,
  (df_tesco_nestle_2024_les_osa_alerts.ORGANIZATION_UNIT_NUM == df_tesco_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_tesco_nestle_2024_les_osa_alerts.RETAILER_ITEM_ID == df_tesco_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_tesco_nestle_2024_les_osa_alerts.sales_dt_1 == df_tesco_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_tesco_les_osa_combined_1.count())

# COMMAND ----------

df_tesco_les_osa_combined_2 = df_tesco_nestle_2024_les_osa_alerts.join(
  df_tesco_nestle_measured_rep_responses_to_2024_alerts,
  (df_tesco_nestle_2024_les_osa_alerts.ORGANIZATION_UNIT_NUM == df_tesco_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_tesco_nestle_2024_les_osa_alerts.RETAILER_ITEM_ID == df_tesco_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_tesco_nestle_2024_les_osa_alerts.sales_dt_2 == df_tesco_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_tesco_les_osa_combined_2.count())

# COMMAND ----------

df_tesco_les_osa_combined_3 = df_tesco_nestle_2024_les_osa_alerts.join(
  df_tesco_nestle_measured_rep_responses_to_2024_alerts,
  (df_tesco_nestle_2024_les_osa_alerts.ORGANIZATION_UNIT_NUM == df_tesco_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_tesco_nestle_2024_les_osa_alerts.RETAILER_ITEM_ID == df_tesco_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_tesco_nestle_2024_les_osa_alerts.sales_dt_3 == df_tesco_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_tesco_les_osa_combined_3.count())

# COMMAND ----------

df_tesco_les_osa_combined_4 = df_tesco_nestle_2024_les_osa_alerts.join(
  df_tesco_nestle_measured_rep_responses_to_2024_alerts,
  (df_tesco_nestle_2024_les_osa_alerts.ORGANIZATION_UNIT_NUM == df_tesco_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_tesco_nestle_2024_les_osa_alerts.RETAILER_ITEM_ID == df_tesco_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_tesco_nestle_2024_les_osa_alerts.sales_dt_4 == df_tesco_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_tesco_les_osa_combined_4.count())

# COMMAND ----------

df_tesco_les_osa_combined_5 = df_tesco_nestle_2024_les_osa_alerts.join(
  df_tesco_nestle_measured_rep_responses_to_2024_alerts,
  (df_tesco_nestle_2024_les_osa_alerts.ORGANIZATION_UNIT_NUM == df_tesco_nestle_measured_rep_responses_to_2024_alerts.epos_organization_unit_num) & (df_tesco_nestle_2024_les_osa_alerts.RETAILER_ITEM_ID == df_tesco_nestle_measured_rep_responses_to_2024_alerts.epos_retailer_item_id) & (df_tesco_nestle_2024_les_osa_alerts.sales_dt_5 == df_tesco_nestle_measured_rep_responses_to_2024_alerts.call_date), 'inner'
  )

print(df_tesco_les_osa_combined_5.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. All Retailers Combined

# COMMAND ----------

sql_query_nestle_measured_DLA_rep_responses = """
    select count(measurement_duration), sum(total_intervention_effect), sum(total_impact) from acosta_retail_analytics_im.ds_intervention_summary
    where
    mdm_country_id = 30 -- UK
    and
    mdm_client_id = 16320	-- Nestle UK
    and
    mdm_holding_id = 3257 -- AcostaRetailUK
    and
    objective_typ = 'DLA'
    and
    is_complete = 'TRUE'
    and
    call_date >= '2024-01-01'
    and
    call_date <= '2024-07-02'
"""

df_nestle_measured_DLA_rep_responses = spark.sql(sql_query_nestle_measured_DLA_rep_responses)

display(df_nestle_measured_DLA_rep_responses)

# COMMAND ----------

sql_query_nestle_measured_rep_responses = """
    select count(measurement_duration), sum(total_intervention_effect), sum(total_impact) from acosta_retail_analytics_im.ds_intervention_summary
    where
    mdm_country_id = 30 -- UK
    and
    mdm_client_id = 16320	-- Nestle UK
    and
    mdm_holding_id = 3257 -- AcostaRetailUK
    and
    is_complete = 'TRUE'
    and
    call_date >= '2024-01-01'
    and
    call_date <= '2024-07-02'
"""

df_nestle_measured_rep_responses = spark.sql(sql_query_nestle_measured_rep_responses)

display(df_nestle_measured_rep_responses)

# COMMAND ----------


