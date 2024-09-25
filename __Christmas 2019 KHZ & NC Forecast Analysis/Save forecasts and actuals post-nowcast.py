# Databricks notebook source
# MAGIC %md
# MAGIC ### 1. Asda Kraftheinz

# COMMAND ----------

# df1a = spark.sql("""
# select fc.sales_dt, fc.BASELINE_POS_ITEM_QTY, sales.pos_item_qty, sales.pos_amt, sales.on_hand_inventory_qty, hou.organization_unit_num, hri.RETAILER_ITEM_ID, sri.RETAILER_ITEM_DESC
# FROM asda_kraftheinz_uk_retail_alert_im.vw_drfe_forecast_baseline_unit as fc
# INNER JOIN asda_kraftheinz_uk_dv.link_epos_summary as les
#   ON fc.hub_organization_unit_hk = les.hub_organization_unit_hk AND fc.hub_retailer_item_hk = les.hub_retailer_item_hk AND fc.sales_dt = les.sales_dt
# INNER JOIN asda_kraftheinz_uk_dv.sat_link_epos_summary as sales
#   ON les.LINK_EPOS_SUMMARY_HK = sales.LINK_EPOS_SUMMARY_HK
# INNER JOIN asda_kraftheinz_uk_dv.hub_organization_unit as hou
#   ON hou.hub_organization_unit_hk = fc.hub_organization_unit_hk
# INNER JOIN asda_kraftheinz_uk_dv.hub_retailer_item as hri
#   ON fc.hub_retailer_item_hk = hri.hub_retailer_item_hk
# INNER JOIN asda_kraftheinz_uk_dv.sat_retailer_item as sri
#   ON fc.hub_retailer_item_hk = sri.hub_retailer_item_hk

# where fc.SALES_DT >= '2019-10-15'""")

# COMMAND ----------

# df1a.count()

# COMMAND ----------

# display(df1a)

# COMMAND ----------

# df1a.coalesce(1)\
#     .write.format('com.databricks.spark.csv')\
#     .option('header', 'true')\
#     .mode('overwrite')\
#     .save('/mnt/artifacts/hugh/asda-kraftheinz-post-2019-10-15-sales-new-predictions')

# COMMAND ----------

df1b = spark.sql("""
select fc.sales_dt, fc.BASELINE_POS_ITEM_QTY, sales.pos_item_qty, sales.pos_amt, sales.on_hand_inventory_qty, hou.organization_unit_num, hri.RETAILER_ITEM_ID, sri.RETAILER_ITEM_DESC
FROM asda_kraftheinz_uk_retail_alert_im.vw_drfe_forecast_baseline_unit as fc
INNER JOIN asda_kraftheinz_uk_dv.link_epos_summary as les
  ON fc.hub_organization_unit_hk = les.hub_organization_unit_hk AND fc.hub_retailer_item_hk = les.hub_retailer_item_hk AND fc.sales_dt = les.sales_dt
INNER JOIN asda_kraftheinz_uk_dv.sat_link_epos_summary as sales
  ON les.LINK_EPOS_SUMMARY_HK = sales.LINK_EPOS_SUMMARY_HK
INNER JOIN asda_kraftheinz_uk_dv.hub_organization_unit as hou
  ON hou.hub_organization_unit_hk = fc.hub_organization_unit_hk
INNER JOIN asda_kraftheinz_uk_dv.hub_retailer_item as hri
  ON fc.hub_retailer_item_hk = hri.hub_retailer_item_hk
INNER JOIN asda_kraftheinz_uk_dv.sat_retailer_item as sri
  ON fc.hub_retailer_item_hk = sri.hub_retailer_item_hk

where fc.SALES_DT >= '2019-12-01'""")

# COMMAND ----------

df1b.count()

# COMMAND ----------

display(df1b)

# COMMAND ----------

df1b.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save('/mnt/artifacts/hugh/asda-kraftheinz-post-2019-12-01-sales-new-predictions')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Asda NestleCereals

# COMMAND ----------

# df2a = spark.sql("""
# select fc.sales_dt, fc.BASELINE_POS_ITEM_QTY, sales.pos_item_qty, sales.pos_amt, sales.on_hand_inventory_qty, hou.organization_unit_num, hri.RETAILER_ITEM_ID, sri.RETAILER_ITEM_DESC
# FROM asda_nestlecereals_uk_retail_alert_im.vw_drfe_forecast_baseline_unit as fc
# INNER JOIN asda_nestlecereals_uk_dv.link_epos_summary as les
#   ON fc.hub_organization_unit_hk = les.hub_organization_unit_hk AND fc.hub_retailer_item_hk = les.hub_retailer_item_hk AND fc.sales_dt = les.sales_dt
# INNER JOIN asda_nestlecereals_uk_dv.sat_link_epos_summary as sales
#   ON les.LINK_EPOS_SUMMARY_HK = sales.LINK_EPOS_SUMMARY_HK
# INNER JOIN asda_nestlecereals_uk_dv.hub_organization_unit as hou
#   ON hou.hub_organization_unit_hk = fc.hub_organization_unit_hk
# INNER JOIN asda_nestlecereals_uk_dv.hub_retailer_item as hri
#   ON fc.hub_retailer_item_hk = hri.hub_retailer_item_hk
# INNER JOIN asda_nestlecereals_uk_dv.sat_retailer_item as sri
#   ON fc.hub_retailer_item_hk = sri.hub_retailer_item_hk

# where fc.SALES_DT >= '2019-10-15'""")

# COMMAND ----------

# df2a.count()

# COMMAND ----------

# display(df2a)

# COMMAND ----------

# df2a.coalesce(1)\
#     .write.format('com.databricks.spark.csv')\
#     .option('header', 'true')\
#     .mode('overwrite')\
#     .save('/mnt/artifacts/hugh/asda-nestlecereals-post-2019-10-15-sales-newer-predictions')

# COMMAND ----------

df2b = spark.sql("""
select fc.sales_dt, fc.BASELINE_POS_ITEM_QTY, sales.pos_item_qty, sales.pos_amt, sales.on_hand_inventory_qty, hou.organization_unit_num, hri.RETAILER_ITEM_ID, sri.RETAILER_ITEM_DESC
FROM asda_nestlecereals_uk_retail_alert_im.vw_drfe_forecast_baseline_unit as fc
INNER JOIN asda_nestlecereals_uk_dv.link_epos_summary as les
  ON fc.hub_organization_unit_hk = les.hub_organization_unit_hk AND fc.hub_retailer_item_hk = les.hub_retailer_item_hk AND fc.sales_dt = les.sales_dt
INNER JOIN asda_nestlecereals_uk_dv.sat_link_epos_summary as sales
  ON les.LINK_EPOS_SUMMARY_HK = sales.LINK_EPOS_SUMMARY_HK
INNER JOIN asda_nestlecereals_uk_dv.hub_organization_unit as hou
  ON hou.hub_organization_unit_hk = fc.hub_organization_unit_hk
INNER JOIN asda_nestlecereals_uk_dv.hub_retailer_item as hri
  ON fc.hub_retailer_item_hk = hri.hub_retailer_item_hk
INNER JOIN asda_nestlecereals_uk_dv.sat_retailer_item as sri
  ON fc.hub_retailer_item_hk = sri.hub_retailer_item_hk

where fc.SALES_DT >= '2019-12-01'""")

# COMMAND ----------

df2b.count()

# COMMAND ----------

# display(df2b)

# COMMAND ----------

df2b.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save('/mnt/artifacts/hugh/asda-nestlecereals-post-2019-12-01-sales-newer-predictions')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Tesco Kraftheinz

# COMMAND ----------

# df3a = spark.sql("""

# select fc.sales_dt, fc.BASELINE_POS_ITEM_QTY, sales.pos_item_qty, sales.pos_amt, sales.on_hand_inventory_qty, hou.organization_unit_num, hri.RETAILER_ITEM_ID, sri.RETAILER_ITEM_DESC
# FROM tesco_kraftheinz_uk_retail_alert_im.vw_drfe_forecast_baseline_unit as fc
# INNER JOIN tesco_kraftheinz_uk_dv.link_epos_summary as les
#   ON fc.hub_organization_unit_hk = les.hub_organization_unit_hk AND fc.hub_retailer_item_hk = les.hub_retailer_item_hk AND fc.sales_dt = les.sales_dt
# INNER JOIN tesco_kraftheinz_uk_dv.sat_link_epos_summary as sales
#   ON les.LINK_EPOS_SUMMARY_HK = sales.LINK_EPOS_SUMMARY_HK
# INNER JOIN tesco_kraftheinz_uk_dv.hub_organization_unit as hou
#   ON hou.hub_organization_unit_hk = fc.hub_organization_unit_hk
# INNER JOIN tesco_kraftheinz_uk_dv.hub_retailer_item as hri
#   ON fc.hub_retailer_item_hk = hri.hub_retailer_item_hk
# INNER JOIN tesco_kraftheinz_uk_dv.sat_retailer_item as sri
#   ON fc.hub_retailer_item_hk = sri.hub_retailer_item_hk
  
# where fc.SALES_DT >= '2019-10-15'""")

# COMMAND ----------

# df3a.count()

# COMMAND ----------

# display(df3a)

# COMMAND ----------

# df3a.coalesce(1)\
#     .write.format('com.databricks.spark.csv')\
#     .option('header', 'true')\
#     .mode('overwrite')\
#     .save('/mnt/artifacts/hugh/tesco-kraftheinz-post-2019-10-15-sales-new-predictions')

# COMMAND ----------

df3b = spark.sql("""

select fc.sales_dt, fc.BASELINE_POS_ITEM_QTY, sales.pos_item_qty, sales.pos_amt, sales.on_hand_inventory_qty, hou.organization_unit_num, hri.RETAILER_ITEM_ID, sri.RETAILER_ITEM_DESC
FROM tesco_kraftheinz_uk_retail_alert_im.vw_drfe_forecast_baseline_unit as fc
INNER JOIN tesco_kraftheinz_uk_dv.link_epos_summary as les
  ON fc.hub_organization_unit_hk = les.hub_organization_unit_hk AND fc.hub_retailer_item_hk = les.hub_retailer_item_hk AND fc.sales_dt = les.sales_dt
INNER JOIN tesco_kraftheinz_uk_dv.sat_link_epos_summary as sales
  ON les.LINK_EPOS_SUMMARY_HK = sales.LINK_EPOS_SUMMARY_HK
INNER JOIN tesco_kraftheinz_uk_dv.hub_organization_unit as hou
  ON hou.hub_organization_unit_hk = fc.hub_organization_unit_hk
INNER JOIN tesco_kraftheinz_uk_dv.hub_retailer_item as hri
  ON fc.hub_retailer_item_hk = hri.hub_retailer_item_hk
INNER JOIN tesco_kraftheinz_uk_dv.sat_retailer_item as sri
  ON fc.hub_retailer_item_hk = sri.hub_retailer_item_hk
  
where fc.SALES_DT >= '2019-12-01'""")

# COMMAND ----------

df3b.count()

# COMMAND ----------

# display(df3b)

# COMMAND ----------

df3b.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save('/mnt/artifacts/hugh/tesco-kraftheinz-post-2019-12-01-sales-new-predictions')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Tesco Nestlecereals

# COMMAND ----------

# df4a = spark.sql("""

# select fc.sales_dt, fc.BASELINE_POS_ITEM_QTY, sales.pos_item_qty, sales.pos_amt, sales.on_hand_inventory_qty, hou.organization_unit_num, hri.RETAILER_ITEM_ID, sri.RETAILER_ITEM_DESC
# FROM tesco_nestlecereals_uk_retail_alert_im.vw_drfe_forecast_baseline_unit as fc
# INNER JOIN tesco_nestlecereals_uk_dv.link_epos_summary as les
#   ON fc.hub_organization_unit_hk = les.hub_organization_unit_hk AND fc.hub_retailer_item_hk = les.hub_retailer_item_hk AND fc.sales_dt = les.sales_dt
# INNER JOIN tesco_nestlecereals_uk_dv.sat_link_epos_summary as sales
#   ON les.LINK_EPOS_SUMMARY_HK = sales.LINK_EPOS_SUMMARY_HK
# INNER JOIN tesco_nestlecereals_uk_dv.hub_organization_unit as hou
#   ON hou.hub_organization_unit_hk = fc.hub_organization_unit_hk
# INNER JOIN tesco_nestlecereals_uk_dv.hub_retailer_item as hri
#   ON fc.hub_retailer_item_hk = hri.hub_retailer_item_hk
# INNER JOIN tesco_nestlecereals_uk_dv.sat_retailer_item as sri
#   ON fc.hub_retailer_item_hk = sri.hub_retailer_item_hk
  
# where fc.SALES_DT >= '2019-10-15'""")

# COMMAND ----------

# display(df4a)

# COMMAND ----------

# df4a.count()

# COMMAND ----------

# df4a.coalesce(1)\
#     .write.format('com.databricks.spark.csv')\
#     .option('header', 'true')\
#     .mode('overwrite')\
#     .save('/mnt/artifacts/hugh/tesco-nestlecereals-post-2019-10-15-sales-new-predictions')

# COMMAND ----------

df4b = spark.sql("""

select fc.sales_dt, fc.BASELINE_POS_ITEM_QTY, sales.pos_item_qty, sales.pos_amt, sales.on_hand_inventory_qty, hou.organization_unit_num, hri.RETAILER_ITEM_ID, sri.RETAILER_ITEM_DESC
FROM tesco_nestlecereals_uk_retail_alert_im.vw_drfe_forecast_baseline_unit as fc
INNER JOIN tesco_nestlecereals_uk_dv.link_epos_summary as les
  ON fc.hub_organization_unit_hk = les.hub_organization_unit_hk AND fc.hub_retailer_item_hk = les.hub_retailer_item_hk AND fc.sales_dt = les.sales_dt
INNER JOIN tesco_nestlecereals_uk_dv.sat_link_epos_summary as sales
  ON les.LINK_EPOS_SUMMARY_HK = sales.LINK_EPOS_SUMMARY_HK
INNER JOIN tesco_nestlecereals_uk_dv.hub_organization_unit as hou
  ON hou.hub_organization_unit_hk = fc.hub_organization_unit_hk
INNER JOIN tesco_nestlecereals_uk_dv.hub_retailer_item as hri
  ON fc.hub_retailer_item_hk = hri.hub_retailer_item_hk
INNER JOIN tesco_nestlecereals_uk_dv.sat_retailer_item as sri
  ON fc.hub_retailer_item_hk = sri.hub_retailer_item_hk
  
where fc.SALES_DT >= '2019-12-01'""")

# COMMAND ----------

# display(df4b)

# COMMAND ----------

df4b.count()

# COMMAND ----------

df4b.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save('/mnt/artifacts/hugh/tesco-nestlecereals-post-2019-12-01-sales-new-predictions')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Morrisons Kraftheinz

# COMMAND ----------

# df5a = spark.sql("""

# select fc.sales_dt, fc.BASELINE_POS_ITEM_QTY, sales.pos_item_qty, sales.pos_amt, sales.on_hand_inventory_qty, hou.organization_unit_num, hri.RETAILER_ITEM_ID, sri.RETAILER_ITEM_DESC
# FROM morrisons_kraftheinz_uk_retail_alert_im.vw_drfe_forecast_baseline_unit as fc
# INNER JOIN morrisons_kraftheinz_uk_dv.link_epos_summary as les
#   ON fc.hub_organization_unit_hk = les.hub_organization_unit_hk AND fc.hub_retailer_item_hk = les.hub_retailer_item_hk AND fc.sales_dt = les.sales_dt
# INNER JOIN morrisons_kraftheinz_uk_dv.sat_link_epos_summary as sales
#   ON les.LINK_EPOS_SUMMARY_HK = sales.LINK_EPOS_SUMMARY_HK
# INNER JOIN morrisons_kraftheinz_uk_dv.hub_organization_unit as hou
#   ON hou.hub_organization_unit_hk = fc.hub_organization_unit_hk
# INNER JOIN morrisons_kraftheinz_uk_dv.hub_retailer_item as hri
#   ON fc.hub_retailer_item_hk = hri.hub_retailer_item_hk
# INNER JOIN morrisons_kraftheinz_uk_dv.sat_retailer_item as sri
#   ON fc.hub_retailer_item_hk = sri.hub_retailer_item_hk
  
# where fc.SALES_DT >= '2019-10-15'""")

# COMMAND ----------

# display(df5a)

# COMMAND ----------

# df5a.count()

# COMMAND ----------

# df5a.coalesce(1)\
#     .write.format('com.databricks.spark.csv')\
#     .option('header', 'true')\
#     .mode('overwrite')\
#     .save('/mnt/artifacts/hugh/morrisons-kraftheinz-post-2019-10-15-sales-new-predictions')

# COMMAND ----------

df5b = spark.sql("""

select fc.sales_dt, fc.BASELINE_POS_ITEM_QTY, sales.pos_item_qty, sales.pos_amt, sales.on_hand_inventory_qty, hou.organization_unit_num, hri.RETAILER_ITEM_ID, sri.RETAILER_ITEM_DESC
FROM morrisons_kraftheinz_uk_retail_alert_im.vw_drfe_forecast_baseline_unit as fc
INNER JOIN morrisons_kraftheinz_uk_dv.link_epos_summary as les
  ON fc.hub_organization_unit_hk = les.hub_organization_unit_hk AND fc.hub_retailer_item_hk = les.hub_retailer_item_hk AND fc.sales_dt = les.sales_dt
INNER JOIN morrisons_kraftheinz_uk_dv.sat_link_epos_summary as sales
  ON les.LINK_EPOS_SUMMARY_HK = sales.LINK_EPOS_SUMMARY_HK
INNER JOIN morrisons_kraftheinz_uk_dv.hub_organization_unit as hou
  ON hou.hub_organization_unit_hk = fc.hub_organization_unit_hk
INNER JOIN morrisons_kraftheinz_uk_dv.hub_retailer_item as hri
  ON fc.hub_retailer_item_hk = hri.hub_retailer_item_hk
INNER JOIN morrisons_kraftheinz_uk_dv.sat_retailer_item as sri
  ON fc.hub_retailer_item_hk = sri.hub_retailer_item_hk
  
where fc.SALES_DT >= '2019-12-01'""")

# COMMAND ----------

# display(df5b)

# COMMAND ----------

df5b.count()

# COMMAND ----------

df5b.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save('/mnt/artifacts/hugh/morrisons-kraftheinz-post-2019-12-01-sales-new-predictions')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Morrisons Nestlecereals

# COMMAND ----------

# df6a = spark.sql("""

# select fc.sales_dt, fc.BASELINE_POS_ITEM_QTY, sales.pos_item_qty, sales.pos_amt, sales.on_hand_inventory_qty, hou.organization_unit_num, hri.RETAILER_ITEM_ID, sri.RETAILER_ITEM_DESC
# FROM morrisons_nestlecereals_uk_retail_alert_im.vw_drfe_forecast_baseline_unit as fc
# INNER JOIN morrisons_nestlecereals_uk_dv.link_epos_summary as les
#   ON fc.hub_organization_unit_hk = les.hub_organization_unit_hk AND fc.hub_retailer_item_hk = les.hub_retailer_item_hk AND fc.sales_dt = les.sales_dt
# INNER JOIN morrisons_nestlecereals_uk_dv.sat_link_epos_summary as sales
#   ON les.LINK_EPOS_SUMMARY_HK = sales.LINK_EPOS_SUMMARY_HK
# INNER JOIN morrisons_nestlecereals_uk_dv.hub_organization_unit as hou
#   ON hou.hub_organization_unit_hk = fc.hub_organization_unit_hk
# INNER JOIN morrisons_nestlecereals_uk_dv.hub_retailer_item as hri
#   ON fc.hub_retailer_item_hk = hri.hub_retailer_item_hk
# INNER JOIN morrisons_nestlecereals_uk_dv.sat_retailer_item as sri
#   ON fc.hub_retailer_item_hk = sri.hub_retailer_item_hk
  
# where fc.SALES_DT >= '2019-10-15'""")

# COMMAND ----------

# df6a.count()

# COMMAND ----------

# display(df6a)

# COMMAND ----------

# df6a.coalesce(1)\
#     .write.format('com.databricks.spark.csv')\
#     .option('header', 'true')\
#     .mode('overwrite')\
#     .save('/mnt/artifacts/hugh/morrisons-nestlecereals-post-2019-10-15-sales-new-predictions')

# COMMAND ----------

df6b = spark.sql("""

select fc.sales_dt, fc.BASELINE_POS_ITEM_QTY, sales.pos_item_qty, sales.pos_amt, sales.on_hand_inventory_qty, hou.organization_unit_num, hri.RETAILER_ITEM_ID, sri.RETAILER_ITEM_DESC
FROM morrisons_nestlecereals_uk_retail_alert_im.vw_drfe_forecast_baseline_unit as fc
INNER JOIN morrisons_nestlecereals_uk_dv.link_epos_summary as les
  ON fc.hub_organization_unit_hk = les.hub_organization_unit_hk AND fc.hub_retailer_item_hk = les.hub_retailer_item_hk AND fc.sales_dt = les.sales_dt
INNER JOIN morrisons_nestlecereals_uk_dv.sat_link_epos_summary as sales
  ON les.LINK_EPOS_SUMMARY_HK = sales.LINK_EPOS_SUMMARY_HK
INNER JOIN morrisons_nestlecereals_uk_dv.hub_organization_unit as hou
  ON hou.hub_organization_unit_hk = fc.hub_organization_unit_hk
INNER JOIN morrisons_nestlecereals_uk_dv.hub_retailer_item as hri
  ON fc.hub_retailer_item_hk = hri.hub_retailer_item_hk
INNER JOIN morrisons_nestlecereals_uk_dv.sat_retailer_item as sri
  ON fc.hub_retailer_item_hk = sri.hub_retailer_item_hk
  
where fc.SALES_DT >= '2019-12-01'""")

# COMMAND ----------

df6b.count()

# COMMAND ----------

# display(df6b)

# COMMAND ----------

df6b.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save('/mnt/artifacts/hugh/morrisons-nestlecereals-post-2019-12-01-sales-new-predictions')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Sainsburys Kraftheinz

# COMMAND ----------

# df7a = spark.sql("""

# select fc.sales_dt, fc.BASELINE_POS_ITEM_QTY, sales.pos_item_qty, sales.pos_amt, sales.on_hand_inventory_qty, hou.organization_unit_num, hri.RETAILER_ITEM_ID, sri.RETAILER_ITEM_DESC
# FROM sainsburys_kraftheinz_uk_retail_alert_im.vw_drfe_forecast_baseline_unit as fc
# INNER JOIN sainsburys_kraftheinz_uk_dv.link_epos_summary as les
#   ON fc.hub_organization_unit_hk = les.hub_organization_unit_hk AND fc.hub_retailer_item_hk = les.hub_retailer_item_hk AND fc.sales_dt = les.sales_dt
# INNER JOIN sainsburys_kraftheinz_uk_dv.sat_link_epos_summary as sales
#   ON les.LINK_EPOS_SUMMARY_HK = sales.LINK_EPOS_SUMMARY_HK
# INNER JOIN sainsburys_kraftheinz_uk_dv.hub_organization_unit as hou
#   ON hou.hub_organization_unit_hk = fc.hub_organization_unit_hk
# INNER JOIN sainsburys_kraftheinz_uk_dv.hub_retailer_item as hri
#   ON fc.hub_retailer_item_hk = hri.hub_retailer_item_hk
# INNER JOIN sainsburys_kraftheinz_uk_dv.sat_retailer_item as sri
#   ON fc.hub_retailer_item_hk = sri.hub_retailer_item_hk
  
# where fc.SALES_DT >= '2019-10-15'""")

# COMMAND ----------

# df7a.count()

# COMMAND ----------

# display(df7a)

# COMMAND ----------

# df7a.coalesce(1)\
#     .write.format('com.databricks.spark.csv')\
#     .option('header', 'true')\
#     .mode('overwrite')\
#     .save('/mnt/artifacts/hugh/sainsburys-kraftheinz-post-2019-10-15-sales-new-predictions')

# COMMAND ----------

df7b = spark.sql("""

select fc.sales_dt, fc.BASELINE_POS_ITEM_QTY, sales.pos_item_qty, sales.pos_amt, sales.on_hand_inventory_qty, hou.organization_unit_num, hri.RETAILER_ITEM_ID, sri.RETAILER_ITEM_DESC
FROM sainsburys_kraftheinz_uk_retail_alert_im.vw_drfe_forecast_baseline_unit as fc
INNER JOIN sainsburys_kraftheinz_uk_dv.link_epos_summary as les
  ON fc.hub_organization_unit_hk = les.hub_organization_unit_hk AND fc.hub_retailer_item_hk = les.hub_retailer_item_hk AND fc.sales_dt = les.sales_dt
INNER JOIN sainsburys_kraftheinz_uk_dv.sat_link_epos_summary as sales
  ON les.LINK_EPOS_SUMMARY_HK = sales.LINK_EPOS_SUMMARY_HK
INNER JOIN sainsburys_kraftheinz_uk_dv.hub_organization_unit as hou
  ON hou.hub_organization_unit_hk = fc.hub_organization_unit_hk
INNER JOIN sainsburys_kraftheinz_uk_dv.hub_retailer_item as hri
  ON fc.hub_retailer_item_hk = hri.hub_retailer_item_hk
INNER JOIN sainsburys_kraftheinz_uk_dv.sat_retailer_item as sri
  ON fc.hub_retailer_item_hk = sri.hub_retailer_item_hk
  
where fc.SALES_DT >= '2019-12-01'""")

# COMMAND ----------

df7b.count()

# COMMAND ----------

# display(df7b)

# COMMAND ----------

df7b.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save('/mnt/artifacts/hugh/sainsburys-kraftheinz-post-2019-12-01-sales-new-predictions')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Sainsburys Nestlecereals

# COMMAND ----------

# df8a = spark.sql("""

# select fc.sales_dt, fc.BASELINE_POS_ITEM_QTY, sales.pos_item_qty, sales.pos_amt, sales.on_hand_inventory_qty, hou.organization_unit_num, hri.RETAILER_ITEM_ID, sri.RETAILER_ITEM_DESC
# FROM sainsburys_nestlecereals_uk_retail_alert_im.vw_drfe_forecast_baseline_unit as fc
# INNER JOIN sainsburys_nestlecereals_uk_dv.link_epos_summary as les
#   ON fc.hub_organization_unit_hk = les.hub_organization_unit_hk AND fc.hub_retailer_item_hk = les.hub_retailer_item_hk AND fc.sales_dt = les.sales_dt
# INNER JOIN sainsburys_nestlecereals_uk_dv.sat_link_epos_summary as sales
#   ON les.LINK_EPOS_SUMMARY_HK = sales.LINK_EPOS_SUMMARY_HK
# INNER JOIN sainsburys_nestlecereals_uk_dv.hub_organization_unit as hou
#   ON hou.hub_organization_unit_hk = fc.hub_organization_unit_hk
# INNER JOIN sainsburys_nestlecereals_uk_dv.hub_retailer_item as hri
#   ON fc.hub_retailer_item_hk = hri.hub_retailer_item_hk
# INNER JOIN sainsburys_nestlecereals_uk_dv.sat_retailer_item as sri
#   ON fc.hub_retailer_item_hk = sri.hub_retailer_item_hk
  
# where fc.SALES_DT >= '2019-10-15'""")

# COMMAND ----------

# df8a.count()

# COMMAND ----------

# display(df8a)

# COMMAND ----------

# df8a.coalesce(1)\
#     .write.format('com.databricks.spark.csv')\
#     .option('header', 'true')\
#     .mode('overwrite')\
#     .save('/mnt/artifacts/hugh/sainsburys-nestlecereals-post-2019-10-15-sales-new-predictions')

# COMMAND ----------

df8b = spark.sql("""

select fc.sales_dt, fc.BASELINE_POS_ITEM_QTY, sales.pos_item_qty, sales.pos_amt, sales.on_hand_inventory_qty, hou.organization_unit_num, hri.RETAILER_ITEM_ID, sri.RETAILER_ITEM_DESC
FROM sainsburys_nestlecereals_uk_retail_alert_im.vw_drfe_forecast_baseline_unit as fc
INNER JOIN sainsburys_nestlecereals_uk_dv.link_epos_summary as les
  ON fc.hub_organization_unit_hk = les.hub_organization_unit_hk AND fc.hub_retailer_item_hk = les.hub_retailer_item_hk AND fc.sales_dt = les.sales_dt
INNER JOIN sainsburys_nestlecereals_uk_dv.sat_link_epos_summary as sales
  ON les.LINK_EPOS_SUMMARY_HK = sales.LINK_EPOS_SUMMARY_HK
INNER JOIN sainsburys_nestlecereals_uk_dv.hub_organization_unit as hou
  ON hou.hub_organization_unit_hk = fc.hub_organization_unit_hk
INNER JOIN sainsburys_nestlecereals_uk_dv.hub_retailer_item as hri
  ON fc.hub_retailer_item_hk = hri.hub_retailer_item_hk
INNER JOIN sainsburys_nestlecereals_uk_dv.sat_retailer_item as sri
  ON fc.hub_retailer_item_hk = sri.hub_retailer_item_hk
  
where fc.SALES_DT >= '2019-12-01'""")

# COMMAND ----------

df8b.count()

# COMMAND ----------

# display(df8b)

# COMMAND ----------

# df8b.coalesce(1)\
#     .write.format('com.databricks.spark.csv')\
#     .option('header', 'true')\
#     .mode('overwrite')\
#     .save('/mnt/artifacts/hugh/sainsburys-nestlecereals-post-2019-12-01-sales-new-predictions')

# COMMAND ----------


