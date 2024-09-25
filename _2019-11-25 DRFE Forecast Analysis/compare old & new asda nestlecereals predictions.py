# Databricks notebook source
PATH_RESULTS_INPUT_OLD = '/mnt/artifacts/hugh/asda-nestlecereals-post-2019-10-12-sales-old-predictions'

df_old_forecast = spark.read.format('csv').option('header', 'true').load(PATH_RESULTS_INPUT_OLD)

# COMMAND ----------

df_old_forecast.count()

# COMMAND ----------

display(df_old_forecast)

# COMMAND ----------

PATH_RESULTS_INPUT_NEW = '/mnt/artifacts/hugh/asda-nestlecereals-post-2019-10-12-sales-new-predictions'

df_new_forecast = spark.read.format('csv').option('header', 'true').load(PATH_RESULTS_INPUT_NEW)

# COMMAND ----------

df_new_forecast.count()

# COMMAND ----------

display(df_new_forecast)

# COMMAND ----------

df1 = df_old_forecast
df2 = df_new_forecast.withColumnRenamed('BASELINE_POS_ITEM_QTY', 'NEW_BASELINE_POS_ITEM_QTY')

# COMMAND ----------

df_forecast_comparison = df1.join(df2, ((df1.sales_dt == df2.sales_dt) & (df1.organization_unit_num == df2.organization_unit_num)) & (df1.RETAILER_ITEM_ID == df2.RETAILER_ITEM_ID)).select(df1['*'], df2['NEW_BASELINE_POS_ITEM_QTY'])

# COMMAND ----------

df_forecast_comparison.count()

# COMMAND ----------

display(df_forecast_comparison)

# COMMAND ----------

df_forecast_comparison.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save('/mnt/artifacts/hugh/asda-nestlecereals-post-2019-10-12-predictions-comparison')

# COMMAND ----------

df = spark.sql("""
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


where fc.SALES_DT >= '2019-10-12'""")

# COMMAND ----------

df.count()

# COMMAND ----------

df.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save('/mnt/artifacts/hugh/asda-nestlecereals-post-2019-10-12-sales-new-forecast')

# COMMAND ----------

display(df)

# COMMAND ----------

df.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save('/mnt/artifacts/hugh/asda-nestlecereals-post-2019-10-12-sales-old-forecast')

# COMMAND ----------

df2 = spark.sql("""
select fc.sales_dt, fc.BASELINE_POS_ITEM_QTY, sales.pos_item_qty, sales.pos_amt, sales.on_hand_inventory_qty, hou.organization_unit_num, hri.RETAILER_ITEM_ID, sri.RETAILER_ITEM_DESC
FROM walmart_rbusahygiene_us_retail_alert_im.vw_drfe_forecast_baseline_unit as fc
INNER JOIN walmart_rbusahygiene_us_dv.link_epos_summary as les
  ON fc.hub_organization_unit_hk = les.hub_organization_unit_hk AND fc.hub_retailer_item_hk = les.hub_retailer_item_hk AND fc.sales_dt = les.sales_dt
INNER JOIN walmart_rbusahygiene_us_dv.sat_link_epos_summary as sales
  ON les.LINK_EPOS_SUMMARY_HK = sales.LINK_EPOS_SUMMARY_HK
INNER JOIN walmart_rbusahygiene_us_dv.hub_organization_unit as hou
  ON hou.hub_organization_unit_hk = fc.hub_organization_unit_hk
INNER JOIN walmart_rbusahygiene_us_dv.hub_retailer_item as hri
  ON fc.hub_retailer_item_hk = hri.hub_retailer_item_hk
INNER JOIN walmart_rbusahygiene_us_dv.sat_retailer_item as sri
  ON fc.hub_retailer_item_hk = sri.hub_retailer_item_hk

where fc.SALES_DT >= '2019-01-13'""")

# COMMAND ----------

sales_dates = df2.select('SALES_DT').distinct().orderBy('SALES_DT', ascending=False)

# COMMAND ----------

display(sales_dates)

# COMMAND ----------

df2.count()

# COMMAND ----------

display(df2)

# COMMAND ----------

df2.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save('/mnt/artifacts/hugh/walmart-rbusahygiene-post-2019-08-13-sales-old-forecast')

# COMMAND ----------

df3 = spark.sql("""

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
  
where fc.SALES_DT >= '2019-10-15'""")

# COMMAND ----------

df3.count()

# COMMAND ----------

df3.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save('/mnt/artifacts/hugh/tesco-kraftheinz-post-2019-10-15-sales-old-forecast')

# COMMAND ----------

df4 = spark.sql("""

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
  
where fc.SALES_DT >= '2019-10-15'""")

# COMMAND ----------

df4.count()

# COMMAND ----------

df4.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save('/mnt/artifacts/hugh/tesco-nestlecereals-post-2019-10-15-sales-new-forecast')

# COMMAND ----------

df4.coalesce(1)\
    .write.format('com.databricks.spark.csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save('/mnt/artifacts/hugh/tesco-nestlecereals-post-2019-10-15-sales-old-forecast')

# COMMAND ----------

# MAGIC %md
# MAGIC This is a deep dive into a question about missing campbell's snacks alerts - 
# MAGIC 1. need to check whether the forecast was created - yes
# MAGIC 2. is there LSV for the dates required? - yes
# MAGIC 3. export some forecast + actuals to hand calculate and verify - yes
# MAGIC 4. check alerts tables for population - yes for sales_dt = 6/29 and 6/30 - unsure whether 7/1 should be. 
# MAGIC
# MAGIC Does not look like forecasting engine is root cause of no alerts, and it appears to have mostly gone downstream correctly. 
# MAGIC
# MAGIC Hypothesis: out of sequence data processing or data propagation failure in the alerts process? 
# MAGIC
# MAGIC Item with ridiculous LSV: TESCO - 51169241, Sainsbury 7861026 , Morrisons 176668
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC /*select * from tesco_kraftheinz_uk_retail_alert_im.lost_sales_value
# MAGIC where HUB_RETAILER_ITEM_HK = '64c1acb3bf54cad58e3fe00c010efebe'
# MAGIC order by LOST_SALES_AMT desc*/
# MAGIC
# MAGIC select * from morrisons_kraftheinz_uk_dv.hub_retailer_item

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct(hub_organization_unit_hk)) from walmart_clorox_retail_alert_im.vw_alert_on_shelf_availability
# MAGIC WHERE SALES_DT = "2019-07-01" --or sales_dt = "2019-06-29" /* 3,605 alerts for sales_dt = 2019-06-30 */
# MAGIC   and LOST_SALES_AMT > 30
# MAGIC --WHERE sales_dt = "2019-07-01"  /* THIS GIVES ZERO RECORDS - confirmed normal with below test on Clorox */

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from walmart_campbellssnack_retail_alert_im.vw_alert_on_shelf_availability
# MAGIC WHERE SALES_DT = "2019-07-01" --or sales_dt = "2019-06-29" /* 3,605 alerts for sales_dt = 2019-06-30 */
# MAGIC   and LOST_SALES_AMT > 7
# MAGIC --WHERE sales_dt = "2019-07-01"  /* THIS GIVES ZERO RECORDS - confirmed normal with below test on Clorox */

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Let's check Clorox since they had alerts! */
# MAGIC select * from walmart_clorox_retail_alert_im.vw_alert_on_shelf_availability
# MAGIC --WHERE SALES_DT = "2019-06-30" --or sales_dt = "2019-06-29" /* 3,600+ alerts for sales_dt = 2019-06-30 */
# MAGIC --and LOST_SALES_AMT > 7
# MAGIC WHERE sales_dt = "2019-07-01"  /* THIS GIVES NON-ZERO RECORDS */
# MAGIC /* CONCLUSION: today's date should have no records, but previous day's date should */

# COMMAND ----------

# MAGIC %sql
# MAGIC select sales_dt, max(BASELINE_POS_ITEM_QTY) from walmart_campbellssnack_retail_alert_im.vw_drfe_forecast_baseline_unit
# MAGIC where sales_dt >= "2019-06-29"
# MAGIC group by sales_dt
# MAGIC
# MAGIC /* Yep, at least some high enough forecasts to have likely lost sales */

# COMMAND ----------

# MAGIC %sql
# MAGIC /* TESCO QUERY */
# MAGIC select fc.sales_dt, fc.BASELINE_POS_ITEM_QTY, sales.pos_item_qty, sales.pos_amt, sales.on_hand_inventory_qty, hou.organization_unit_num, hri.RETAILER_ITEM_ID, sri.RETAILER_ITEM_DESC
# MAGIC FROM tesco_kraftheinz_uk_retail_alert_im.vw_drfe_forecast_baseline_unit as fc
# MAGIC INNER JOIN tesco_kraftheinz_uk_dv.link_epos_summary as les
# MAGIC   ON fc.hub_organization_unit_hk = les.hub_organization_unit_hk AND fc.hub_retailer_item_hk = les.hub_retailer_item_hk AND fc.sales_dt = les.sales_dt
# MAGIC INNER JOIN tesco_kraftheinz_uk_dv.sat_link_epos_summary as sales
# MAGIC   ON les.LINK_EPOS_SUMMARY_HK = sales.LINK_EPOS_SUMMARY_HK
# MAGIC INNER JOIN tesco_kraftheinz_uk_dv.hub_organization_unit as hou
# MAGIC   ON hou.hub_organization_unit_hk = fc.hub_organization_unit_hk
# MAGIC INNER JOIN tesco_kraftheinz_uk_dv.hub_retailer_item as hri
# MAGIC   ON fc.hub_retailer_item_hk = hri.hub_retailer_item_hk
# MAGIC INNER JOIN tesco_kraftheinz_uk_dv.sat_retailer_item as sri
# MAGIC   ON fc.hub_retailer_item_hk = sri.hub_retailer_item_hk
# MAGIC
# MAGIC
# MAGIC where fc.SALES_DT >= '2019-10-15' and RETAILER_ITEM_ID in ('50282902','50020522','50504780','50174648','51169241')
# MAGIC /* Let's generate an extract of the last two days with baseline quantities high enough to likely find missing sales from them. */

# COMMAND ----------

# MAGIC %sql
# MAGIC /* TESCO QUERY */
# MAGIC select sum(fc.BASELINE_POS_ITEM_QTY), sum(sales.pos_item_qty), hri.RETAILER_ITEM_ID, sri.RETAILER_ITEM_DESC
# MAGIC FROM tesco_kraftheinz_uk_retail_alert_im.vw_drfe_forecast_baseline_unit as fc
# MAGIC INNER JOIN tesco_kraftheinz_uk_dv.link_epos_summary as les
# MAGIC   ON fc.hub_organization_unit_hk = les.hub_organization_unit_hk AND fc.hub_retailer_item_hk = les.hub_retailer_item_hk AND fc.sales_dt = les.sales_dt
# MAGIC INNER JOIN tesco_kraftheinz_uk_dv.sat_link_epos_summary as sales
# MAGIC   ON les.LINK_EPOS_SUMMARY_HK = sales.LINK_EPOS_SUMMARY_HK
# MAGIC INNER JOIN tesco_kraftheinz_uk_dv.hub_organization_unit as hou
# MAGIC   ON hou.hub_organization_unit_hk = fc.hub_organization_unit_hk
# MAGIC INNER JOIN tesco_kraftheinz_uk_dv.hub_retailer_item as hri
# MAGIC   ON fc.hub_retailer_item_hk = hri.hub_retailer_item_hk
# MAGIC INNER JOIN tesco_kraftheinz_uk_dv.sat_retailer_item as sri
# MAGIC   ON fc.hub_retailer_item_hk = sri.hub_retailer_item_hk
# MAGIC where fc.SALES_DT >= '2019-09-01'
# MAGIC
# MAGIC group by RETAILER_ITEM_ID, RETAILER_ITEM_DESC
# MAGIC order by sum(fc.BASELINE_POS_ITEM_QTY) desc
# MAGIC
# MAGIC /* Let's generate an extract of the last two days with baseline quantities high enough to likely find missing sales from them. */

# COMMAND ----------

# MAGIC %sql
# MAGIC /* ASDA QUERY */
# MAGIC select fc.sales_dt, fc.BASELINE_POS_ITEM_QTY, sales.pos_item_qty, sales.pos_amt, sales.on_hand_inventory_qty, hou.organization_unit_num, hri.RETAILER_ITEM_ID, sri.RETAILER_ITEM_DESC
# MAGIC FROM asda_kraftheinz_uk_retail_alert_im.vw_drfe_forecast_baseline_unit as fc
# MAGIC INNER JOIN asda_kraftheinz_uk_dv.link_epos_summary as les
# MAGIC   ON fc.hub_organization_unit_hk = les.hub_organization_unit_hk AND fc.hub_retailer_item_hk = les.hub_retailer_item_hk AND fc.sales_dt = les.sales_dt
# MAGIC INNER JOIN asda_kraftheinz_uk_dv.sat_link_epos_summary as sales
# MAGIC   ON les.LINK_EPOS_SUMMARY_HK = sales.LINK_EPOS_SUMMARY_HK
# MAGIC INNER JOIN asda_kraftheinz_uk_dv.hub_organization_unit as hou
# MAGIC   ON hou.hub_organization_unit_hk = fc.hub_organization_unit_hk
# MAGIC INNER JOIN asda_kraftheinz_uk_dv.hub_retailer_item as hri
# MAGIC   ON fc.hub_retailer_item_hk = hri.hub_retailer_item_hk
# MAGIC INNER JOIN asda_kraftheinz_uk_dv.sat_retailer_item as sri
# MAGIC   ON fc.hub_retailer_item_hk = sri.hub_retailer_item_hk
# MAGIC
# MAGIC
# MAGIC where fc.SALES_DT >= '2019-11-01' and RETAILER_ITEM_ID in ('6300602','6345070','6461453','50415444','6300633','6455377')
# MAGIC /* Let's generate an extract of the last two days with baseline quantities high enough to likely find missing sales from them. */
