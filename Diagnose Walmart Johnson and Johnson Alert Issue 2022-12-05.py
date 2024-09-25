# Databricks notebook source
df0 = spark.sql("""SELECT DISTINCT RECORD_SOURCE_CD FROM walmart_dole_retail_alert_im.lost_sales_value""")
# recent_df = df.filter(df.SALES_DT >= '2019-02-09').filter(df.MODEL_TYPE.isin('RidgeCV', 'LassoCV', 'BayesianRidge'))

display(df0)

# COMMAND ----------

df1 = spark.sql("""SELECT DISTINCT 	SALES_DT FROM walmart_dole_retail_alert_im.lost_sales_value""")
# recent_df = df.filter(df.SALES_DT >= '2019-02-09').filter(df.MODEL_TYPE.isin('RidgeCV', 'LassoCV', 'BayesianRidge'))

display(df1)

# COMMAND ----------

df2 = spark.sql("""SELECT * FROM walmart_dole_retail_alert_im.lost_sales_value where record_source_cd == 'drfe'""")
# recent_df = df.filter(df.SALES_DT >= '2019-02-09').filter(df.MODEL_TYPE.isin('RidgeCV', 'LassoCV', 'BayesianRidge'))

display(df2)

# COMMAND ----------

df2.count()

# COMMAND ----------

df3 = spark.sql("""SELECT * FROM walmart_dole_retail_alert_im.alert_on_shelf_availability
                 where record_source_cd == 'drfe' and SALES_DT <= '2019-02-21' and SALES_DT >= '2019-02-17'""")
# recent_df = df.filter(df.SALES_DT >= '2019-02-09').filter(df.MODEL_TYPE.isin('RidgeCV', 'LassoCV', 'BayesianRidge'))

display(df3)

# COMMAND ----------

# MAGIC %sql
# MAGIC alert_on_shelf_availability

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from walmart_tysonhillshire_retail_alert_im.alert_on_shelf_availability
# MAGIC where SALES_DT == '2019-02-21'
# MAGIC and HUB_ORGANIZATION_UNIT_HK = 'a928731e103dfc64c0027fa84709689e'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from walmart_minutemaid_retail_alert_im.alert_on_shelf_availability
# MAGIC where SALES_DT == '2019-02-21'
# MAGIC and HUB_ORGANIZATION_UNIT_HK = 'a928731e103dfc64c0027fa84709689e'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from walmart_minutemaid_retail_alert_im.alert_on_shelf_availability
# MAGIC where SALES_DT >= '2019-02-15'
# MAGIC and HUB_ORGANIZATION_UNIT_HK = 'a928731e103dfc64c0027fa84709689e'
# MAGIC and HUB_RETAILER_ITEM_HK = 'c4b509f39a762b086b8421123c9b576c'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from walmart_minutemaid_retail_alert_im.lost_sales_value
# MAGIC where SALES_DT <= '2019-02-26'
# MAGIC and SALES_DT >= '2019-02-23'
# MAGIC and HUB_ORGANIZATION_UNIT_HK = 'a928731e103dfc64c0027fa84709689e'
# MAGIC and HUB_RETAILER_ITEM_HK = 'c4b509f39a762b086b8421123c9b576c'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from walmart_minutemaid_retail_alert_im.lost_sales_value
# MAGIC where SALES_DT <= '2019-02-22'
# MAGIC and SALES_DT >= '2019-02-15'
# MAGIC and HUB_ORGANIZATION_UNIT_HK = 'a928731e103dfc64c0027fa84709689e'
# MAGIC and HUB_RETAILER_ITEM_HK = 'c4b509f39a762b086b8421123c9b576c'

# COMMAND ----------

# MAGIC %sql
# MAGIC select LOAD_TS	RECORD_SOURCE_CD, BASELINE_POS_ITEM_QTY, SALES_DT from walmart_minutemaid_retail_alert_im.drfe_forecast_baseline_unit
# MAGIC where SALES_DT >= '2019-02-21'
# MAGIC and HUB_ORGANIZATION_UNIT_HK = 'a928731e103dfc64c0027fa84709689e'
# MAGIC and HUB_RETAILER_ITEM_HK = 'c4b509f39a762b086b8421123c9b576c'

# COMMAND ----------

# MAGIC %sql
# MAGIC select SALES_DT, LOAD_TS, RECORD_SOURCE_CD, POS_ITEM_QTY, POS_AMT, ON_HAND_INVENTORY_QTY from walmart_minutemaid_dv.vw_sat_link_epos_summary
# MAGIC where SALES_DT <= '2019-02-26'
# MAGIC and SALES_DT >= '2019-02-21'
# MAGIC and HUB_ORGANIZATION_UNIT_HK = 'a928731e103dfc64c0027fa84709689e'
# MAGIC and HUB_RETAILER_ITEM_HK = 'c4b509f39a762b086b8421123c9b576c'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from walmart_minutemaid_dv.vw_sat_link_epos_summary
# MAGIC where SALES_DT <= '2019-02-21'
# MAGIC and SALES_DT >= '2019-02-15'
# MAGIC and HUB_ORGANIZATION_UNIT_HK = 'a928731e103dfc64c0027fa84709689e'
# MAGIC and HUB_RETAILER_ITEM_HK = 'c4b509f39a762b086b8421123c9b576c'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from walmart_minutemaid_dv.hub_organization_unit where ORGANIZATION_UNIT_NUM = 3702

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from walmart_minutemaid_dv.hub_retailer_item where RETAILER_ITEM_ID = 9004038

# COMMAND ----------

# MAGIC %sql
# MAGIC use walmart_syn_team_alerts_im;
# MAGIC select * from alert_on_shelf_availability where HUB_ORGANIZATION_UNIT_HK = 'a928731e103dfc64c0027fa84709689e'
# MAGIC and HUB_RETAILER_ITEM_HK = 'c4b509f39a762b086b8421123c9b576c' and sales_dt='2019-02-21'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct SALES_DT from retail_alert_walmart_johnsonandjohnson_us_im.alert_on_shelf_availability
# MAGIC order by SALES_DT desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct SALES_DT from retail_alert_walmart_johnsonandjohnson_us_im.drfe_forecast_baseline_unit
# MAGIC order by SALES_DT desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_alert_walmart_johnsonandjohnson_us_im.drfe_forecast_baseline_unit
# MAGIC where SALES_DT >= '2022-11-28'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct SALES_DT from retail_alert_walmart_johnsonandjohnson_us_im.lost_sales_value
# MAGIC order by SALES_DT desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct SALES_DT from retaillink_walmart_johnsonandjohnson_us_dv.vw_latest_sat_epos_summary
# MAGIC order by SALES_DT desc

# COMMAND ----------

df_sql_query_walmart = """
    select SALES_DT, BASELINE_POS_ITEM_QTY from retail_alert_walmart_johnsonandjohnson_us_im.drfe_forecast_baseline_unit
    where SALES_DT >= '2022-12-04'
    order by SALES_DT desc
"""
