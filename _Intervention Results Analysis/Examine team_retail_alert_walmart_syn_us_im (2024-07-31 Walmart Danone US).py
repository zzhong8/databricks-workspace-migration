# Databricks notebook source
import datetime
import pyspark.sql.functions as pyf

# COMMAND ----------

sql_query_walmart_danone_p6m_OSA_alerts_by_date = """
    SELECT sales_dt, count(*) as num_OSA_alerts from team_retail_alert_walmart_syn_us_im.alert_on_shelf_availability
    WHERE Retail_Client = 'walmart_danoneusllc_us'
    AND sales_dt >= '2024-01-01'
    AND sales_dt <= '2024-06-30'
    group by sales_dt
    ORDER BY sales_dt
"""

df_walmart_danone_p6m_OSA_alerts_by_date = spark.sql(sql_query_walmart_danone_p6m_OSA_alerts_by_date)

display(df_walmart_danone_p6m_OSA_alerts_by_date)

# COMMAND ----------

df_walmart_danone_p6m_OSA_alerts_by_date_pre = df_walmart_danone_p6m_OSA_alerts_by_date.filter(df_walmart_danone_p6m_OSA_alerts_by_date.sales_dt < '2024-05-30')

display(df_walmart_danone_p6m_OSA_alerts_by_date_pre)

# COMMAND ----------

df_walmart_danone_p6m_OSA_alerts_by_date_post = df_walmart_danone_p6m_OSA_alerts_by_date.filter(df_walmart_danone_p6m_OSA_alerts_by_date.sales_dt >= '2024-05-30')

display(df_walmart_danone_p6m_OSA_alerts_by_date_post)

# COMMAND ----------

df_walmart_danone_p2m_OSA_alerts_by_date_pre = df_walmart_danone_p6m_OSA_alerts_by_date.filter(df_walmart_danone_p6m_OSA_alerts_by_date.sales_dt >= '2024-05-01')
df_walmart_danone_p2m_OSA_alerts_by_date_pre = df_walmart_danone_p2m_OSA_alerts_by_date_pre.filter(df_walmart_danone_p2m_OSA_alerts_by_date_pre.sales_dt < '2024-05-30')

display(df_walmart_danone_p2m_OSA_alerts_by_date_pre)

# COMMAND ----------

df_walmart_danone_p2m_OSA_alerts_by_date_pre_agg = df_walmart_danone_p2m_OSA_alerts_by_date_pre.summary()

display(df_walmart_danone_p2m_OSA_alerts_by_date_pre_agg)

# COMMAND ----------

df_walmart_danone_p2m_OSA_alerts_by_date_post = df_walmart_danone_p6m_OSA_alerts_by_date.filter(df_walmart_danone_p6m_OSA_alerts_by_date.sales_dt >= '2024-05-30')

display(df_walmart_danone_p2m_OSA_alerts_by_date_post)

# COMMAND ----------

df_walmart_danone_p2m_OSA_alerts_by_date_post_agg = df_walmart_danone_p2m_OSA_alerts_by_date_post.summary()

display(df_walmart_danone_p2m_OSA_alerts_by_date_post_agg)

# COMMAND ----------

sql_query_walmart_danone_p6m_inv_cleanup_alerts_by_date = """
    SELECT sales_dt, count(*) as num_inv_cleanup_alerts from team_retail_alert_walmart_syn_us_im.alert_inventory_cleanup
    WHERE Retail_Client = 'walmart_danoneusllc_us'
    AND sales_dt >= '2024-01-01'
    AND sales_dt <= '2024-06-30'
    group by sales_dt
    ORDER BY sales_dt
"""

df_walmart_danone_p6m_inv_cleanup_alerts_by_date = spark.sql(sql_query_walmart_danone_p6m_inv_cleanup_alerts_by_date)

display(df_walmart_danone_p6m_inv_cleanup_alerts_by_date)

# COMMAND ----------

df_walmart_danone_p2m_inv_cleanup_alerts_by_date_pre = df_walmart_danone_p6m_inv_cleanup_alerts_by_date.filter(df_walmart_danone_p6m_inv_cleanup_alerts_by_date.sales_dt >= '2024-05-01')
df_walmart_danone_p2m_inv_cleanup_alerts_by_date_pre = df_walmart_danone_p2m_inv_cleanup_alerts_by_date_pre.filter(df_walmart_danone_p2m_inv_cleanup_alerts_by_date_pre.sales_dt < '2024-05-30')

display(df_walmart_danone_p2m_inv_cleanup_alerts_by_date_pre)

# COMMAND ----------

df_walmart_danone_p2m_inv_cleanup_alerts_by_date_post = df_walmart_danone_p6m_inv_cleanup_alerts_by_date.filter(df_walmart_danone_p6m_inv_cleanup_alerts_by_date.sales_dt >= '2024-05-30')

display(df_walmart_danone_p2m_inv_cleanup_alerts_by_date_post)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(*) from acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 882 -- Danone
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC actionable_flg = 1
# MAGIC -- and
# MAGIC -- objective_typ = 'Opportunity'
# MAGIC     AND call_date >= '2024-01-01'
# MAGIC     AND call_date <= '2024-06-30'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(measurement_duration), sum(total_intervention_effect), sum(total_impact) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 882 -- Danone
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC objective_typ = 'DLA'
# MAGIC and
# MAGIC is_complete = 'True'
# MAGIC and
# MAGIC call_date >= '2024-01-01'
# MAGIC AND 
# MAGIC call_date <= '2024-06-30'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select mdm_country_nm, mdm_holding_nm, mdm_client_nm, objective_typ, concat(year(call_date), '-', format_number(month(call_date),'00')) as year_month, count(measurement_duration), sum(total_intervention_effect), sum(total_impact) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_nm in ('us', 'ca')
# MAGIC and
# MAGIC is_complete = 'True'
# MAGIC and
# MAGIC call_date >= '2024-01-01'
# MAGIC AND call_date <= '2024-06-30'
# MAGIC group by mdm_country_nm, mdm_holding_nm, mdm_client_nm, objective_typ, year_month
# MAGIC order by mdm_country_nm, mdm_holding_nm, mdm_client_nm, objective_typ, year_month

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select concat(year(call_date), '-', format_number(month(call_date),'00')) as year_month, count(measurement_duration), sum(total_intervention_effect), sum(total_impact) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 882 -- Danone
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC objective_typ = 'DLA'
# MAGIC and
# MAGIC is_complete = 'True'
# MAGIC and
# MAGIC call_date >= '2024-01-01'
# MAGIC AND call_date <= '2024-06-30'
# MAGIC group by year_month
# MAGIC order by year_month

# COMMAND ----------

sql_query_walmart_danoneusllc_p6m_measured_DLA_interventions_by_date = """
    select call_date, count(measurement_duration), sum(total_intervention_effect), sum(total_impact) from acosta_retail_analytics_im.ds_intervention_summary
    where
    mdm_country_id = 1 -- US
    and
    mdm_client_id = 882 -- Danone
    and
    mdm_holding_id = 71 -- Walmart
    and
    coalesce(mdm_banner_id, -1) = -1 -- default
    and
    objective_typ = 'DLA'
    and
    is_complete = 'True'
    and
    call_date >= '2024-01-01'
    AND call_date <= '2024-06-30'
    group by call_date
    order by call_date
"""

df_walmart_danoneusllc_p6m_measured_DLA_interventions_by_date = spark.sql(sql_query_walmart_danoneusllc_p6m_measured_DLA_interventions_by_date)

display(df_walmart_danoneusllc_p6m_measured_DLA_interventions_by_date)

# COMMAND ----------

df_walmart_danoneusllc_p6m_measured_DLA_interventions_by_date_pre = df_walmart_danoneusllc_p6m_measured_DLA_interventions_by_date.filter(df_walmart_danoneusllc_p6m_measured_DLA_interventions_by_date.call_date < '2024-06-01')

display(df_walmart_danoneusllc_p6m_measured_DLA_interventions_by_date_pre)

# COMMAND ----------

df_walmart_danoneusllc_p6m_measured_DLA_interventions_by_date_post = df_walmart_danoneusllc_p6m_measured_DLA_interventions_by_date.filter(df_walmart_danoneusllc_p6m_measured_DLA_interventions_by_date.call_date >= '2024-06-01')

display(df_walmart_danoneusllc_p6m_measured_DLA_interventions_by_date_post)

# COMMAND ----------

df_walmart_danoneusllc_p2m_measured_DLA_interventions_by_date_pre = df_walmart_danoneusllc_p6m_measured_DLA_interventions_by_date.filter(df_walmart_danoneusllc_p6m_measured_DLA_interventions_by_date.call_date >= '2024-05-01')

df_walmart_danoneusllc_p2m_measured_DLA_interventions_by_date_pre = df_walmart_danoneusllc_p2m_measured_DLA_interventions_by_date_pre.filter(df_walmart_danoneusllc_p2m_measured_DLA_interventions_by_date_pre.call_date < '2024-06-01')

display(df_walmart_danoneusllc_p2m_measured_DLA_interventions_by_date_pre)

# COMMAND ----------

df_walmart_danoneusllc_p6m_measured_DLA_interventions_by_date_post = df_walmart_danoneusllc_p6m_measured_DLA_interventions_by_date.filter(df_walmart_danoneusllc_p6m_measured_DLA_interventions_by_date.call_date >= '2024-06-01')

display(df_walmart_danoneusllc_p6m_measured_DLA_interventions_by_date_post)

# COMMAND ----------


