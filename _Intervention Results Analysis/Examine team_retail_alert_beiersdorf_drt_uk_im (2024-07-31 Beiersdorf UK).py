# Databricks notebook source
import datetime
import pyspark.sql.functions as pyf

# COMMAND ----------

# MAGIC %sql
# MAGIC desc team_retail_alert_beiersdorf_drt_uk_im.alert_on_shelf_availability

# COMMAND ----------

sql_query_asda_beiersdorf_p12m_OSA_alerts_by_date = """
    SELECT sales_dt, count(*) as num_OSA_alerts from team_retail_alert_beiersdorf_drt_uk_im.alert_on_shelf_availability
    WHERE Retail_Client = 'asda_beiersdorf_uk'
    AND sales_dt >= '2023-07-01'
    AND sales_dt <= '2024-06-30'
    group by sales_dt
    ORDER BY sales_dt
"""

df_asda_beiersdorf_p12m_OSA_alerts_by_date = spark.sql(sql_query_asda_beiersdorf_p12m_OSA_alerts_by_date)

display(df_asda_beiersdorf_p12m_OSA_alerts_by_date)

# COMMAND ----------

df_asda_beiersdorf_p12m_OSA_alerts_by_date_pre = df_asda_beiersdorf_p12m_OSA_alerts_by_date.filter(df_asda_beiersdorf_p12m_OSA_alerts_by_date.sales_dt < '2024-01-23')

display(df_asda_beiersdorf_p12m_OSA_alerts_by_date_pre)

# COMMAND ----------

df_asda_beiersdorf_p12m_OSA_alerts_by_date_post = df_asda_beiersdorf_p12m_OSA_alerts_by_date.filter(df_asda_beiersdorf_p12m_OSA_alerts_by_date.sales_dt >= '2024-01-23')

display(df_asda_beiersdorf_p12m_OSA_alerts_by_date_post)

# COMMAND ----------

sql_query_asda_beiersdorf_p12m_inv_cleanup_alerts_by_date = """
    SELECT sales_dt, count(*) from team_retail_alert_beiersdorf_drt_uk_im.alert_inventory_cleanup
    WHERE Retail_Client = 'asda_beiersdorf_uk'
    AND sales_dt >= '2023-07-01'
    AND sales_dt <= '2024-06-30'
    group by sales_dt
    ORDER BY sales_dt
"""

df_asda_beiersdorf_p12m_inv_cleanup_alerts_by_date = spark.sql(sql_query_asda_beiersdorf_p12m_inv_cleanup_alerts_by_date)

display(df_asda_beiersdorf_p12m_inv_cleanup_alerts_by_date)

# COMMAND ----------

df_asda_beiersdorf_p12m_inv_cleanup_alerts_by_date_pre = df_asda_beiersdorf_p12m_inv_cleanup_alerts_by_date.filter(df_asda_beiersdorf_p12m_inv_cleanup_alerts_by_date.sales_dt < '2024-01-23')

display(df_asda_beiersdorf_p12m_inv_cleanup_alerts_by_date_pre)

# COMMAND ----------

df_asda_beiersdorf_p12m_inv_cleanup_alerts_by_date_post = df_asda_beiersdorf_p12m_inv_cleanup_alerts_by_date.filter(df_asda_beiersdorf_p12m_inv_cleanup_alerts_by_date.sales_dt >= '2024-01-23')

display(df_asda_beiersdorf_p12m_inv_cleanup_alerts_by_date_post)

# COMMAND ----------

sql_query_morrisons_beiersdorf_p12m_OSA_alerts_by_date = """
    SELECT sales_dt, count(*) from team_retail_alert_beiersdorf_drt_uk_im.alert_on_shelf_availability
    WHERE Retail_Client = 'morrisons_beiersdorf_uk'
    AND sales_dt >= '2023-07-01'
    AND sales_dt <= '2024-06-30'
    group by sales_dt
    ORDER BY sales_dt
"""

df_morrisons_beiersdorf_p12m_OSA_alerts_by_date = spark.sql(sql_query_morrisons_beiersdorf_p12m_OSA_alerts_by_date)

display(df_morrisons_beiersdorf_p12m_OSA_alerts_by_date)

# COMMAND ----------

sql_query_morrisons_beiersdorf_p12m_inv_cleanup_alerts_by_date = """
    SELECT sales_dt, count(*) from team_retail_alert_beiersdorf_drt_uk_im.alert_inventory_cleanup
    WHERE Retail_Client = 'morrisons_beiersdorf_uk'
    AND sales_dt >= '2023-07-01'
    AND sales_dt <= '2024-06-30'
    group by sales_dt
    ORDER BY sales_dt
"""

df_morrisons_beiersdorf_p12m_inv_cleanup_alerts_by_date = spark.sql(sql_query_morrisons_beiersdorf_p12m_inv_cleanup_alerts_by_date)

display(df_morrisons_beiersdorf_p12m_inv_cleanup_alerts_by_date)

# COMMAND ----------

sql_query_tesco_beiersdorf_p12m_OSA_alerts_by_date = """
    SELECT sales_dt, count(*) from team_retail_alert_beiersdorf_drt_uk_im.alert_on_shelf_availability
    WHERE Retail_Client = 'tesco_beiersdorf_uk'
    AND sales_dt >= '2023-07-01'
    AND sales_dt <= '2024-06-30'
    group by sales_dt
    ORDER BY sales_dt
"""

df_tesco_beiersdorf_p12m_OSA_alerts_by_date = spark.sql(sql_query_tesco_beiersdorf_p12m_OSA_alerts_by_date)

display(df_tesco_beiersdorf_p12m_OSA_alerts_by_date)

# COMMAND ----------

sql_query_tesco_beiersdorf_p12m_inv_cleanup_alerts_by_date = """
    SELECT sales_dt, count(*) from team_retail_alert_beiersdorf_drt_uk_im.alert_inventory_cleanup
    WHERE Retail_Client = 'tesco_beiersdorf_uk'
    AND sales_dt >= '2023-07-01'
    AND sales_dt <= '2024-06-30'
    group by sales_dt
    ORDER BY sales_dt
"""

df_tesco_beiersdorf_p12m_inv_cleanup_alerts_by_date = spark.sql(sql_query_tesco_beiersdorf_p12m_inv_cleanup_alerts_by_date)

display(df_tesco_beiersdorf_p12m_inv_cleanup_alerts_by_date)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(*) from acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC actionable_flg = 1
# MAGIC -- and
# MAGIC -- objective_typ = 'Opportunity'
# MAGIC AND call_date >= '2023-07-01'
# MAGIC AND call_date <= '2024-06-30'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %md
# MAGIC ### LOESS IVM

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(measurement_duration), sum(total_intervention_effect), sum(total_impact) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'DLA'
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC call_date >= '2023-07-01'
# MAGIC AND call_date <= '2024-06-30'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select concat(year(call_date), '-', format_number(month(call_date),'00')) as year_month, count(measurement_duration), sum(total_intervention_effect), sum(total_impact) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC objective_typ = 'DLA'
# MAGIC and
# MAGIC is_complete = 'True'
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC call_date >= '2023-07-01'
# MAGIC AND call_date <= '2024-06-30'
# MAGIC group by year_month
# MAGIC order by year_month

# COMMAND ----------

sql_query_asda_beiersdorf_p12m_measured_DLA_interventions_by_date = """
    select call_date, count(measurement_duration), sum(total_intervention_effect), sum(total_impact) from acosta_retail_analytics_im.ds_intervention_summary
    where
    mdm_country_id = 30 -- UK
    and
    mdm_client_id = 17686 -- Beiersdorf UK	
    and
    mdm_holding_id = 3257 -- AcostaRetailUK
    and
    objective_typ = 'DLA'
    and
    is_complete = 'True'
    and
    mdm_banner_id = 7743 -- Asda
    and
    call_date >= '2023-07-01'
    AND call_date <= '2024-06-30'
    group by call_date
    order by call_date
"""

df_asda_beiersdorf_p12m_measured_DLA_interventions_by_date = spark.sql(sql_query_asda_beiersdorf_p12m_measured_DLA_interventions_by_date)

display(df_asda_beiersdorf_p12m_measured_DLA_interventions_by_date)

# COMMAND ----------

df_asda_beiersdorf_p12m_measured_DLA_interventions_by_date_pre = df_asda_beiersdorf_p12m_measured_DLA_interventions_by_date.filter(df_asda_beiersdorf_p12m_measured_DLA_interventions_by_date.call_date < '2024-01-25')

display(df_asda_beiersdorf_p12m_measured_DLA_interventions_by_date_pre)

# COMMAND ----------

df_asda_beiersdorf_p12m_measured_DLA_interventions_by_date_post = df_asda_beiersdorf_p12m_measured_DLA_interventions_by_date.filter(df_asda_beiersdorf_p12m_measured_DLA_interventions_by_date.call_date >= '2024-01-25')

display(df_asda_beiersdorf_p12m_measured_DLA_interventions_by_date_post)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select mdm_country_nm, mdm_holding_nm, mdm_client_nm, objective_typ, concat(year(call_date), '-', format_number(month(call_date),'00')) as year_month, count(measurement_duration), sum(total_intervention_effect), sum(total_impact) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_nm in ('uk')
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
# MAGIC select mdm_country_nm, mdm_holding_nm, mdm_client_nm, concat(year(call_date), '-', format_number(month(call_date),'00')) as year_month, count(measurement_duration), sum(total_intervention_effect), sum(total_impact) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_nm in ('uk')
# MAGIC and
# MAGIC is_complete = 'True'
# MAGIC and
# MAGIC call_date >= '2024-01-01'
# MAGIC AND call_date <= '2024-06-30'
# MAGIC group by mdm_country_nm, mdm_holding_nm, mdm_client_nm, year_month
# MAGIC order by mdm_country_nm, mdm_holding_nm, mdm_client_nm, year_month

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(measurement_duration), sum(total_intervention_effect), sum(total_impact) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- objective_typ = 'DLA'
# MAGIC and
# MAGIC is_complete = 1
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC call_date >= '2023-01-01'
# MAGIC AND call_date <= '2024-12-31'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(measurement_duration), sum(total_intervention_effect), sum(total_impact) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- objective_typ = 'DLA'
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC call_date >= '2023-01-01'
# MAGIC AND call_date <= '2024-12-31'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC select concat(year(call_date), '-', format_number(month(call_date),'00')) as year_month, count(measurement_duration), sum(total_intervention_effect), sum(total_impact) from acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC -- and
# MAGIC -- objective_typ = 'DLA'
# MAGIC and
# MAGIC is_complete = 'true'
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC call_date >= '2023-07-01'
# MAGIC AND call_date <= '2024-06-30'
# MAGIC group by year_month
# MAGIC order by year_month

# COMMAND ----------

sql_query_asda_beiersdorf_p12m_measured_total_interventions_by_date_test = """
    select call_date, count(measurement_duration), sum(total_intervention_effect), sum(total_impact) from acosta_retail_analytics_im.ds_intervention_summary
    where
    mdm_country_id = 30 -- UK
    and
    mdm_client_id = 17686 -- Beiersdorf UK	
    and
    mdm_holding_id = 3257 -- AcostaRetailUK
    and
    is_complete = 'True'
    and
    mdm_banner_id = 7743 -- Asda
    and
    call_date >= '2023-07-01'
    AND call_date <= '2024-06-30'
    group by call_date
    order by call_date
"""

df_asda_beiersdorf_p12m_measured_total_interventions_by_date_test = spark.sql(sql_query_asda_beiersdorf_p12m_measured_total_interventions_by_date_test)

display(df_asda_beiersdorf_p12m_measured_total_interventions_by_date_test)

# COMMAND ----------

df_asda_beiersdorf_p12m_measured_total_interventions_by_date_test_pre = df_asda_beiersdorf_p12m_measured_total_interventions_by_date_test.filter(df_asda_beiersdorf_p12m_measured_total_interventions_by_date_test.call_date < '2024-01-25')

display(df_asda_beiersdorf_p12m_measured_total_interventions_by_date_test_pre)

# COMMAND ----------

sql_query_asda_beiersdorf_p12m_measured_total_interventions_by_date = """
    select call_date, count(measurement_duration), sum(total_intervention_effect), sum(total_impact) from acosta_retail_analytics_im.ds_intervention_summary
    where
    mdm_country_id = 30 -- UK
    and
    mdm_client_id = 17686 -- Beiersdorf UK	
    and
    mdm_holding_id = 3257 -- AcostaRetailUK
    and
    is_complete = 'True'
    and
    mdm_banner_id = 7743 -- Asda
    and
    call_date >= '2023-07-01'
    AND call_date <= '2024-06-30'
    group by call_date
    order by call_date
"""

df_asda_beiersdorf_p12m_measured_total_interventions_by_date = spark.sql(sql_query_asda_beiersdorf_p12m_measured_total_interventions_by_date)

display(df_asda_beiersdorf_p12m_measured_total_interventions_by_date)

# COMMAND ----------

df_asda_beiersdorf_p12m_measured_total_interventions_by_date_pre = df_asda_beiersdorf_p12m_measured_total_interventions_by_date.filter(df_asda_beiersdorf_p12m_measured_total_interventions_by_date.call_date < '2024-01-25')

display(df_asda_beiersdorf_p12m_measured_total_interventions_by_date_pre)

# COMMAND ----------

df_asda_beiersdorf_p12m_measured_total_interventions_by_date_pre_agg = df_asda_beiersdorf_p12m_measured_total_interventions_by_date_pre.summary()

display(df_asda_beiersdorf_p12m_measured_total_interventions_by_date_pre_agg)

# COMMAND ----------

df_asda_beiersdorf_p12m_measured_total_interventions_by_date_post = df_asda_beiersdorf_p12m_measured_total_interventions_by_date.filter(df_asda_beiersdorf_p12m_measured_total_interventions_by_date.call_date >= '2024-01-25')

display(df_asda_beiersdorf_p12m_measured_total_interventions_by_date_post)

# COMMAND ----------

df_asda_beiersdorf_p12m_measured_total_interventions_by_date_post_agg = df_asda_beiersdorf_p12m_measured_total_interventions_by_date_post.summary()

display(df_asda_beiersdorf_p12m_measured_total_interventions_by_date_post_agg)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Science IVM

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_retail_analytics_im.interventions_parameters
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC objective_typ in ('DLA', 'Opportunity')
# MAGIC and
# MAGIC actionable_flg = 'true'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select call_date, count(*) from acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 30 -- UK
# MAGIC and
# MAGIC mdm_client_id = 17686 -- Beiersdorf UK	
# MAGIC and
# MAGIC mdm_holding_id = 3257 -- AcostaRetailUK
# MAGIC and
# MAGIC mdm_banner_id = 7743 -- Asda
# MAGIC and
# MAGIC actionable_flg = 1
# MAGIC -- and
# MAGIC -- objective_typ = 'Opportunity'
# MAGIC AND call_date >= '2023-07-01'
# MAGIC AND call_date <= '2024-06-30'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

sql_query_asda_beiersdorf_p12m_measured_DLA_interventions_by_date_drfe = """
    select call_date, count(measurement_duration), sum(total_intervention_effect), sum(total_impact) from acosta_retail_analytics_im.ds_intervention_summary_20240802_asda_beiersdorf_drfe_ivm_test
    where
    mdm_country_id = 30 -- UK
    and
    mdm_client_id = 17686 -- Beiersdorf UK	
    and
    mdm_holding_id = 3257 -- AcostaRetailUK
    and
    objective_typ = 'DLA'
    and
    is_complete = 'True'
    and
    mdm_banner_id = 7743 -- Asda
    and
    call_date >= '2023-07-01'
    AND call_date <= '2024-06-30'
    group by call_date
    order by call_date
"""

df_asda_beiersdorf_p12m_measured_DLA_interventions_by_date_drfe = spark.sql(sql_query_asda_beiersdorf_p12m_measured_DLA_interventions_by_date_drfe)

display(df_asda_beiersdorf_p12m_measured_DLA_interventions_by_date_drfe)

# COMMAND ----------

df_asda_beiersdorf_p12m_measured_DLA_interventions_by_date_drfe_pre = df_asda_beiersdorf_p12m_measured_DLA_interventions_by_date_drfe.filter(df_asda_beiersdorf_p12m_measured_DLA_interventions_by_date_drfe.call_date < '2024-01-25')

display(df_asda_beiersdorf_p12m_measured_DLA_interventions_by_date_drfe_pre)

# COMMAND ----------

df_asda_beiersdorf_p12m_measured_DLA_interventions_by_date_drfe_post = df_asda_beiersdorf_p12m_measured_DLA_interventions_by_date_drfe.filter(df_asda_beiersdorf_p12m_measured_DLA_interventions_by_date_drfe.call_date >= '2024-01-25')

display(df_asda_beiersdorf_p12m_measured_DLA_interventions_by_date_drfe_post)

# COMMAND ----------

sql_query_asda_beiersdorf_p12m_measured_total_interventions_by_date_drfe = """
    select call_date, count(measurement_duration), sum(total_intervention_effect), sum(total_impact) from acosta_retail_analytics_im.ds_intervention_summary_20240802_asda_beiersdorf_drfe_ivm_test
    where
    mdm_country_id = 30 -- UK
    and
    mdm_client_id = 17686 -- Beiersdorf UK	
    and
    mdm_holding_id = 3257 -- AcostaRetailUK
    and
    mdm_banner_id = 7743 -- Asda
    and
    is_complete = 'True'
    and
    call_date >= '2023-07-01'
    AND call_date <= '2024-06-30'
    group by call_date
    order by call_date
"""

df_asda_beiersdorf_p12m_measured_total_interventions_by_date_drfe = spark.sql(sql_query_asda_beiersdorf_p12m_measured_total_interventions_by_date_drfe)

display(df_asda_beiersdorf_p12m_measured_total_interventions_by_date_drfe)

# COMMAND ----------

df_asda_beiersdorf_p12m_measured_total_interventions_by_date_drfe_pre = df_asda_beiersdorf_p12m_measured_total_interventions_by_date_drfe.filter(df_asda_beiersdorf_p12m_measured_total_interventions_by_date_drfe.call_date < '2024-01-25')

display(df_asda_beiersdorf_p12m_measured_total_interventions_by_date_drfe_pre)

# COMMAND ----------

df_asda_beiersdorf_p12m_measured_total_interventions_by_date_drfe_post = df_asda_beiersdorf_p12m_measured_total_interventions_by_date_drfe.filter(df_asda_beiersdorf_p12m_measured_total_interventions_by_date_drfe.call_date >= '2024-01-25')

display(df_asda_beiersdorf_p12m_measured_total_interventions_by_date_drfe_post)

# COMMAND ----------

sql_query_asda_beiersdorf_p12m_measured_DLA_interventions_by_date_loess = """
    select call_date, count(measurement_duration), sum(total_intervention_effect), sum(total_impact) from acosta_retail_analytics_im.ds_intervention_summary_20240806_asda_beiersdorf_loess_ivm_test
    where
    mdm_country_id = 30 -- UK
    and
    mdm_client_id = 17686 -- Beiersdorf UK	
    and
    mdm_holding_id = 3257 -- AcostaRetailUK
    and
    mdm_banner_id = 7743 -- Asda
    and
    objective_typ = 'DLA'
    and
    is_complete = 'True'
    and
    call_date >= '2023-07-01'
    AND call_date <= '2024-06-30'
    group by call_date
    order by call_date
"""

df_asda_beiersdorf_p12m_measured_DLA_interventions_by_date_loess = spark.sql(sql_query_asda_beiersdorf_p12m_measured_DLA_interventions_by_date_loess)

display(df_asda_beiersdorf_p12m_measured_DLA_interventions_by_date_loess)

# COMMAND ----------

df_asda_beiersdorf_p12m_measured_DLA_interventions_by_date_loess_pre = df_asda_beiersdorf_p12m_measured_DLA_interventions_by_date_loess.filter(df_asda_beiersdorf_p12m_measured_DLA_interventions_by_date_loess.call_date < '2024-01-25')

display(df_asda_beiersdorf_p12m_measured_DLA_interventions_by_date_loess_pre)

# COMMAND ----------

df_asda_beiersdorf_p12m_measured_DLA_interventions_by_date_loess_post = df_asda_beiersdorf_p12m_measured_DLA_interventions_by_date_loess.filter(df_asda_beiersdorf_p12m_measured_DLA_interventions_by_date_loess.call_date >= '2024-01-25')

display(df_asda_beiersdorf_p12m_measured_DLA_interventions_by_date_loess_post)

# COMMAND ----------

sql_query_asda_beiersdorf_p12m_measured_total_interventions_by_date_loess = """
    select call_date, count(measurement_duration), sum(total_intervention_effect), sum(total_impact) from acosta_retail_analytics_im.ds_intervention_summary_20240806_asda_beiersdorf_loess_ivm_test
    where
    mdm_country_id = 30 -- UK
    and
    mdm_client_id = 17686 -- Beiersdorf UK	
    and
    mdm_holding_id = 3257 -- AcostaRetailUK
    and
    mdm_banner_id = 7743 -- Asda
    and
    is_complete = 'True'
    and
    call_date >= '2023-07-01'
    AND call_date <= '2024-06-30'
    group by call_date
    order by call_date
"""

df_asda_beiersdorf_p12m_measured_total_interventions_by_date_loess = spark.sql(sql_query_asda_beiersdorf_p12m_measured_total_interventions_by_date_loess)

display(df_asda_beiersdorf_p12m_measured_total_interventions_by_date_loess)

# COMMAND ----------

df_asda_beiersdorf_p12m_measured_total_interventions_by_date_loess_pre = df_asda_beiersdorf_p12m_measured_total_interventions_by_date_loess.filter(df_asda_beiersdorf_p12m_measured_total_interventions_by_date_loess.call_date < '2024-01-25')

display(df_asda_beiersdorf_p12m_measured_total_interventions_by_date_loess_pre)

# COMMAND ----------

df_asda_beiersdorf_p12m_measured_total_interventions_by_date_loess_pre_agg = df_asda_beiersdorf_p12m_measured_total_interventions_by_date_loess_pre.summary()

display(df_asda_beiersdorf_p12m_measured_total_interventions_by_date_loess_pre_agg)

# COMMAND ----------

df_asda_beiersdorf_p12m_measured_total_interventions_by_date_loess_post = df_asda_beiersdorf_p12m_measured_total_interventions_by_date_loess.filter(df_asda_beiersdorf_p12m_measured_total_interventions_by_date_loess.call_date >= '2024-01-25')

display(df_asda_beiersdorf_p12m_measured_total_interventions_by_date_loess_post)

# COMMAND ----------

df_asda_beiersdorf_p12m_measured_total_interventions_by_date_loess_post_agg = df_asda_beiersdorf_p12m_measured_total_interventions_by_date_loess_post.summary()

display(df_asda_beiersdorf_p12m_measured_total_interventions_by_date_loess_post_agg)

# COMMAND ----------


