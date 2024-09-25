# Databricks notebook source
import datetime
import warnings
from dateutil.relativedelta import relativedelta

import pyspark.sql.functions as pyf
import pyspark.sql.types as pyt
from pyspark.sql.window import Window

# COMMAND ----------

# Import only recent data (currently set to most recent 6 months)
today_date = datetime.date.today()

###################### TEMP CODE ######################
today_date = datetime.datetime(2023, 12, 8, 0, 0)
###################### TEMP CODE ######################

min_date = today_date - relativedelta(months=3)
max_date = today_date - relativedelta(days=1)

min_date_filter = "2023-01-01"
max_date_filter = "2023-12-31"

print(min_date_filter)
print(max_date_filter)

# COMMAND ----------

team_alerts_query = 'SELECT * FROM batch_control.dla_team_alert_dependency_config where dependency_acosta_customer like "sainsburys%" and AUDIT_CREATE_TS >= "2022-01-01"'

df_team_alerts = spark.sql(team_alerts_query)

display(df_team_alerts)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.interventions_retailer_client_config_gen2_migration
# MAGIC where mdm_banner_id = 7745

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM batch_control.infomart_param_config
# MAGIC where param_key = 'forecast_version'
# MAGIC order by param_value, ACOSTA_CUSTOMER

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM batch_control.dla_team_alert_base_config

# COMMAND ----------

df_team_alerts = df_team_alerts.withColumn("team_alert_im", pyf.concat(pyf.lit("team_retail_alert_"), pyf.col("TEAM_NM"), pyf.lit("_"), pyf.col("TEAM_COUNTRY"), pyf.lit("_im")))

df_team_alerts = df_team_alerts.withColumn("RETAIL_CLIENT", pyf.concat(pyf.col("DEPENDENCY_ACOSTA_CUSTOMER"), pyf.lit("_"), pyf.col("DEPENDENCY_ACOSTA_CUSTOMER_COUNTRY")))

df_team_alerts = df_team_alerts.withColumn("retailer", pyf.split(pyf.col("DEPENDENCY_ACOSTA_CUSTOMER"), "_").getItem(0))
df_team_alerts = df_team_alerts.withColumn("client", pyf.split(pyf.col("DEPENDENCY_ACOSTA_CUSTOMER"), "_").getItem(1))

df_team_alerts = df_team_alerts.withColumn("ACOSTA_CUSTOMER", pyf.concat(pyf.col("retailer"), pyf.lit("-"), pyf.col("client")))

display(df_team_alerts)

# COMMAND ----------

source_system_query = "select distinct SYSTEM_NM_ABBR, ACOSTA_CUSTOMER from batch_control.application_component_to_acostacustomer_mapping where APPLICATION_NM = 'Data Led Alerts' and SYSTEM_NM_ABBR not in ('tescolink', '8451stratum') order by ACOSTA_CUSTOMER"

df_source_system = spark.sql(source_system_query)

display(df_source_system)

# COMMAND ----------

# Define the join conditions
join_conditions = ['ACOSTA_CUSTOMER']
df_team_alerts = df_team_alerts.join(df_source_system, on=join_conditions, how='inner')

df_team_alerts = df_team_alerts.withColumn("epos_datavault_db_nm", pyf.concat(pyf.col("SYSTEM_NM_ABBR"), pyf.lit("_"), pyf.col("RETAIL_CLIENT"), pyf.lit("_dv")))

display(df_team_alerts)

# COMMAND ----------

mdm_ids_query = "SELECT epos_datavault_db_nm, mdm_country_id, mdm_holding_id, mdm_banner_id, mdm_client_id FROM acosta_retail_analytics_im.interventions_retailer_client_config_gen2_migration where mdm_banner_id = 7745"

df_mdm_ids = spark.sql(mdm_ids_query)

display(df_mdm_ids)

# COMMAND ----------

# Define the join conditions
join_conditions = ['epos_datavault_db_nm']
df_team_alerts = df_team_alerts.join(df_mdm_ids, on=join_conditions, how='inner')

display(df_team_alerts)

# COMMAND ----------

pandas_df_team_alerts = df_team_alerts.toPandas()

result_list_start_date = []
result_list_stores = []
result_list_items = []
result_list_inv = []
result_list_osa = []
result_list_inv_lsv = []
result_list_osa_lsv = []

for row in df_team_alerts.rdd.collect():
    team_alert_im = row["team_alert_im"]
    retail_client = row["RETAIL_CLIENT"]
    epos_datavault_db = row["epos_datavault_db_nm"]

    # Exception for Kroger Bluetriton
    if (epos_datavault_db == 'market6_kroger_bluetriton_us_dv'):
        epos_datavault_db = '8451stratum_kroger_bluetriton_us_dv'

    # Exception for Walmart Kens DRT
    if (team_alert_im == 'team_retail_alert_kens_walmart_drt_us_im'):
        team_alert_im = 'team_retail_alert_kens_drt_us_im'

    # # Exception for Sainsburys UK BAT (British American Tobacco) which isn't set up yet
    # if (retail_client == 'sainsburys_bat_uk'):
    #     result_list_stores.append(0)
    #     result_list_items.append(0)
    #     result_list_inv.append(0)
    #     result_list_osa.append(0)
    #     continue

    query_start_date = f"""
        (SELECT min(sales_dt) FROM {team_alert_im}.alert_on_shelf_availability
         WHERE Retail_Client = '{retail_client}'
         AND sales_dt >= '{min_date_filter}'
         AND sales_dt <= '{max_date_filter}')
    """

    query_num_stores = f"""
        (SELECT COUNT(distinct organization_unit_num) FROM {epos_datavault_db}.vw_latest_sat_epos_summary
         WHERE sales_dt >= '{min_date_filter}'
         AND sales_dt <= '{max_date_filter}')
    """

    query_num_items = f"""
        (SELECT COUNT(distinct retailer_item_id) FROM {epos_datavault_db}.vw_latest_sat_epos_summary
         WHERE sales_dt >= '{min_date_filter}'
         AND sales_dt <= '{max_date_filter}')
    """

    query_inv = f"""
        (SELECT COUNT(*) FROM {team_alert_im}.alert_inventory_cleanup
         WHERE Retail_Client = '{retail_client}'
         AND sales_dt >= '{min_date_filter}'
         AND sales_dt <= '{max_date_filter}')
    """

    query_inv_lsv = f"""
        (SELECT SUM(LOST_SALES_AMT) FROM {team_alert_im}.alert_inventory_cleanup
         WHERE Retail_Client = '{retail_client}'
         AND sales_dt >= '{min_date_filter}'
         AND sales_dt <= '{max_date_filter}')
    """

    query_osa = f"""
        (SELECT COUNT(*) FROM {team_alert_im}.alert_on_shelf_availability
         WHERE Retail_Client = '{retail_client}'
         AND sales_dt >= '{min_date_filter}'
         AND sales_dt <= '{max_date_filter}')
    """

    query_osa_lsv = f"""
        (SELECT sum(LOST_SALES_AMT) FROM {team_alert_im}.alert_on_shelf_availability
         WHERE Retail_Client = '{retail_client}'
         AND sales_dt >= '{min_date_filter}'
         AND sales_dt <= '{max_date_filter}')
    """

    result_start_date = spark.sql(query_start_date).collect()[0][0]
    result_list_start_date.append(result_start_date)

    result_stores = spark.sql(query_num_stores).collect()[0][0]
    result_list_stores.append(result_stores)

    result_items = spark.sql(query_num_items).collect()[0][0]
    result_list_items.append(result_items)

    result_inv = spark.sql(query_inv).collect()[0][0]
    result_list_inv.append(result_inv)

    result_osa = spark.sql(query_osa).collect()[0][0]
    result_list_osa.append(result_osa)

    result_inv_lsv = spark.sql(query_inv_lsv).collect()[0][0]
    result_list_inv_lsv.append(result_inv_lsv)

    result_osa_lsv = spark.sql(query_osa_lsv).collect()[0][0]
    result_list_osa_lsv.append(result_osa_lsv)

# Create a DataFrame from the list

pandas_df_team_alerts['alert_start_date'] = result_list_start_date
pandas_df_team_alerts['num_stores_2023'] = result_list_stores
pandas_df_team_alerts['num_items_2023'] = result_list_items
pandas_df_team_alerts['num_inv_alerts_2023'] = result_list_inv
pandas_df_team_alerts['num_osa_alerts_2023'] = result_list_osa
pandas_df_team_alerts['total_inv_lsv_2023'] = result_list_inv_lsv
pandas_df_team_alerts['total_osa_lsv_2023'] = result_list_osa_lsv

df_team_alerts_final = spark.createDataFrame(pandas_df_team_alerts) 

df_team_alerts_final = df_team_alerts_final.withColumn("num_alerts_2023", pyf.col('num_inv_alerts_2023') + pyf.col('num_osa_alerts_2023'))
df_team_alerts_final = df_team_alerts_final.withColumn("total_lsv_2023", pyf.col('total_inv_lsv_2023') + pyf.col('total_osa_lsv_2023'))

display(df_team_alerts_final)

# COMMAND ----------

# MAGIC   %sql
# MAGIC
# MAGIC   SELECT COUNT(distinct organization_unit_num), count(DISTINCT retailer_item_id) FROM horizon_sainsburys_nestlecore_uk_dv.vw_latest_sat_epos_summary
# MAGIC     where sales_dt >= '2023-01-01'
# MAGIC     AND sales_dt <= '2023-12-31'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc sainsburys_nestlecore_uk_dv.vw_sat_link_epos_summary

# COMMAND ----------

# MAGIC   %sql
# MAGIC
# MAGIC   SELECT COUNT(distinct HUB_ORGANIZATION_UNIT_HK), count(DISTINCT HUB_RETAILER_ITEM_HK) FROM sainsburys_nestlecore_uk_dv.vw_sat_link_epos_summary
# MAGIC     where sales_dt >= '2023-01-01'
# MAGIC     AND sales_dt <= '2023-12-31'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc nestlecore_drt_uk_team_alerts_im.alert_on_shelf_availability

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from nestlecore_drt_uk_team_alerts_im.alert_on_shelf_availability
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select max(sales_dt), count(*), sum(LOST_SALES_AMT) from team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
# MAGIC     where Retail_Client = "sainsburys_nestlecore_uk"
# MAGIC     AND sales_dt >= '2023-01-01'
# MAGIC     AND sales_dt <= '2023-12-31'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select sales_dt, count(*), sum(LOST_SALES_AMT) from team_retail_alert_nestlecore_drt_uk_im.alert_on_shelf_availability
# MAGIC     where Retail_Client = "sainsburys_nestlecore_uk"
# MAGIC     AND sales_dt >= '2023-01-01'
# MAGIC     AND sales_dt <= '2023-12-31'
# MAGIC group by sales_dt
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(sales_dt), count(*), sum(LOST_SALES_AMT) from team_retail_alert_nestlecore_drt_uk_im.alert_inventory_cleanup
# MAGIC     where Retail_Client = "sainsburys_nestlecore_uk"
# MAGIC     AND sales_dt >= '2023-01-01'
# MAGIC     AND sales_dt <= '2023-12-31'

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(sales_dt), count(*), sum(LOST_SALES_AMT) from nestlecore_drt_uk_team_alerts_im.alert_on_shelf_availability
# MAGIC     where Retail_Client = "sainsburys_nestlecore_uk"
# MAGIC     AND sales_dt >= '2023-01-01'
# MAGIC     AND sales_dt <= '2023-12-31'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select  max(sales_dt), count(*), sum(LOST_SALES_AMT) from nestlecore_drt_uk_team_alerts_im.alert_inventory_cleanup
# MAGIC     where Retail_Client = "sainsburys_nestlecore_uk"
# MAGIC     AND sales_dt >= '2023-01-01'
# MAGIC     AND sales_dt <= '2023-12-31'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables in batch_control

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from batch_control.application_component_to_acostacustomer_mapping
# MAGIC where APPLICATION_NM = 'Data Led Alerts'
# MAGIC -- and COMPONENT_NM = 'Ingestion'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from batch_control.source_to_target_columnmapping_config

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from batch_control.dla_team_alert_base_config

# COMMAND ----------

export_path = '/mnt/artifacts/reference/dla_team_alert_dependency_config_sainsburys/'

df_team_alerts_final \
    .write.format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .save(export_path)

# COMMAND ----------

# query = "SELECT * FROM acosta_retail_analytics_im.interventions_retailer_client_config_gen2_migration"

# df_result = spark.sql(query)

# export_path = '/mnt/artifacts/reference/interventions_retailer_client_config_gen2_migration/'

# df_result \
#     .write.format('delta') \
#     .mode('overwrite') \
#     .option('overwriteSchema', 'true') \
#     .save(export_path)

# COMMAND ----------


