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
today_date = datetime.datetime(2024, 8, 1, 0, 0)
###################### TEMP CODE ######################

min_date = today_date - relativedelta(months=7)
max_date = today_date - relativedelta(days=6)

min_date_filter = min_date.strftime(format="%Y-%m-%d")
max_date_filter = max_date.strftime(format="%Y-%m-%d")

print(min_date_filter)
print(max_date_filter)

# COMMAND ----------

team_alerts_query = "SELECT * FROM batch_control.dla_team_alert_dependency_config"

df_team_alerts = spark.sql(team_alerts_query)

display(df_team_alerts)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.interventions_retailer_client_config

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.interventions_retailer_client_config_gen2_migration

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM batch_control.infomart_param_config
# MAGIC where param_key = 'forecast_version'
# MAGIC order by param_value, ACOSTA_CUSTOMER

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

pandas_df_team_alerts = df_team_alerts.toPandas()

result_list_stores = []
result_list_items = []
result_list_inv = []
result_list_osa = []
result_list_osa_les = []

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

    # Exception for Sainsburys UK BAT (British American Tobacco) which isn't set up yet
    if (retail_client == 'sainsburys_bat_uk'):
        result_list_stores.append(0)
        result_list_items.append(0)
        result_list_inv.append(0)
        result_list_osa.append(0)
        result_list_osa_les.append(0)
        continue

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

    query_osa = f"""
        (SELECT COUNT(*) FROM {team_alert_im}.alert_on_shelf_availability
         WHERE Retail_Client = '{retail_client}'
         AND sales_dt >= '{min_date_filter}'
         AND sales_dt <= '{max_date_filter}')
    """

    query_osa_les = f"""
        (SELECT COUNT(*) FROM {team_alert_im}.alert_on_shelf_availability
         WHERE Retail_Client = '{retail_client}'
         AND ALERT_TYPE_NM = 'LowExpectedSales'
         AND sales_dt >= '{min_date_filter}'
         AND sales_dt <= '{max_date_filter}')
    """

    result_stores = spark.sql(query_num_stores).collect()[0][0]
    result_list_stores.append(result_stores)

    result_items = spark.sql(query_num_items).collect()[0][0]
    result_list_items.append(result_items)

    result_inv = spark.sql(query_inv).collect()[0][0]
    result_list_inv.append(result_inv)

    result_osa = spark.sql(query_osa).collect()[0][0]
    result_list_osa.append(result_osa)

    result_osa_les = spark.sql(query_osa_les).collect()[0][0]
    result_list_osa_les.append(result_osa_les)

# COMMAND ----------

# Create a DataFrame from the list
pandas_df_team_alerts['num_stores_2024'] = result_list_stores
pandas_df_team_alerts['num_items_2024'] = result_list_items
pandas_df_team_alerts['num_inv_alerts_2024'] = result_list_inv
pandas_df_team_alerts['num_osa_alerts_2024'] = result_list_osa
pandas_df_team_alerts['num_osa_les_alerts_2024'] = result_list_osa_les

df_team_alerts_final = spark.createDataFrame(pandas_df_team_alerts) 

df_team_alerts_final = df_team_alerts_final.filter(pyf.col('num_stores_2024') > 0)

display(df_team_alerts_final)

# COMMAND ----------

candidate_alerts_query = "SELECT * FROM acosta_retail_analytics_im.interventions_retailer_client_config_gen2_migration where alertgen_im_db_nm <> ''"

df_candidate_alerts = spark.sql(candidate_alerts_query)
df_candidate_alerts = df_candidate_alerts.drop("active_flg", "audit_create_ts", "audit_update_ts", "vm_type")

df_candidate_alerts = df_candidate_alerts.filter(pyf.col('mdm_client_id') != 16343) # GeneralMills UK
df_candidate_alerts = df_candidate_alerts.filter(pyf.col('mdm_client_id') != 13429) # NestleWatersN.A
df_candidate_alerts = df_candidate_alerts.filter(pyf.col('mdm_client_id') != 15599) # Snyder's-Lance,Inc (aka Campbells Snack)
df_candidate_alerts = df_candidate_alerts.filter(pyf.col('mdm_client_id') != 13054) # NestleCoffeePartners (aka Starbucks)
df_candidate_alerts = df_candidate_alerts.filter(pyf.col('mdm_client_id') != 17683) # Premier Foods UK

df_candidate_alerts = df_candidate_alerts.filter(pyf.col('alertgen_im_db_nm') != "retail_alert_boots_perfettivanmelle_uk_im") # Boots Perfetti

display(df_candidate_alerts)

# COMMAND ----------

columns = df_candidate_alerts.columns

vals = [
  (2, "ca", 2596, "Walmart Canada", None, None, None, None, "retaillink_walmart_bandgfoods_ca_dv", "retail_alert_walmart_bandgfoods_ca_im", True),
  (2, "ca", 2596, "Walmart Canada", None, None, None, None, "retaillink_walmart_minutemaidcompany_ca_dv", "retail_alert_walmart_minutemaidcompany_ca_im", True),
  (2, "ca", 2596, "Walmart Canada", None, None, None, None, "retaillink_walmart_rbhealth_ca_dv", "retail_alert_walmart_rbhealth_ca_im", True),
  (2, "ca", 2596, "Walmart Canada", None, None, None, None, "retaillink_walmart_treehouse_ca_dv", "retail_alert_walmart_treehouse_ca_im", True),
  (2, "ca", 2596, "Walmart Canada", None, None, 16317, "Voortman Cookies Limited CA", "retaillink_walmart_voortman_ca_dv", "retail_alert_walmart_voortman_ca_im", True),
  (2, "ca", 2596, "Walmart Canada", None, None, 17444, "Danone Canada", "retaillink_walmart_danone_ca_dv", "retail_alert_walmart_danone_ca_im", True),

  (30, "uk", 3257, "AcostaRetailUK", 7745, "Sainsburys", 16320, "Nestle UK", "circana_sainsburys_nestlecore_uk_dv", "retail_alert_sainsburys_nestlecore_uk_im", False),
  (30, "uk", 3257, "AcostaRetailUK", 7745, "Sainsburys", 17713, "Perfetti Van Melle UK", "circana_sainsburys_perfettivanmelle_uk_dv", "retail_alert_sainsburys_nestlecore_uk_im", False),

  (1, "us", 91, "KrogerCo", None, None, 17694, "Bluetriton Brands, Inc.", "market6_kroger_bluetriton_us_dv", "retail_alert_kroger_bluetriton_us_im", True),
  (1, "us", 71, "Wal-Mart", None, None, 882, "DanoneU.S.LLC", "retaillink_walmart_danoneusllc_us_dv", "retail_alert_walmart_danoneusllc_us_im", True),
  (1, "us", 71, "Wal-Mart", None, None, 16269, "Sanofi", "retaillink_walmart_sanofi_us_dv", "retail_alert_walmart_sanofi_us_im", True)
]

df_candidate_alerts_additional_ca_and_uk_clients = spark.createDataFrame(vals, columns)

df_candidate_alerts = df_candidate_alerts.union(df_candidate_alerts_additional_ca_and_uk_clients)

display(df_candidate_alerts)

# COMMAND ----------

pandas_df_candidate_alerts = df_candidate_alerts.toPandas()

result_list_alert_inventory_cleanup = []
result_list_alert_on_shelf_availability = []
result_list_alert_osa_invalid_alerts = []
result_list_alert_osa_low_expected_sale = []

for row in df_candidate_alerts.rdd.collect():
    candidate_alert_im = row["alertgen_im_db_nm"]
    mdm_country_id = row["mdm_country_id"]

    # Exception for non-UK clients, which only have inventory_cleanup and on_shelf_availability alerts
    if (mdm_country_id < 30):
        query_alert_inventory_cleanup = f"""
            (SELECT COUNT(*) FROM {candidate_alert_im}.alert_inventory_cleanup
            WHERE sales_dt >= '{min_date_filter}'
            AND sales_dt <= '{max_date_filter}')
        """

        query_alert_on_shelf_availability = f"""
            (SELECT COUNT(*) FROM {candidate_alert_im}.alert_on_shelf_availability
            WHERE sales_dt >= '{min_date_filter}'
            AND sales_dt <= '{max_date_filter}')
        """

        result_alert_inventory_cleanup = spark.sql(query_alert_inventory_cleanup).collect()[0][0]
        result_list_alert_inventory_cleanup.append(result_alert_inventory_cleanup)

        result_alert_on_shelf_availability = spark.sql(query_alert_on_shelf_availability).collect()[0][0]
        result_list_alert_on_shelf_availability.append(result_alert_on_shelf_availability)

        result_list_alert_osa_invalid_alerts.append(0)
        result_list_alert_osa_low_expected_sale.append(0)

        continue

    query_alert_inventory_cleanup = f"""
        (SELECT COUNT(*) FROM {candidate_alert_im}.alert_inventory_cleanup
         WHERE sales_dt >= '{min_date_filter}'
         AND sales_dt <= '{max_date_filter}')
    """

    query_alert_on_shelf_availability = f"""
        (SELECT COUNT(*) FROM {candidate_alert_im}.alert_on_shelf_availability
         WHERE sales_dt >= '{min_date_filter}'
         AND sales_dt <= '{max_date_filter}')
    """

    query_alert_osa_invalid_alerts = f"""
        (SELECT COUNT(*) FROM {candidate_alert_im}.alert_osa_invalid_alerts
         WHERE sales_dt >= '{min_date_filter}'
         AND sales_dt <= '{max_date_filter}')
    """

    query_alert_osa_low_expected_sale = f"""
        (SELECT COUNT(*) FROM {candidate_alert_im}.alert_osa_low_expected_sale
         WHERE sales_dt >= '{min_date_filter}'
         AND sales_dt <= '{max_date_filter}')
    """

    result_alert_inventory_cleanup = spark.sql(query_alert_inventory_cleanup).collect()[0][0]
    result_list_alert_inventory_cleanup.append(result_alert_inventory_cleanup)

    result_alert_on_shelf_availability = spark.sql(query_alert_on_shelf_availability).collect()[0][0]
    result_list_alert_on_shelf_availability.append(result_alert_on_shelf_availability)

    result_alert_osa_invalid_alerts = spark.sql(query_alert_osa_invalid_alerts).collect()[0][0]
    result_list_alert_osa_invalid_alerts.append(result_alert_osa_invalid_alerts)
    
    result_alert_osa_low_expected_sale = spark.sql(query_alert_osa_low_expected_sale).collect()[0][0]
    result_list_alert_osa_low_expected_sale.append(result_alert_osa_low_expected_sale)

# COMMAND ----------

# Create a DataFrame from the list
pandas_df_candidate_alerts['num_candidate_inv_alerts_2024'] = result_list_alert_inventory_cleanup
pandas_df_candidate_alerts['num_candidate_osa_alerts_2024'] = result_list_alert_on_shelf_availability
pandas_df_candidate_alerts['num_candidate_invalid_osa_alerts_2024'] = result_list_alert_osa_invalid_alerts
pandas_df_candidate_alerts['num_candidate_low_expected_sales_osa_alerts_2024'] = result_list_alert_osa_low_expected_sale

df_candidate_alerts_final = spark.createDataFrame(pandas_df_candidate_alerts)

display(df_candidate_alerts_final)

# COMMAND ----------

df_team_and_candidate_alerts_final = df_team_alerts_final.join(df_candidate_alerts_final, df_team_alerts_final.epos_datavault_db_nm == df_candidate_alerts_final.epos_datavault_db_nm, "left_outer")

display(df_team_and_candidate_alerts_final)

# COMMAND ----------

export_path = '/mnt/artifacts/reference/dla_team_alert_counts/'

df_candidate_alerts_final \
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


