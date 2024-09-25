# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### TODO: Connect this to alerting info

# COMMAND ----------

# 4175784672492281 is the Gen2 DEV Data Science Databricks Workspace (eus2d-dbw-dp-im-retailfcsteng), which is currently not used
# 3174563515979991 is the Gen2 PROD DLA Alert-Gen Databricks Workspace (eus2p-dbw-dp-im-alertgen)

# Meijer and Target data ingestion jobs do not have identifiers in u.custom_tags.dbr_tags

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   u.account_id,
# MAGIC   u.workspace_id,
# MAGIC   u.sku_name,
# MAGIC   u.cloud,
# MAGIC   u.custom_tags,
# MAGIC   u.custom_tags.dbr_tags,
# MAGIC   u.usage_start_time,
# MAGIC   u.usage_end_time,
# MAGIC   u.usage_date,
# MAGIC   date_format(u.usage_date, 'yyyy-MM') as YearMonth,
# MAGIC   u.usage_unit,
# MAGIC   u.usage_quantity,
# MAGIC   lp.pricing.default as list_price,
# MAGIC   lp.pricing.default * u.usage_quantity as list_cost,
# MAGIC   u.usage_metadata.*
# MAGIC from
# MAGIC   system.billing.usage u 
# MAGIC   inner join system.billing.list_prices lp on u.cloud = lp.cloud and
# MAGIC     u.sku_name = lp.sku_name and
# MAGIC     u.usage_start_time >= lp.price_start_time and
# MAGIC     (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)
# MAGIC
# MAGIC where
# MAGIC --   usage_metadata.job_id in ('1023227822628161', '110744752182754', '913197950211972', '736054730536366')
# MAGIC -- and 
# MAGIC workspace_id = '4175784672492281'
# MAGIC -- and usage_date >= '2023-10-27'

# COMMAND ----------

# Note: Data platform changed spark.sql.shuffle.partitions to auto for the value measurement linked service on 2023-11-23

# COMMAND ----------

import datetime
import warnings
from dateutil.relativedelta import relativedelta

import pyspark.sql.functions as pyf
import pyspark.sql.types as pyt
from pyspark.sql.window import Window

# COMMAND ----------

def check_path_exists(path, file_format, errors='raise'):
    """
    Gets a path to a file with the file format and return boolean
    :param path: a path to the file
    :param file_format: format of the file
    :param errors: must be either 'raise' or 'ignore'
    :return: boolean
    """
    from pyspark.sql.utils import AnalysisException
    from pyspark.sql.session import SparkSession

    spark = SparkSession.builder.getOrCreate()

    try:
        _ = spark.read.format(file_format).load(path)
        does_exist = True
    except AnalysisException:
        does_exist = False

    if 'ra' in errors.lower() and does_exist is False:
        raise ValueError('The `{}` does not exist'.format(path))

    return does_exist

# COMMAND ----------

# Import only recent data (currently set to most recent 3 months)
today_date = datetime.date.today()
min_date = today_date - relativedelta(months=3)
min_date_filter = min_date.strftime(format="%Y-%m-%d")
print(min_date_filter)

spark.conf.set("abc.min_date_filter", min_date_filter)

# COMMAND ----------

import_path = '/mnt/prod-ro/artifacts/reference/dla_team_alert_dependency_config/'

import_path_exists_flag = check_path_exists(import_path, 'delta', 'ignore')

if import_path_exists_flag:
    df_dla_team_alert_dependency_config = spark.read.format('delta').load(import_path)

    display(df_dla_team_alert_dependency_config)

# import_path = '/mnt/prod-ro/artifacts/reference/interventions_retailer_client_config_gen2_migration/'

# df_interventions_retailer_client_config_gen2_migration = spark.read.format('delta').load(import_path)

# display(df_interventions_retailer_client_config_gen2_migration)

# COMMAND ----------

df_retailer_client_cols = [
    'TEAM_TYP',
    'ACTIVE_FLG',
    'DEPENDENCY_ACOSTA_CUSTOMER',
    'DEPENDENCY_ACOSTA_CUSTOMER_COUNTRY',
    'GLOBAL_CONNECT_MANUFACTURER_ID',
    'GLOBAL_CONNECT_PARENT_CHAIN_ID',
    'num_inv_alerts_P3M',
    'num_osa_alerts_P3M'
]

df_retailer_client = df_dla_team_alert_dependency_config.select(df_retailer_client_cols).distinct()
df_retailer_client = df_retailer_client.withColumn("retailer", pyf.split(pyf.col("DEPENDENCY_ACOSTA_CUSTOMER"), "_").getItem(0))
df_retailer_client = df_retailer_client.withColumn("client", pyf.split(pyf.col("DEPENDENCY_ACOSTA_CUSTOMER"), "_").getItem(1))

df_retailer_client = df_retailer_client.withColumnRenamed('DEPENDENCY_ACOSTA_CUSTOMER_COUNTRY', 'country').drop('DEPENDENCY_ACOSTA_CUSTOMER')
df_retailer_client = df_retailer_client.withColumnRenamed('TEAM_TYP', 'team')
df_retailer_client = df_retailer_client.withColumnRenamed('ACTIVE_FLG', 'DLA_active_flg')
df_retailer_client = df_retailer_client.withColumnRenamed('GLOBAL_CONNECT_MANUFACTURER_ID', 'manufacturer_id')
df_retailer_client = df_retailer_client.withColumnRenamed('GLOBAL_CONNECT_PARENT_CHAIN_ID', 'parent_chain_id')
df_retailer_client = df_retailer_client.select('retailer', 'client', 'country', 'team', 'manufacturer_id', 'parent_chain_id', 'DLA_active_flg', 'num_inv_alerts_P3M', 'num_osa_alerts_P3M')

display(df_retailer_client)

# COMMAND ----------

# Select only the relevant columns and use distinct to get unique values
unique_retailers = df_retailer_client.select("retailer").distinct()
unique_clients = df_retailer_client.select("client").distinct()
unique_teams = df_retailer_client.select("team").distinct()

# Convert the result to lists
retailers = [row['retailer'] for row in unique_retailers.collect()]
clients = [row['client'] for row in unique_clients.collect()]
teams = [row['team'] for row in unique_teams.collect()]

retailers = sorted(retailers, key=len, reverse=True)
clients = sorted(clients, key=len, reverse=True)
teams = sorted(teams, key=len, reverse=True)

print("Unique Retailers:", retailers)
print("Unique Clients:", clients)
print("Unique Teams:", teams)

systems = [
  'acosta',
  'premium360',
  'rex',
  'tracfonewirelessinc',
  'all'
]

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in system.compute

# COMMAND ----------

# MAGIC %sql
# MAGIC desc system.compute.clusters

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from
# MAGIC   system.compute.clusters c
# MAGIC where
# MAGIC   cluster_id = '1106-230300-i6uo4mac'
# MAGIC and workspace_id = '6241648350210293'

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from
# MAGIC   system.compute.clusters c
# MAGIC where
# MAGIC   cluster_id in (
# MAGIC     '1105-071242-zt1y3c5u',
# MAGIC     '1105-062933-jxvpgryn',
# MAGIC     '1105-054525-co68pb3o',
# MAGIC     '1105-050531-z99ta3hl'
# MAGIC   )
# MAGIC and workspace_id = '6241648350210293'

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from system.compute.warehouse_events

# COMMAND ----------

# MAGIC %sql
# MAGIC desc system.billing.usage

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from system.billing.list_prices
# MAGIC where cloud = 'AZURE'
# MAGIC and sku_name in ('PREMIUM_JOBS_COMPUTE', 'PREMIUM_ALL_PURPOSE_COMPUTE')
# MAGIC order by sku_name, price_start_time desc

# COMMAND ----------

# MAGIC %md
# MAGIC ### Section 1 - Data Science Processes (DRFE and IVM)

# COMMAND ----------

# MAGIC %sql
# MAGIC select usage_date, count(distinct usage_metadata.job_id)
# MAGIC from
# MAGIC   system.billing.usage u 
# MAGIC where
# MAGIC   usage_metadata.job_id is not Null
# MAGIC and workspace_id = '6241648350210293'
# MAGIC group by usage_date
# MAGIC order by usage_date

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from
# MAGIC   system.billing.usage u 
# MAGIC   inner join system.billing.list_prices lp on u.cloud = lp.cloud and
# MAGIC     u.sku_name = lp.sku_name and
# MAGIC     u.usage_start_time >= lp.price_start_time and
# MAGIC     (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)
# MAGIC
# MAGIC where
# MAGIC   usage_metadata.job_id is not Null
# MAGIC and workspace_id = '6241648350210293'
# MAGIC and usage_date >= "${abc.min_date_filter}"

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct usage_metadata.job_id)
# MAGIC from
# MAGIC   system.billing.usage u 
# MAGIC   inner join system.billing.list_prices lp on u.cloud = lp.cloud and
# MAGIC     u.sku_name = lp.sku_name and
# MAGIC     u.usage_start_time >= lp.price_start_time and
# MAGIC     (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)
# MAGIC
# MAGIC where
# MAGIC   usage_metadata.job_id is not Null
# MAGIC and workspace_id = '6241648350210293'
# MAGIC and usage_date >= "${abc.min_date_filter}"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM
# MAGIC   system.billing.usage u 
# MAGIC   INNER JOIN system.billing.list_prices lp ON u.cloud = lp.cloud AND
# MAGIC     u.sku_name = lp.sku_name AND
# MAGIC     u.usage_start_time >= lp.price_start_time AND
# MAGIC     (u.usage_end_time <= lp.price_end_time OR lp.price_end_time IS NULL)
# MAGIC WHERE
# MAGIC usage_metadata.job_id IS NOT NULL AND workspace_id in (
# MAGIC     '6506617936411518', -- retaillink databricks workspace
# MAGIC     '1503220589847731', -- market6 databricks workspace
# MAGIC     '8173526553309', -- bigred databricks workspace
# MAGIC     '3394176885759303', -- tescopartnertoolkit databricks workspace
# MAGIC     '5294095351256433', -- msd databricks workspace
# MAGIC     '8214564272272557', -- horizon databricks workspace
# MAGIC     '5996698886943388', -- rsi databricks workspace
# MAGIC     '4478472492095652' -- vendornet databricks workspace
# MAGIC   )
# MAGIC   AND custom_tags.dbr_tags IS NOT NULL AND usage_date >= "${abc.min_date_filter}"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   COUNT(DISTINCT CASE WHEN usage_metadata.job_id IS NOT NULL AND workspace_id = '6241648350210293' AND custom_tags.dbr_tags LIKE '%Forecast DRFE%' AND usage_date >= "${abc.min_date_filter}" THEN usage_metadata.job_id END) AS total_DRFE_forecast_jobs,
# MAGIC
# MAGIC   COUNT(DISTINCT CASE WHEN usage_metadata.job_id IS NOT NULL AND workspace_id = '6241648350210293' AND custom_tags.dbr_tags LIKE "%value measurement%" 
# MAGIC   AND usage_date >= "${abc.min_date_filter}" THEN usage_metadata.job_id END) AS total_value_measurement_jobs,
# MAGIC
# MAGIC   COUNT(DISTINCT CASE WHEN usage_metadata.job_id IS NOT NULL AND workspace_id = '6241648350210293' AND custom_tags.dbr_tags IS NULL
# MAGIC   AND usage_date >= "${abc.min_date_filter}" THEN usage_metadata.job_id END) AS manual_data_science_jobs,
# MAGIC
# MAGIC   COUNT(DISTINCT CASE WHEN usage_metadata.job_id IS NOT NULL AND workspace_id = '3174563515979991' AND custom_tags.dbr_tags <> " "
# MAGIC   AND usage_date >= "${abc.min_date_filter}" THEN usage_metadata.job_id END) AS total_DLA_alert_gen_jobs,
# MAGIC
# MAGIC   COUNT(DISTINCT CASE WHEN usage_metadata.job_id IS NOT NULL AND workspace_id = '3174563515979991' AND custom_tags.dbr_tags == " " 
# MAGIC   AND usage_date >= "${abc.min_date_filter}" THEN usage_metadata.job_id END) AS manual_DLA_alert_gen_jobs,
# MAGIC
# MAGIC   COUNT(DISTINCT CASE WHEN usage_metadata.job_id IS NOT NULL AND workspace_id = '8394656167977443' AND custom_tags.dbr_tags <> " "
# MAGIC   AND usage_date >= "${abc.min_date_filter}" THEN usage_metadata.job_id END) AS total_team_alerts_jobs,
# MAGIC
# MAGIC   COUNT(DISTINCT CASE WHEN usage_metadata.job_id IS NOT NULL AND workspace_id = '8394656167977443' AND custom_tags.dbr_tags == " " 
# MAGIC   AND usage_date >= "${abc.min_date_filter}" THEN usage_metadata.job_id END) AS manual_team_alerts_jobs,
# MAGIC
# MAGIC   COUNT(DISTINCT CASE WHEN usage_metadata.job_id IS NOT NULL AND workspace_id in (
# MAGIC     '6506617936411518', -- retaillink databricks workspace
# MAGIC     '1503220589847731', -- market6 databricks workspace
# MAGIC     '8173526553309', -- bigred databricks workspace
# MAGIC     '3394176885759303', -- tescopartnertoolkit databricks workspace
# MAGIC     '5294095351256433', -- msd databricks workspace
# MAGIC     '8214564272272557', -- horizon databricks workspace
# MAGIC     '5996698886943388', -- rsi databricks workspace
# MAGIC     '4478472492095652' -- vendornet databricks workspace
# MAGIC   )
# MAGIC   AND custom_tags.dbr_tags IS NOT NULL AND custom_tags.dbr_tags NOT LIKE '%config_backup%' AND usage_date >= "${abc.min_date_filter}" THEN usage_metadata.job_id END) AS identified_data_ingestion_jobs,
# MAGIC
# MAGIC   COUNT(DISTINCT CASE WHEN usage_metadata.job_id IS NOT NULL AND workspace_id in (
# MAGIC     '6506617936411518', -- retaillink databricks workspace
# MAGIC     '1503220589847731', -- market6 databricks workspace
# MAGIC     '8173526553309', -- bigred databricks workspace
# MAGIC     '3394176885759303', -- tescopartnertoolkit databricks workspace
# MAGIC     '5294095351256433', -- msd databricks workspace
# MAGIC     '8214564272272557', -- horizon databricks workspace
# MAGIC     '5996698886943388', -- rsi databricks workspace
# MAGIC     '4478472492095652' -- vendornet databricks workspace
# MAGIC   )
# MAGIC   AND custom_tags.dbr_tags IS NOT NULL AND custom_tags.dbr_tags LIKE '%config_backup%' AND usage_date >= "${abc.min_date_filter}" THEN usage_metadata.job_id END) AS config_backup_jobs,
# MAGIC
# MAGIC   COUNT(DISTINCT CASE WHEN usage_metadata.job_id IS NOT NULL AND workspace_id in (
# MAGIC     '6506617936411518', -- retaillink databricks workspace
# MAGIC     '1503220589847731', -- market6 databricks workspace
# MAGIC     '8173526553309', -- bigred databricks workspace
# MAGIC     '3394176885759303', -- tescopartnertoolkit databricks workspace
# MAGIC     '5294095351256433', -- msd databricks workspace
# MAGIC     '8214564272272557', -- horizon databricks workspace
# MAGIC     '5996698886943388', -- rsi databricks workspace
# MAGIC     '4478472492095652' -- vendornet databricks workspace
# MAGIC   )
# MAGIC   AND custom_tags.dbr_tags IS NULL AND usage_date >= "${abc.min_date_filter}" THEN usage_metadata.job_id END) AS unidentified_data_ingestion_jobs
# MAGIC
# MAGIC FROM
# MAGIC   system.billing.usage u 
# MAGIC   INNER JOIN system.billing.list_prices lp ON u.cloud = lp.cloud AND
# MAGIC     u.sku_name = lp.sku_name AND
# MAGIC     u.usage_start_time >= lp.price_start_time AND
# MAGIC     (u.usage_end_time <= lp.price_end_time OR lp.price_end_time IS NULL);

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   u.account_id,
# MAGIC   u.workspace_id,
# MAGIC   u.sku_name,
# MAGIC   u.cloud,
# MAGIC   u.custom_tags,
# MAGIC   u.usage_start_time,
# MAGIC   u.usage_end_time,
# MAGIC   u.usage_date,
# MAGIC   date_format(u.usage_date, 'yyyy-MM') as YearMonth,
# MAGIC   u.usage_unit,
# MAGIC   u.usage_quantity,
# MAGIC   lp.pricing.default as list_price,
# MAGIC   lp.pricing.default * u.usage_quantity as list_cost,
# MAGIC   u.usage_metadata.*
# MAGIC from
# MAGIC   system.billing.usage u 
# MAGIC   inner join system.billing.list_prices lp on u.cloud = lp.cloud and
# MAGIC     u.sku_name = lp.sku_name and
# MAGIC     u.usage_start_time >= lp.price_start_time and
# MAGIC     (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)
# MAGIC
# MAGIC where
# MAGIC   usage_metadata.job_id is Null
# MAGIC and workspace_id = '6241648350210293'
# MAGIC and usage_date >= '2003-10-27'

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   u.account_id,
# MAGIC   u.workspace_id,
# MAGIC   u.sku_name,
# MAGIC   u.cloud,
# MAGIC   u.custom_tags,
# MAGIC   u.usage_start_time,
# MAGIC   u.usage_end_time,
# MAGIC   u.usage_date,
# MAGIC   date_format(u.usage_date, 'yyyy-MM') as YearMonth,
# MAGIC   u.usage_unit,
# MAGIC   u.usage_quantity,
# MAGIC   lp.pricing.default as list_price,
# MAGIC   lp.pricing.default * u.usage_quantity as list_cost,
# MAGIC   u.usage_metadata.*
# MAGIC from
# MAGIC   system.billing.usage u 
# MAGIC   inner join system.billing.list_prices lp on u.cloud = lp.cloud and
# MAGIC     u.sku_name = lp.sku_name and
# MAGIC     u.usage_start_time >= lp.price_start_time and
# MAGIC     (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)
# MAGIC
# MAGIC where
# MAGIC   usage_metadata.job_id is not Null
# MAGIC and workspace_id = '6241648350210293'
# MAGIC and usage_date >= "${abc.min_date_filter}"

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   u.account_id,
# MAGIC   u.workspace_id,
# MAGIC   u.sku_name,
# MAGIC   u.cloud,
# MAGIC   u.custom_tags,
# MAGIC   u.custom_tags.dbr_tags,
# MAGIC   u.usage_start_time,
# MAGIC   u.usage_end_time,
# MAGIC   u.usage_date,
# MAGIC   date_format(u.usage_date, 'yyyy-MM') as YearMonth,
# MAGIC   u.usage_unit,
# MAGIC   u.usage_quantity,
# MAGIC   lp.pricing.default as list_price,
# MAGIC   lp.pricing.default * u.usage_quantity as list_cost,
# MAGIC   u.usage_metadata.*
# MAGIC from
# MAGIC   system.billing.usage u 
# MAGIC   inner join system.billing.list_prices lp on u.cloud = lp.cloud and
# MAGIC     u.sku_name = lp.sku_name and
# MAGIC     u.usage_start_time >= lp.price_start_time and
# MAGIC     (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)
# MAGIC
# MAGIC where
# MAGIC   usage_metadata.job_id = '370096683446259'
# MAGIC and workspace_id = '6241648350210293'
# MAGIC and usage_date >= '2023-10-27'

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   u.account_id,
# MAGIC   u.workspace_id,
# MAGIC   u.sku_name,
# MAGIC   u.cloud,
# MAGIC   u.custom_tags,
# MAGIC   u.custom_tags.dbr_tags,
# MAGIC   u.usage_start_time,
# MAGIC   u.usage_end_time,
# MAGIC   u.usage_date,
# MAGIC   date_format(u.usage_date, 'yyyy-MM') as YearMonth,
# MAGIC   u.usage_unit,
# MAGIC   u.usage_quantity,
# MAGIC   lp.pricing.default as list_price,
# MAGIC   lp.pricing.default * u.usage_quantity as list_cost,
# MAGIC   u.usage_metadata.*
# MAGIC from
# MAGIC   system.billing.usage u 
# MAGIC   inner join system.billing.list_prices lp on u.cloud = lp.cloud and
# MAGIC     u.sku_name = lp.sku_name and
# MAGIC     u.usage_start_time >= lp.price_start_time and
# MAGIC     (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)
# MAGIC
# MAGIC where
# MAGIC   usage_metadata.job_id in ('1023227822628161', '110744752182754', '913197950211972', '736054730536366')
# MAGIC and workspace_id = '6241648350210293'
# MAGIC and usage_date >= '2023-10-27'

# COMMAND ----------

test_table_query = """

select
  u.account_id,
  u.workspace_id,
  u.sku_name,
  u.cloud,
  u.custom_tags,
  u.custom_tags.dbr_tags,
  u.usage_start_time,
  u.usage_end_time,
  u.usage_date,
  date_format(u.usage_date, 'yyyy-MM') as YearMonth,
  u.usage_unit,
  u.usage_quantity,
  lp.pricing.default as list_price,
  lp.pricing.default * u.usage_quantity as list_cost,
  u.usage_metadata.cluster_id,
  u.usage_metadata.job_id, 
  u.usage_metadata.node_type 
from
  system.billing.usage u 
  inner join system.billing.list_prices lp on u.cloud = lp.cloud and
    u.sku_name = lp.sku_name and
    u.usage_start_time >= lp.price_start_time and
    (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)

where
usage_metadata.job_id is not Null
and workspace_id = '6241648350210293'
and usage_date >= '""" + min_date_filter + """'"""

test_table = spark.sql(test_table_query)

print(test_table.count())

# COMMAND ----------

system_billing_table_query_ds = """

select
  u.account_id,
  u.workspace_id,
  u.sku_name,
  u.cloud,
  u.custom_tags.dbr_tags,
  explode(split(regexp_replace(u.custom_tags.dbr_tags, '^,|,$', ''), ',')) AS split_data,
  u.usage_start_time,
  u.usage_end_time,
  u.usage_date,
  date_format(u.usage_date, 'yyyy-MM') as YearMonth,
  u.usage_unit,
  u.usage_quantity,
  lp.pricing.default as list_price,
  lp.pricing.default * u.usage_quantity as list_cost,
  u.usage_metadata.cluster_id,
  u.usage_metadata.job_id, 
  u.usage_metadata.node_type 
from
  system.billing.usage u 
  inner join system.billing.list_prices lp on u.cloud = lp.cloud and
    u.sku_name = lp.sku_name and
    u.usage_start_time >= lp.price_start_time and
    (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)

where
usage_metadata.job_id is not Null
and workspace_id = '6241648350210293' -- PROD DATA SCIENCE DATABRICKS WORKSPACE
and usage_date >= '""" + min_date_filter + """'"""

df_system_billing_table_ds = spark.sql(system_billing_table_query_ds)

print(df_system_billing_table_ds.count())

# COMMAND ----------

display(df_system_billing_table_ds)

# COMMAND ----------

group_cols = [
  "account_id",
  "workspace_id",
  "sku_name",
  "cloud",
  "dbr_tags",
  "usage_start_time",
  "usage_end_time",
  "usage_date",
  "YearMonth",
  "usage_unit",
  "usage_quantity",
  "list_price",
  "list_cost",
  "cluster_id",
  "job_id", 
  "node_type" 
]

# COMMAND ----------

df_system_billing_table_Forecast_DRFE = df_system_billing_table_ds.where(pyf.col("dbr_tags").contains("Forecast DRFE"))

df_system_billing_table_value_measurement = df_system_billing_table_ds.where(pyf.col("dbr_tags").contains("value measurement"))

print(df_system_billing_table_Forecast_DRFE.count())
print(df_system_billing_table_value_measurement.count())

# COMMAND ----------

# Split the 'split_data' column into two columns
split_data_df_Forecast_DRFE = df_system_billing_table_Forecast_DRFE.withColumn("key", pyf.split(df_system_billing_table_Forecast_DRFE["split_data"], ":").getItem(0))
split_data_df_Forecast_DRFE = split_data_df_Forecast_DRFE.withColumn("value", pyf.split(df_system_billing_table_Forecast_DRFE["split_data"], ":").getItem(1))

# Pivot the DataFrame
pivoted_df_Forecast_DRFE = split_data_df_Forecast_DRFE.groupBy(group_cols).pivot("key").agg({"value": "first"})

# Show the result
display(pivoted_df_Forecast_DRFE)

# COMMAND ----------

# Split the 'split_data' column into two columns
split_data_df_value_measurement = df_system_billing_table_value_measurement.withColumn("key", pyf.split(df_system_billing_table_value_measurement["split_data"], ":").getItem(0))
split_data_df_value_measurement = split_data_df_value_measurement.withColumn("value", pyf.split(df_system_billing_table_value_measurement["split_data"], ":").getItem(1))

# Pivot the DataFrame
pivoted_df_value_measurement = split_data_df_value_measurement.groupBy(group_cols).pivot("key").agg({"value": "first"})

# Show the result
display(pivoted_df_value_measurement)

# COMMAND ----------

selected_cols = [
  "sku_name",
  "data_label",
  "job_id",
  # "cluster_id",
  "job_cluster_country_cd",
  "job_cluster_acosta_customer",
  "node_type",
  "usage_date",
  "usage_unit",
  "usage_quantity",
  "list_price",
  "list_cost"
]

selected_cols_plus_workspace_id = [
  "sku_name",
  "data_label",
  "job_id",
  "workspace_id",
  "job_cluster_country_cd",
  "job_cluster_acosta_customer",
  "node_type",
  "usage_date",
  "usage_unit",
  "usage_quantity",
  "list_price",
  "list_cost"
]

final_selected_cols = [
  "sku_name",
  "job_cluster_country_cd",
  "job_cluster_acosta_customer",
  "num_distinct_jobs",
  "total_usage_quantity",
  "total_cost"
]

# COMMAND ----------

pivoted_df_Forecast_DRFE_agg = pivoted_df_Forecast_DRFE.select(selected_cols).groupBy(
  'sku_name', 'data_label', 'job_cluster_acosta_customer', 'job_cluster_country_cd', 'usage_unit').agg(
    pyf.countDistinct("usage_date").alias('num_forecast_days'),   
    pyf.countDistinct("job_id").alias('num_distinct_jobs'),
    pyf.sum("usage_quantity").alias('total_usage_quantity'), 
    pyf.avg("list_price").alias('average_list_price'), 
    pyf.sum("list_cost").alias('total_cost')
    ).orderBy('sku_name', 'data_label', 'job_cluster_country_cd', 'job_cluster_acosta_customer', 'usage_unit')
  
display(pivoted_df_Forecast_DRFE_agg)

# COMMAND ----------

# Create a new column 'system' using a case expression
expr_str0 = "CASE " + " ".join([f"WHEN job_cluster_acosta_customer LIKE '%{team}%' THEN '{team}' " for team in teams]) + "ELSE NULL END AS team"

# Create a new column 'retailer' using a case expression
expr_str1 = "CASE " + " ".join([f"WHEN job_cluster_acosta_customer LIKE '%{retailer}%' THEN '{retailer}' " for retailer in retailers]) + "ELSE NULL END AS retailer"

# Create a new column 'client' using a case expression
expr_str2 = "CASE " + " ".join([f"WHEN job_cluster_acosta_customer LIKE '%{client}%' THEN '{client}' " for client in clients]) + "ELSE NULL END AS client"

# Create a new column 'system' using a case expression
expr_str3 = "CASE " + " ".join([f"WHEN job_cluster_acosta_customer LIKE '%{system}%' THEN '{system}' " for system in systems]) + "ELSE NULL END AS system"

# Assuming you have a DataFrame called df with the specified columns
columns_to_sum4 = ['DRFE_forecast_jobs', 'value_measurement_jobs', 'DLA_alert_gen_jobs',
                  'team_alerts_jobs', 'data_ingestion_jobs']

# Assuming you have a DataFrame called df with the specified columns
columns_to_sum5 = ['DRFE_forecast_DBUs', 'value_measurement_DBUs', 'DLA_alert_gen_DBUs',
                  'team_alerts_DBUs', 'data_ingestion_DBUs']

# Assuming you have a DataFrame called df with the specified columns
columns_to_sum6 = ['DRFE_forecast_cost', 'value_measurement_cost', 'DLA_alert_gen_cost',
                  'team_alerts_cost', 'data_ingestion_cost']

# Create the PySpark expression
expr_str4 = "+".join([f"coalesce({col}, 0)" for col in columns_to_sum4])
expr_str5 = "+".join([f"coalesce({col}, 0)" for col in columns_to_sum5])
expr_str6 = "+".join([f"coalesce({col}, 0)" for col in columns_to_sum6])

# COMMAND ----------

df_Forecast_DRFE_by_customer = pivoted_df_Forecast_DRFE_agg.select(final_selected_cols).withColumnRenamed('job_cluster_country_cd', 'country')

df_Forecast_DRFE_by_customer = df_Forecast_DRFE_by_customer.withColumn("retailer", pyf.expr(expr_str1))
df_Forecast_DRFE_by_customer = df_Forecast_DRFE_by_customer.withColumn("client", pyf.expr(expr_str2))
df_Forecast_DRFE_by_customer = df_Forecast_DRFE_by_customer.withColumn("system", pyf.expr(expr_str3))

df_Forecast_DRFE_by_customer = df_Forecast_DRFE_by_customer.withColumnRenamed('num_distinct_jobs', 'DRFE_forecast_jobs')
df_Forecast_DRFE_by_customer = df_Forecast_DRFE_by_customer.withColumnRenamed('total_usage_quantity', 'DRFE_forecast_DBUs')
df_Forecast_DRFE_by_customer = df_Forecast_DRFE_by_customer.withColumnRenamed('total_cost', 'DRFE_forecast_cost')

df_Forecast_DRFE_by_customer = df_Forecast_DRFE_by_customer.select("sku_name", "system", "retailer", "client", "country", "DRFE_forecast_jobs", "DRFE_forecast_DBUs", "DRFE_forecast_cost")

display(df_Forecast_DRFE_by_customer)

# COMMAND ----------

pivoted_df_Forecast_DRFE_agg_all_customers = pivoted_df_Forecast_DRFE.select(selected_cols).groupBy(
  'data_label').agg(
    pyf.countDistinct("job_id").alias('num_distinct_jobs'),
    pyf.sum("usage_quantity").alias('total_usage_quantity'), 
    pyf.avg("list_price").alias('average_list_price'), 
    pyf.sum("list_cost").alias('total_cost')
    )
  
display(pivoted_df_Forecast_DRFE_agg_all_customers)

# COMMAND ----------

pivoted_df_value_measurement_agg = pivoted_df_value_measurement.select(selected_cols).groupBy(
  'sku_name', 'data_label', 'job_cluster_acosta_customer', 'job_cluster_country_cd', 'usage_unit').agg(
    pyf.countDistinct("job_id").alias('num_distinct_jobs'),
    pyf.sum("usage_quantity").alias('total_usage_quantity'), 
    pyf.avg("list_price").alias('average_list_price'), 
    pyf.sum("list_cost").alias('total_cost')
    ).orderBy('sku_name', 'data_label', 'job_cluster_country_cd', 'job_cluster_acosta_customer', 'usage_unit')
  
display(pivoted_df_value_measurement_agg)

# COMMAND ----------

df_value_measurement_by_customer = pivoted_df_value_measurement_agg.select(final_selected_cols).withColumnRenamed('job_cluster_country_cd', 'country')

df_value_measurement_by_customer = df_value_measurement_by_customer.withColumn("retailer", pyf.expr(expr_str1))
df_value_measurement_by_customer = df_value_measurement_by_customer.withColumn("client", pyf.expr(expr_str2))
df_value_measurement_by_customer = df_value_measurement_by_customer.withColumn("system", pyf.expr(expr_str3))

# Extra step in team alert to combine rows, since Nestle UK has both Gen1 ADLS IVM runs and Gen2 ADLS IVM runs

df_value_measurement_by_customer = df_value_measurement_by_customer.groupBy(
  'sku_name', 'system', 'retailer', 'client', 'country').agg(
    pyf.sum("num_distinct_jobs").alias('value_measurement_jobs'), 
    pyf.sum("total_usage_quantity").alias('value_measurement_DBUs'), 
    pyf.sum("total_cost").alias('value_measurement_cost')
    ).orderBy('sku_name', 'system', 'retailer', 'client', 'country')
  
# df_value_measurement_by_customer = df_value_measurement_by_customer.withColumnRenamed('num_distinct_jobs', 'value_measurement_jobs')
# df_value_measurement_by_customer = df_value_measurement_by_customer.withColumnRenamed('total_usage_quantity', 'value_measurement_DBUs')
# df_value_measurement_by_customer = df_value_measurement_by_customer.withColumnRenamed('total_cost', 'value_measurement_cost')

# df_value_measurement_by_customer = df_value_measurement_by_customer.select("sku_name", "system", "retailer", "client", "country", "value_measurement_jobs", "value_measurement_DBUs", "value_measurement_cost")

display(df_value_measurement_by_customer)

# COMMAND ----------

# Note: in the eus2p-df-dp-im-retailreport ADF there are 440 pipeline runs (both successful and failed runs) over the same period. The discrepancy is because each failed pipeline results in up to 4 jobs (due to the config settings which will retrigger a failed job up to 3 additional times)

pivoted_df_value_measurement_agg_all_customers = pivoted_df_value_measurement.select(selected_cols).groupBy(
  'data_label').agg(
    pyf.countDistinct("job_id").alias('num_distinct_jobs'),
    pyf.sum("usage_quantity").alias('total_usage_quantity'), 
    pyf.avg("list_price").alias('average_list_price'), 
    pyf.sum("list_cost").alias('total_cost')
    )
  
display(pivoted_df_value_measurement_agg_all_customers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Section 2 - DLA Individual Alert-Gen Processes

# COMMAND ----------

# MAGIC %sql
# MAGIC select usage_date, count(distinct usage_metadata.job_id)
# MAGIC from
# MAGIC   system.billing.usage u 
# MAGIC where
# MAGIC   usage_metadata.job_id is not Null
# MAGIC and workspace_id = '3174563515979991' -- PROD INDIVIDUAL ALERT-GEN DATABRICKS WORKSPACE
# MAGIC group by usage_date
# MAGIC order by usage_date

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from
# MAGIC   system.billing.usage u 
# MAGIC   inner join system.billing.list_prices lp on u.cloud = lp.cloud and
# MAGIC     u.sku_name = lp.sku_name and
# MAGIC     u.usage_start_time >= lp.price_start_time and
# MAGIC     (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)
# MAGIC
# MAGIC where
# MAGIC   usage_metadata.job_id is not Null
# MAGIC and workspace_id = '3174563515979991'
# MAGIC and usage_date >= '2023-10-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   u.account_id,
# MAGIC   u.workspace_id,
# MAGIC   u.sku_name,
# MAGIC   u.cloud,
# MAGIC   u.custom_tags,
# MAGIC   u.usage_start_time,
# MAGIC   u.usage_end_time,
# MAGIC   u.usage_date,
# MAGIC   date_format(u.usage_date, 'yyyy-MM') as YearMonth,
# MAGIC   u.usage_unit,
# MAGIC   u.usage_quantity,
# MAGIC   lp.pricing.default as list_price,
# MAGIC   lp.pricing.default * u.usage_quantity as list_cost,
# MAGIC   u.usage_metadata.*
# MAGIC from
# MAGIC   system.billing.usage u 
# MAGIC   inner join system.billing.list_prices lp on u.cloud = lp.cloud and
# MAGIC     u.sku_name = lp.sku_name and
# MAGIC     u.usage_start_time >= lp.price_start_time and
# MAGIC     (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)
# MAGIC
# MAGIC where
# MAGIC   usage_metadata.job_id is Null
# MAGIC and workspace_id = '3174563515979991'
# MAGIC and usage_date >= '2023-10-01'

# COMMAND ----------

system_billing_table_query_DLA_alert_gen = """

select
  u.account_id,
  u.workspace_id,
  u.sku_name,
  u.cloud,
  u.custom_tags.dbr_tags,
  explode(split(regexp_replace(u.custom_tags.dbr_tags, '^,|,$', ''), ',')) AS split_data,
  u.usage_start_time,
  u.usage_end_time,
  u.usage_date,
  date_format(u.usage_date, 'yyyy-MM') as YearMonth,
  u.usage_unit,
  u.usage_quantity,
  lp.pricing.default as list_price,
  lp.pricing.default * u.usage_quantity as list_cost,
  u.usage_metadata.cluster_id,
  u.usage_metadata.job_id, 
  u.usage_metadata.node_type 
from
  system.billing.usage u 
  inner join system.billing.list_prices lp on u.cloud = lp.cloud and
    u.sku_name = lp.sku_name and
    u.usage_start_time >= lp.price_start_time and
    (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)

where
usage_metadata.job_id is not Null
and workspace_id = '3174563515979991' -- PROD ALERT GEN DATABRICKS WORKSPACE
and usage_date >= '""" + min_date_filter + """'"""

df_system_billing_table_DLA_alert_gen = spark.sql(system_billing_table_query_DLA_alert_gen)

print(df_system_billing_table_DLA_alert_gen.count())

# COMMAND ----------

# Split the 'split_data' column into two columns
split_data_df_DLA_alert_gen = df_system_billing_table_DLA_alert_gen.withColumn("key", pyf.split(df_system_billing_table_DLA_alert_gen["split_data"], ":").getItem(0))
split_data_df_DLA_alert_gen = split_data_df_DLA_alert_gen.withColumn("value", pyf.split(df_system_billing_table_DLA_alert_gen["split_data"], ":").getItem(1))

# Pivot the DataFrame
pivoted_df_DLA_alert_gen = split_data_df_DLA_alert_gen.groupBy(group_cols).pivot("key").agg({"value": "first"})

# Show the result
display(pivoted_df_DLA_alert_gen)

# COMMAND ----------

pivoted_df_DLA_alert_gen_OSA_grouping = pivoted_df_DLA_alert_gen.where(pyf.col("data_label").contains("DRFE-OSA Grouping"))

display(pivoted_df_DLA_alert_gen_OSA_grouping)

# COMMAND ----------

pivoted_df_DLA_alert_gen_agg = pivoted_df_DLA_alert_gen.select(selected_cols).groupBy(
  'sku_name', 'data_label', 'job_cluster_acosta_customer', 'job_cluster_country_cd', 'usage_unit').agg(
    pyf.countDistinct("job_id").alias('num_distinct_jobs'),
    pyf.sum("usage_quantity").alias('total_usage_quantity'), 
    pyf.avg("list_price").alias('average_list_price'), 
    pyf.sum("list_cost").alias('total_cost')
    ).orderBy('sku_name', 'data_label', 'job_cluster_country_cd', 'job_cluster_acosta_customer', 'usage_unit')
  
display(pivoted_df_DLA_alert_gen_agg)

# COMMAND ----------

pivoted_df_DLA_alert_gen_agg_by_job_cluster_acosta_customer = pivoted_df_DLA_alert_gen.select(selected_cols).groupBy(
  'sku_name', 'job_cluster_acosta_customer', 'job_cluster_country_cd', 'usage_unit').agg(
    pyf.countDistinct("job_id").alias('num_distinct_jobs'),
    pyf.sum("usage_quantity").alias('total_usage_quantity'), 
    pyf.avg("list_price").alias('average_list_price'), 
    pyf.sum("list_cost").alias('total_cost')
    ).orderBy('sku_name', 'job_cluster_country_cd', 'job_cluster_acosta_customer', 'usage_unit')
  
display(pivoted_df_DLA_alert_gen_agg_by_job_cluster_acosta_customer)

# COMMAND ----------

df_DLA_alert_gen_by_customer = pivoted_df_DLA_alert_gen_agg_by_job_cluster_acosta_customer.select(final_selected_cols).withColumnRenamed('job_cluster_country_cd', 'country')

df_DLA_alert_gen_by_customer = df_DLA_alert_gen_by_customer.withColumn("retailer", pyf.expr(expr_str1))
df_DLA_alert_gen_by_customer = df_DLA_alert_gen_by_customer.withColumn("client", pyf.expr(expr_str2))
df_DLA_alert_gen_by_customer = df_DLA_alert_gen_by_customer.withColumn("system", pyf.expr(expr_str3))

df_DLA_alert_gen_by_customer = df_DLA_alert_gen_by_customer.withColumnRenamed('num_distinct_jobs', 'DLA_alert_gen_jobs')
df_DLA_alert_gen_by_customer = df_DLA_alert_gen_by_customer.withColumnRenamed('total_usage_quantity', 'DLA_alert_gen_DBUs')
df_DLA_alert_gen_by_customer = df_DLA_alert_gen_by_customer.withColumnRenamed('total_cost', 'DLA_alert_gen_cost')

df_DLA_alert_gen_by_customer = df_DLA_alert_gen_by_customer.select("sku_name", "system", "retailer", "client", "country", "DLA_alert_gen_jobs", "DLA_alert_gen_DBUs", "DLA_alert_gen_cost")

display(df_DLA_alert_gen_by_customer)

# COMMAND ----------

pivoted_df_DLA_alert_gen_agg_all_customers = pivoted_df_DLA_alert_gen.select(selected_cols).groupBy(
  'data_label').agg(
    pyf.countDistinct("job_id").alias('num_distinct_jobs'),
    pyf.sum("usage_quantity").alias('total_usage_quantity'), 
    pyf.avg("list_price").alias('average_list_price'), 
    pyf.sum("list_cost").alias('total_cost')
    ).orderBy('data_label')
  
display(pivoted_df_DLA_alert_gen_agg_all_customers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Section 3 - DLA Team Alert Processes

# COMMAND ----------

# MAGIC %sql
# MAGIC select usage_date, count(*)
# MAGIC from
# MAGIC   system.billing.usage u 
# MAGIC where
# MAGIC   usage_metadata.job_id is not Null
# MAGIC and workspace_id = '8394656167977443' -- PROD TEAM ALERTS DATABRICKS WORKSPACE
# MAGIC group by usage_date
# MAGIC order by usage_date

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from
# MAGIC   system.billing.usage u 
# MAGIC   inner join system.billing.list_prices lp on u.cloud = lp.cloud and
# MAGIC     u.sku_name = lp.sku_name and
# MAGIC     u.usage_start_time >= lp.price_start_time and
# MAGIC     (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)
# MAGIC
# MAGIC where
# MAGIC   usage_metadata.job_id is not Null
# MAGIC and workspace_id = '8394656167977443' -- PROD TEAM ALERTS DATABRICKS WORKSPACE
# MAGIC and usage_date >= '2023-10-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   u.account_id,
# MAGIC   u.workspace_id,
# MAGIC   u.sku_name,
# MAGIC   u.cloud,
# MAGIC   u.custom_tags,
# MAGIC   u.usage_start_time,
# MAGIC   u.usage_end_time,
# MAGIC   u.usage_date,
# MAGIC   date_format(u.usage_date, 'yyyy-MM') as YearMonth,
# MAGIC   u.usage_unit,
# MAGIC   u.usage_quantity,
# MAGIC   lp.pricing.default as list_price,
# MAGIC   lp.pricing.default * u.usage_quantity as list_cost,
# MAGIC   u.usage_metadata.*
# MAGIC from
# MAGIC   system.billing.usage u 
# MAGIC   inner join system.billing.list_prices lp on u.cloud = lp.cloud and
# MAGIC     u.sku_name = lp.sku_name and
# MAGIC     u.usage_start_time >= lp.price_start_time and
# MAGIC     (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)
# MAGIC
# MAGIC where
# MAGIC   usage_metadata.job_id is Null
# MAGIC and workspace_id = '8394656167977443' -- PROD TEAM ALERTS DATABRICKS WORKSPACE
# MAGIC and usage_date >= '2023-10-01'

# COMMAND ----------

system_billing_table_query_team_alerts = """

select
  u.account_id,
  u.workspace_id,
  u.sku_name,
  u.cloud,
  u.custom_tags.dbr_tags,
  explode(split(regexp_replace(u.custom_tags.dbr_tags, '^,|,$', ''), ',')) AS split_data,
  u.usage_start_time,
  u.usage_end_time,
  u.usage_date,
  date_format(u.usage_date, 'yyyy-MM') as YearMonth,
  u.usage_unit,
  u.usage_quantity,
  lp.pricing.default as list_price,
  lp.pricing.default * u.usage_quantity as list_cost,
  u.usage_metadata.cluster_id,
  u.usage_metadata.job_id, 
  u.usage_metadata.node_type 
from
  system.billing.usage u 
  inner join system.billing.list_prices lp on u.cloud = lp.cloud and
    u.sku_name = lp.sku_name and
    u.usage_start_time >= lp.price_start_time and
    (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)

where
usage_metadata.job_id is not Null
and workspace_id = '8394656167977443' -- PROD TEAM ALERTS DATABRICKS WORKSPACE
and usage_date >= '""" + min_date_filter + """'"""

df_system_billing_table_team_alerts = spark.sql(system_billing_table_query_team_alerts)

print(df_system_billing_table_team_alerts.count())

# COMMAND ----------

# Split the 'split_data' column into two columns
split_data_df_team_alerts = df_system_billing_table_team_alerts.withColumn("key", pyf.split(df_system_billing_table_team_alerts["split_data"], ":").getItem(0))
split_data_df_team_alerts = split_data_df_team_alerts.withColumn("value", pyf.split(df_system_billing_table_team_alerts["split_data"], ":").getItem(1))

# Pivot the DataFrame
pivoted_df_team_alerts = split_data_df_team_alerts.groupBy(group_cols).pivot("key").agg({"value": "first"})

# Show the result
display(pivoted_df_team_alerts)

# COMMAND ----------

pivoted_df_team_alerts_agg = pivoted_df_team_alerts.select(selected_cols).groupBy(
  'sku_name', 'data_label', 'job_cluster_acosta_customer', 'job_cluster_country_cd', 'usage_unit').agg(
    pyf.countDistinct("job_id").alias('num_distinct_jobs'),
    pyf.sum("usage_quantity").alias('total_usage_quantity'), 
    pyf.avg("list_price").alias('average_list_price'), 
    pyf.sum("list_cost").alias('total_cost')
    ).orderBy('sku_name', 'data_label', 'job_cluster_country_cd', 'job_cluster_acosta_customer', 'usage_unit')
  
display(pivoted_df_team_alerts_agg)

# COMMAND ----------

df_team_alerts_by_customer = pivoted_df_team_alerts_agg.select(final_selected_cols).withColumnRenamed('job_cluster_country_cd', 'country')

df_team_alerts_by_customer = df_team_alerts_by_customer.withColumn("team", pyf.expr(expr_str0))
df_team_alerts_by_customer = df_team_alerts_by_customer.withColumn("retailer", pyf.expr(expr_str1))
df_team_alerts_by_customer = df_team_alerts_by_customer.withColumn("client", pyf.expr(expr_str2))
df_team_alerts_by_customer = df_team_alerts_by_customer.withColumn("system", pyf.expr(expr_str3))

# Extra step in team alert to combine rows, since some retailer client combinations have both a syndicated team and a drt team

# df_team_alerts_by_customer = df_team_alerts_by_customer.groupBy(
#   'sku_name', 'system', 'retailer', 'client', 'country').agg(
#     pyf.sum("num_distinct_jobs").alias('team_alerts_jobs'), 
#     pyf.sum("total_usage_quantity").alias('team_alerts_DBUs'), 
#     pyf.sum("total_cost").alias('team_alerts_cost')
#     ).orderBy('sku_name', 'system', 'retailer', 'client', 'country')

df_team_alerts_by_customer = df_team_alerts_by_customer.withColumnRenamed('num_distinct_jobs', 'team_alerts_jobs')
df_team_alerts_by_customer = df_team_alerts_by_customer.withColumnRenamed('total_usage_quantity', 'team_alerts_DBUs')
df_team_alerts_by_customer = df_team_alerts_by_customer.withColumnRenamed('total_cost', 'team_alerts_cost')

df_team_alerts_by_customer = df_team_alerts_by_customer.select("sku_name", "system", "retailer", "client", "country", "team", "team_alerts_jobs", "team_alerts_DBUs", "team_alerts_cost")

display(df_team_alerts_by_customer)

# COMMAND ----------

pivoted_df_team_alerts_agg_all_customers = pivoted_df_team_alerts.select(selected_cols).groupBy(
  'data_label').agg(
    pyf.countDistinct("job_id").alias('num_distinct_jobs'),
    pyf.sum("usage_quantity").alias('total_usage_quantity'), 
    pyf.avg("list_price").alias('average_list_price'), 
    pyf.sum("list_cost").alias('total_cost')
    ).orderBy('data_label')
  
display(pivoted_df_team_alerts_agg_all_customers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Section 4 - Retail Reporting Processes

# COMMAND ----------

# MAGIC %sql
# MAGIC select usage_date, count(*)
# MAGIC from
# MAGIC   system.billing.usage u 
# MAGIC where
# MAGIC   usage_metadata.job_id is not Null
# MAGIC and workspace_id = '8540025296902421' -- PROD RETAIL REPORT DATABRICKS WORKSPACE
# MAGIC group by usage_date
# MAGIC order by usage_date

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from
# MAGIC   system.billing.usage u 
# MAGIC   inner join system.billing.list_prices lp on u.cloud = lp.cloud and
# MAGIC     u.sku_name = lp.sku_name and
# MAGIC     u.usage_start_time >= lp.price_start_time and
# MAGIC     (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)
# MAGIC
# MAGIC where
# MAGIC   usage_metadata.job_id is not Null
# MAGIC and workspace_id = '8540025296902421' -- PROD RETAIL REPORT DATABRICKS WORKSPACE
# MAGIC and usage_date >= '2023-10-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   u.account_id,
# MAGIC   u.workspace_id,
# MAGIC   u.sku_name,
# MAGIC   u.cloud,
# MAGIC   u.custom_tags,
# MAGIC   u.usage_start_time,
# MAGIC   u.usage_end_time,
# MAGIC   u.usage_date,
# MAGIC   date_format(u.usage_date, 'yyyy-MM') as YearMonth,
# MAGIC   u.usage_unit,
# MAGIC   u.usage_quantity,
# MAGIC   lp.pricing.default as list_price,
# MAGIC   lp.pricing.default * u.usage_quantity as list_cost,
# MAGIC   u.usage_metadata.*
# MAGIC from
# MAGIC   system.billing.usage u 
# MAGIC   inner join system.billing.list_prices lp on u.cloud = lp.cloud and
# MAGIC     u.sku_name = lp.sku_name and
# MAGIC     u.usage_start_time >= lp.price_start_time and
# MAGIC     (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)
# MAGIC
# MAGIC where
# MAGIC   usage_metadata.job_id is Null
# MAGIC and workspace_id = '8540025296902421' -- PROD RETAIL REPORT DATABRICKS WORKSPACE
# MAGIC and usage_date >= '2023-10-01'

# COMMAND ----------

system_billing_table_query_retail_report = """

select
  u.account_id,
  u.workspace_id,
  u.sku_name,
  u.cloud,
  u.custom_tags.dbr_tags,
  explode(split(regexp_replace(u.custom_tags.dbr_tags, '^,|,$', ''), ',')) AS split_data,
  u.usage_start_time,
  u.usage_end_time,
  u.usage_date,
  date_format(u.usage_date, 'yyyy-MM') as YearMonth,
  u.usage_unit,
  u.usage_quantity,
  lp.pricing.default as list_price,
  lp.pricing.default * u.usage_quantity as list_cost,
  u.usage_metadata.cluster_id,
  u.usage_metadata.job_id, 
  u.usage_metadata.node_type 
from
  system.billing.usage u 
  inner join system.billing.list_prices lp on u.cloud = lp.cloud and
    u.sku_name = lp.sku_name and
    u.usage_start_time >= lp.price_start_time and
    (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)

where
usage_metadata.job_id is not Null
and workspace_id = '8540025296902421' -- PROD RETAIL REPORT DATABRICKS WORKSPACE
and usage_date >= '""" + min_date_filter + """'"""

df_system_billing_table_retail_report = spark.sql(system_billing_table_query_retail_report)

print(df_system_billing_table_retail_report.count())

# COMMAND ----------

# Split the 'split_data' column into two columns
split_data_df_retail_report = df_system_billing_table_retail_report.withColumn("key", pyf.split(df_system_billing_table_retail_report["split_data"], ":").getItem(0))
split_data_df_retail_report = split_data_df_retail_report.withColumn("value", pyf.split(df_system_billing_table_retail_report["split_data"], ":").getItem(1))

# Pivot the DataFrame
pivoted_df_retail_report = split_data_df_retail_report.groupBy(group_cols).pivot("key").agg({"value": "first"})

# Show the result
display(pivoted_df_retail_report)

# COMMAND ----------

# This is the 'rsv_nars_load_interventions_config_mapping_param_pl' ADF pipeline in Retail Report, which runs once a day

pivoted_df_retail_report_value_measurement = pivoted_df_retail_report.where(pyf.col("data_label").contains("value measurement"))

display(pivoted_df_retail_report_value_measurement)

# COMMAND ----------

pivoted_df_retail_report_agg_by_job_cluster_acosta_customer = pivoted_df_retail_report.select(selected_cols).groupBy(
  'sku_name', 'data_label', 'job_cluster_acosta_customer', 'job_cluster_country_cd', 'usage_unit').agg(
    pyf.countDistinct("job_id").alias('num_distinct_jobs'),
    pyf.sum("usage_quantity").alias('total_usage_quantity'), 
    pyf.avg("list_price").alias('average_list_price'), 
    pyf.sum("list_cost").alias('total_cost')
    ).orderBy('sku_name', 'data_label', 'job_cluster_country_cd', 'job_cluster_acosta_customer', 'usage_unit')
  
display(pivoted_df_retail_report_agg_by_job_cluster_acosta_customer)

# COMMAND ----------

df_retail_report_by_customer = pivoted_df_retail_report_agg_by_job_cluster_acosta_customer.select(final_selected_cols).withColumnRenamed('job_cluster_country_cd', 'country')

df_retail_report_by_customer = df_retail_report_by_customer.withColumn("retailer", pyf.expr(expr_str1))
df_retail_report_by_customer = df_retail_report_by_customer.withColumn("client", pyf.expr(expr_str2))
df_retail_report_by_customer = df_retail_report_by_customer.withColumn("system", pyf.expr(expr_str3))

df_retail_report_by_customer = df_retail_report_by_customer.withColumnRenamed('num_distinct_jobs', 'retail_report_jobs')
df_retail_report_by_customer = df_retail_report_by_customer.withColumnRenamed('total_usage_quantity', 'retail_report_DBUs')
df_retail_report_by_customer = df_retail_report_by_customer.withColumnRenamed('total_cost', 'retail_report_cost')

df_retail_report_by_customer = df_retail_report_by_customer.select("sku_name", "system", "retailer", "client", "country", "retail_report_jobs", "retail_report_DBUs", "retail_report_cost")

display(df_retail_report_by_customer)

# COMMAND ----------

pivoted_df_retail_report_agg_by_data_label = pivoted_df_retail_report.select(selected_cols).groupBy(
  'data_label').agg(
    pyf.countDistinct("job_id").alias('num_distinct_jobs'),
    pyf.sum("usage_quantity").alias('total_usage_quantity'), 
    pyf.avg("list_price").alias('average_list_price'), 
    pyf.sum("list_cost").alias('total_cost')
    ).orderBy('data_label')
  
display(pivoted_df_retail_report_agg_by_data_label)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Section 5 - Data Ingestion Processes

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from
# MAGIC   system.billing.usage u 
# MAGIC where workspace_id in (
# MAGIC   '8173526553309', -- bigred databricks workspace
# MAGIC   '4478472492095652' -- vendornet databricks workspace
# MAGIC )
# MAGIC and sku_name = 'PREMIUM_JOBS_COMPUTE'
# MAGIC and usage_date >= "${abc.min_date_filter}"

# COMMAND ----------

# MAGIC %sql
# MAGIC select usage_date, count(*)
# MAGIC from
# MAGIC   system.billing.usage u 
# MAGIC where
# MAGIC   usage_metadata.job_id is not Null
# MAGIC and workspace_id in (
# MAGIC   '6506617936411518', -- retaillink databricks workspace
# MAGIC   '7173165358547255', -- premium360/rex databricks workspace
# MAGIC   '1503220589847731', -- market6 databricks workspace
# MAGIC   '8173526553309', -- bigred databricks workspace
# MAGIC   '3394176885759303', -- tescopartnertoolkit databricks workspace
# MAGIC   '5294095351256433', -- msd databricks workspace
# MAGIC   '8214564272272557', -- horizon databricks workspace
# MAGIC   '5996698886943388', -- rsi databricks workspace
# MAGIC   '4478472492095652' -- vendornet databricks workspace
# MAGIC )
# MAGIC group by usage_date
# MAGIC order by usage_date

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from
# MAGIC   system.billing.usage u 
# MAGIC   inner join system.billing.list_prices lp on u.cloud = lp.cloud and
# MAGIC     u.sku_name = lp.sku_name and
# MAGIC     u.usage_start_time >= lp.price_start_time and
# MAGIC     (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)
# MAGIC
# MAGIC where
# MAGIC   usage_metadata.job_id is not Null
# MAGIC and workspace_id in (
# MAGIC   '6506617936411518', -- retaillink databricks workspace
# MAGIC   '7173165358547255', -- premium360/rex databricks workspace
# MAGIC   '1503220589847731', -- market6 databricks workspace
# MAGIC   '8173526553309', -- bigred databricks workspace
# MAGIC   '3394176885759303', -- tescopartnertoolkit databricks workspace
# MAGIC   '5294095351256433', -- msd databricks workspace
# MAGIC   '8214564272272557', -- horizon databricks workspace
# MAGIC   '5996698886943388', -- rsi databricks workspace
# MAGIC   '4478472492095652' -- vendornet databricks workspace
# MAGIC )
# MAGIC and usage_date >= '2023-10-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   u.account_id,
# MAGIC   u.workspace_id,
# MAGIC   u.sku_name,
# MAGIC   u.cloud,
# MAGIC   u.custom_tags,
# MAGIC   u.usage_start_time,
# MAGIC   u.usage_end_time,
# MAGIC   u.usage_date,
# MAGIC   date_format(u.usage_date, 'yyyy-MM') as YearMonth,
# MAGIC   u.usage_unit,
# MAGIC   u.usage_quantity,
# MAGIC   lp.pricing.default as list_price,
# MAGIC   lp.pricing.default * u.usage_quantity as list_cost,
# MAGIC   u.usage_metadata.*
# MAGIC from
# MAGIC   system.billing.usage u 
# MAGIC   inner join system.billing.list_prices lp on u.cloud = lp.cloud and
# MAGIC     u.sku_name = lp.sku_name and
# MAGIC     u.usage_start_time >= lp.price_start_time and
# MAGIC     (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)
# MAGIC
# MAGIC where
# MAGIC   usage_metadata.job_id is Null
# MAGIC and workspace_id in (
# MAGIC   '6506617936411518', -- retaillink databricks workspace
# MAGIC   '7173165358547255', -- premium360/rex databricks workspace
# MAGIC   '1503220589847731', -- market6 databricks workspace
# MAGIC   '8173526553309', -- bigred databricks workspace
# MAGIC   '3394176885759303', -- tescopartnertoolkit databricks workspace
# MAGIC   '5294095351256433', -- msd databricks workspace
# MAGIC   '8214564272272557', -- horizon databricks workspace
# MAGIC   '5996698886943388', -- rsi databricks workspace
# MAGIC   '4478472492095652' -- vendornet databricks workspace
# MAGIC )
# MAGIC and usage_date >= '2023-10-01'

# COMMAND ----------

system_billing_table_query_data_ingestion = """

select
  u.account_id,
  u.workspace_id,
  u.sku_name,
  u.cloud,
  u.custom_tags.dbr_tags,
  explode(split(regexp_replace(u.custom_tags.dbr_tags, '^,|,$', ''), ',')) AS split_data,
  u.usage_start_time,
  u.usage_end_time,
  u.usage_date,
  date_format(u.usage_date, 'yyyy-MM') as YearMonth,
  u.usage_unit,
  u.usage_quantity,
  lp.pricing.default as list_price,
  lp.pricing.default * u.usage_quantity as list_cost,
  u.usage_metadata.cluster_id,
  u.usage_metadata.job_id, 
  u.usage_metadata.node_type 
from
  system.billing.usage u 
  inner join system.billing.list_prices lp on u.cloud = lp.cloud and
    u.sku_name = lp.sku_name and
    u.usage_start_time >= lp.price_start_time and
    (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)

where
usage_metadata.job_id is not Null
and workspace_id in (
  '6506617936411518', -- retaillink databricks workspace
  '7173165358547255', -- premium360/rex databricks workspace
  '1503220589847731', -- market6 databricks workspace
  '8173526553309', -- bigred databricks workspace
  '3394176885759303', -- tescopartnertoolkit databricks workspace
  '5294095351256433', -- msd databricks workspace
  '8214564272272557', -- horizon databricks workspace
  '5996698886943388', -- rsi databricks workspace
  '4478472492095652' -- vendornet databricks workspace
)
and usage_date >= '""" + min_date_filter + """'"""

df_system_billing_table_data_ingestion = spark.sql(system_billing_table_query_data_ingestion)

print(df_system_billing_table_data_ingestion.count())

# COMMAND ----------

display(df_system_billing_table_data_ingestion)

# COMMAND ----------

# Split the 'split_data' column into two columns
split_data_df_data_ingestion = df_system_billing_table_data_ingestion.withColumn("key", pyf.split(df_system_billing_table_data_ingestion["split_data"], ":").getItem(0))
split_data_df_data_ingestion = split_data_df_data_ingestion.withColumn("value", pyf.split(df_system_billing_table_data_ingestion["split_data"], ":").getItem(1))

# Pivot the DataFrame
pivoted_df_data_ingestion = split_data_df_data_ingestion.groupBy(group_cols).pivot("key").agg({"value": "first"})

# Show the result
display(pivoted_df_data_ingestion)

# COMMAND ----------

pivoted_df_data_ingestion_agg_by_job_cluster_acosta_customer = pivoted_df_data_ingestion.select(selected_cols).groupBy(
  'sku_name', 'data_label', 'job_cluster_acosta_customer', 'job_cluster_country_cd', 'usage_unit').agg(
    pyf.countDistinct("usage_date").alias('num_ingestion_days'),
    pyf.countDistinct("job_id").alias('num_distinct_jobs'),
    pyf.sum("usage_quantity").alias('total_usage_quantity'), 
    pyf.avg("list_price").alias('average_list_price'), 
    pyf.sum("list_cost").alias('total_cost')
    ).orderBy('sku_name', 'data_label', 'job_cluster_country_cd', 'job_cluster_acosta_customer', 'usage_unit')
  
display(pivoted_df_data_ingestion_agg_by_job_cluster_acosta_customer)

# COMMAND ----------

df_data_ingestion_by_customer = pivoted_df_data_ingestion_agg_by_job_cluster_acosta_customer.select(final_selected_cols).withColumnRenamed('job_cluster_country_cd', 'country')

df_data_ingestion_by_customer = df_data_ingestion_by_customer.withColumn("retailer", pyf.expr(expr_str1))
df_data_ingestion_by_customer = df_data_ingestion_by_customer.withColumn("client", pyf.expr(expr_str2))
df_data_ingestion_by_customer = df_data_ingestion_by_customer.withColumn("system", pyf.expr(expr_str3))

df_data_ingestion_by_customer = df_data_ingestion_by_customer.withColumnRenamed('num_distinct_jobs', 'data_ingestion_jobs')
df_data_ingestion_by_customer = df_data_ingestion_by_customer.withColumnRenamed('total_usage_quantity', 'data_ingestion_DBUs')
df_data_ingestion_by_customer = df_data_ingestion_by_customer.withColumnRenamed('total_cost', 'data_ingestion_cost')

df_data_ingestion_by_customer = df_data_ingestion_by_customer.select("sku_name", "system", "retailer", "client", "country", "data_ingestion_jobs", "data_ingestion_DBUs", "data_ingestion_cost")

display(df_data_ingestion_by_customer)

# COMMAND ----------

pivoted_df_data_ingestion_agg_by_data_label = pivoted_df_data_ingestion.select(selected_cols_plus_workspace_id).groupBy(
  'data_label', 'workspace_id').agg(
    pyf.countDistinct("job_cluster_acosta_customer").alias('num_distinct_clients'), 
    pyf.countDistinct("usage_date").alias('num_ingestion_days'),   
    pyf.countDistinct("job_id").alias('num_distinct_jobs'),
    pyf.sum("usage_quantity").alias('total_usage_quantity'), 
    pyf.avg("list_price").alias('average_list_price'), 
    pyf.sum("list_cost").alias('total_cost')
    ).orderBy('data_label', 'workspace_id')
  
display(pivoted_df_data_ingestion_agg_by_data_label)

# COMMAND ----------

# Define the join conditions
join_conditions = ['retailer', 'client', 'country']
join_conditions_df_all_costs_by_customer =  ['retailer', 'client', 'country', 'team']

# Perform outer join for each pair of dataframes
df_all_costs_by_customer = df_retailer_client

for df in [df_Forecast_DRFE_by_customer, df_value_measurement_by_customer, df_DLA_alert_gen_by_customer, df_team_alerts_by_customer,
           df_retail_report_by_customer, df_data_ingestion_by_customer]:
    if df == df_team_alerts_by_customer:
        df_all_costs_by_customer = df_all_costs_by_customer.drop('sku_name', 'system').join(df, on=join_conditions_df_all_costs_by_customer, how='left_outer').drop('sku_name', 'system')
    else:
        df_all_costs_by_customer = df_all_costs_by_customer.drop('sku_name', 'system').join(df, on=join_conditions, how='left_outer').drop('sku_name', 'system')

# Show the resulting DataFrame
df_all_costs_by_customer = df_all_costs_by_customer.orderBy('country', 'retailer', 'client', 'team')

df_all_costs_by_customer = df_all_costs_by_customer.withColumn("total_jobs", pyf.expr(expr_str4)).filter(pyf.col('total_jobs') > 0)
df_all_costs_by_customer = df_all_costs_by_customer.withColumn("total_DBUs", pyf.expr(expr_str5))
df_all_costs_by_customer = df_all_costs_by_customer.withColumn("total_cost", pyf.expr(expr_str6))

df_all_costs_by_customer = df_all_costs_by_customer.withColumn("num_inv_alerts", pyf.col('num_inv_alerts_P3M'))
df_all_costs_by_customer = df_all_costs_by_customer.withColumn("num_osa_alerts", pyf.col('num_osa_alerts_P3M'))
df_all_costs_by_customer = df_all_costs_by_customer.withColumn("total_num_alerts", pyf.col('num_inv_alerts') + pyf.col('num_osa_alerts'))

df_all_costs_by_customer = df_all_costs_by_customer.drop("retail_report_jobs", "retail_report_DBUs", "retail_report_cost",
                                                         "num_inv_alerts_P3M", "num_osa_alerts_P3M")

display(df_all_costs_by_customer)

# COMMAND ----------

df_all_costs_by_customer_duplicates = df_all_costs_by_customer.groupby([
    'retailer', 
    'client', 
    'country',
    'parent_chain_id',
    'DLA_active_flg',
    'DRFE_forecast_jobs',
    'DRFE_forecast_DBUs',
    'DRFE_forecast_cost',
    'value_measurement_jobs',
    'value_measurement_DBUs',
    'value_measurement_cost',
    'DLA_alert_gen_jobs',
    'DLA_alert_gen_DBUs',
    'DLA_alert_gen_cost',
    'data_ingestion_jobs',
    'data_ingestion_DBUs',
    'data_ingestion_cost'
]).count().where('count > 1').drop('count')

df_all_costs_by_customer_duplicates = df_all_costs_by_customer_duplicates.withColumns({'team_alerts_jobs': pyf.lit(0), 'team_alerts_DBUs': pyf.lit(0), 'team_alerts_cost': pyf.lit(0)})

df_all_costs_by_customer_duplicates = df_all_costs_by_customer_duplicates.withColumn("total_jobs", pyf.expr(expr_str4)).filter(pyf.col('total_jobs') > 0)
df_all_costs_by_customer_duplicates = df_all_costs_by_customer_duplicates.withColumn("total_DBUs", pyf.expr(expr_str5))
df_all_costs_by_customer_duplicates = df_all_costs_by_customer_duplicates.withColumn("total_cost", pyf.expr(expr_str6))

df_all_costs_by_customer_duplicates = df_all_costs_by_customer_duplicates.withColumns({'num_inv_alerts': pyf.lit(0), 'num_osa_alerts': pyf.lit(0), 'total_num_alerts': pyf.lit(0)})

display(df_all_costs_by_customer_duplicates)

# COMMAND ----------

df_all_alerts_and_costs_P3M = df_all_costs_by_customer.agg(
      pyf.sum("DRFE_forecast_jobs").alias('total_DRFE_forecast_jobs'),
      pyf.sum("DRFE_forecast_DBUs").alias('total_DRFE_forecast_DBUs'),
      pyf.sum("DRFE_forecast_cost").alias('total_DRFE_forecast_cost'),

      pyf.sum("value_measurement_jobs").alias('total_value_measurement_jobs'),
      pyf.sum("value_measurement_DBUs").alias('total_value_measurement_DBUs'),
      pyf.sum("value_measurement_cost").alias('total_value_measurement_cost'),

      pyf.sum("DLA_alert_gen_jobs").alias('total_DLA_alert_gen_jobs'),
      pyf.sum("DLA_alert_gen_DBUs").alias('total_DLA_alert_gen_DBUs'),
      pyf.sum("DLA_alert_gen_cost").alias('total_DLA_alert_gen_cost'),

      pyf.sum("team_alerts_jobs").alias('total_team_alerts_jobs'),
      pyf.sum("team_alerts_DBUs").alias('total_team_alerts_DBUs'),
      pyf.sum("team_alerts_cost").alias('total_team_alerts_cost'),
      
      pyf.sum("data_ingestion_jobs").alias('total_data_ingestion_jobs'),
      pyf.sum("data_ingestion_DBUs").alias('total_data_ingestion_DBUs'),
      pyf.sum("data_ingestion_cost").alias('total_data_ingestion_cost'),

      pyf.sum("total_jobs").alias('total_jobs'),
      pyf.sum("total_DBUs").alias('total_DBUs'),
      pyf.sum("total_cost").alias('total_cost'),

      pyf.sum("num_inv_alerts").alias('total_num_inv_alerts'),
      pyf.sum("num_osa_alerts").alias('total_num_osa_alerts'),
      pyf.sum("total_num_alerts").alias('total_num_alerts')
)

df_all_alerts_and_costs_P3M_duplicates = df_all_costs_by_customer_duplicates.agg(
      pyf.sum("DRFE_forecast_jobs").alias('total_DRFE_forecast_jobs'),
      pyf.sum("DRFE_forecast_DBUs").alias('total_DRFE_forecast_DBUs'),
      pyf.sum("DRFE_forecast_cost").alias('total_DRFE_forecast_cost'),

      pyf.sum("value_measurement_jobs").alias('total_value_measurement_jobs'),
      pyf.sum("value_measurement_DBUs").alias('total_value_measurement_DBUs'),
      pyf.sum("value_measurement_cost").alias('total_value_measurement_cost'),

      pyf.sum("DLA_alert_gen_jobs").alias('total_DLA_alert_gen_jobs'),
      pyf.sum("DLA_alert_gen_DBUs").alias('total_DLA_alert_gen_DBUs'),
      pyf.sum("DLA_alert_gen_cost").alias('total_DLA_alert_gen_cost'),

      pyf.sum("team_alerts_jobs").alias('total_team_alerts_jobs'),
      pyf.sum("team_alerts_DBUs").alias('total_team_alerts_DBUs'),
      pyf.sum("team_alerts_cost").alias('total_team_alerts_cost'),
      
      pyf.sum("data_ingestion_jobs").alias('total_data_ingestion_jobs'),
      pyf.sum("data_ingestion_DBUs").alias('total_data_ingestion_DBUs'),
      pyf.sum("data_ingestion_cost").alias('total_data_ingestion_cost'),

      pyf.sum("total_jobs").alias('total_jobs'),
      pyf.sum("total_DBUs").alias('total_DBUs'),
      pyf.sum("total_cost").alias('total_cost'),

      pyf.sum("num_inv_alerts").alias('total_num_inv_alerts'),
      pyf.sum("num_osa_alerts").alias('total_num_osa_alerts'),
      pyf.sum("total_num_alerts").alias('total_num_alerts')
)

pandas_df_all_alerts_and_costs_P3M = (df_all_alerts_and_costs_P3M.toPandas() - df_all_alerts_and_costs_P3M_duplicates.toPandas()).transpose()

pandas_df_all_alerts_and_costs_P3M

# COMMAND ----------


