# Databricks notebook source
from pprint import pprint

import pyspark
import datetime
import warnings
from dateutil.relativedelta import relativedelta

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from pyspark.sql.window import Window
from pyspark.sql.functions import count, ltrim, rtrim, col, rank, regexp_replace, desc, when

# COMMAND ----------

today_date = datetime.date.today()
min_date_filter = (today_date - relativedelta(days=90)).strftime(format='%Y-%m-%d')

# COMMAND ----------

attributes_by_retailer_client_schema = StructType([
  StructField('retailer_client', StringType(), True),
  
  StructField('num_sales_stores', IntegerType(), True), 
  StructField('num_sales_items', IntegerType(), True),
  StructField('num_sales_days', IntegerType(), True),
  StructField('max_sales_date', DateType(), True)
  
  ])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###DLA Clients

# COMMAND ----------

dla_epos_dvs = [
  "retaillink_walmart_bluetritonbrands_us_dv",
  ## "retaillink_walmart_campbells_us_dv",
  "retaillink_walmart_danoneusllc_us_dv",
  # "retaillink_walmart_dominosugar_us_dv",
  # "retaillink_walmart_gpconsumerproductsoperation_us_dv",
  "retaillink_walmart_harrysinc_us_dv",
  # "retaillink_walmart_johnsonandjohnson_us_dv",
  ## "retaillink_walmart_milos_us_dv",
  ## "retaillink_walmart_minutemaidcompany_us_dv",
  "retaillink_walmart_rbusa_us_dv",
  "retaillink_walmart_sanofi_us_dv",
  # "retaillink_walmart_trucoenterprisedbaotborder_us_dv",

  "retaillink_walmart_rbusa_us_dv",

  "market6_kroger_barilla_us_dv",
  "market6_kroger_campbells_us_dv",
  "market6_kroger_cocacola_us_dv",
  "market6_kroger_danonewave_us_dv",
  "market6_kroger_tyson_us_dv",
  "market6_kroger_wildcat_us_dv",

  "bigred_target_barilla_us_dv",
  "bigred_target_campbells_us_dv",
  "bigred_target_wildcat_us_dv",

  "retaillink_walmart_bandgfoods_ca_dv",
  "retaillink_walmart_campbells_ca_dv",
  "retaillink_walmart_catelli_ca_dv",
  "retaillink_walmart_danone_ca_dv",
  "retaillink_walmart_lego_ca_dv",
  "retaillink_walmart_rbhealth_ca_dv",
  "retaillink_walmart_smuckers_ca_dv",
  "retaillink_walmart_voortman_ca_dv",

  "retaillink_asda_kraftheinz_uk_dv",
  "msd_morrisons_kraftheinz_uk_dv",
  "horizon_sainsburys_kraftheinz_uk_dv",
  "tescopartnertoolkit_tesco_kraftheinz_uk_dv",

  "retaillink_asda_nestlecore_uk_dv",
  "msd_morrisons_nestlecore_uk_dv",
  "horizon_sainsburys_nestlecore_uk_dv",
  "tescopartnertoolkit_tesco_nestlecore_uk_dv",

  "retaillink_asda_heineken_uk_dv",
  "msd_morrisons_heineken_uk_dv",
  "horizon_sainsburys_heineken_uk_dv",
  "tescopartnertoolkit_tesco_heineken_uk_dv",

  "retaillink_asda_premier_uk_dv",
  "msd_morrisons_premier_uk_dv",
  "tescopartnertoolkit_tesco_premier_uk_dv",

  "retaillink_asda_droetker_uk_dv",
  "msd_morrisons_droetker_uk_dv",
  "horizon_sainsburys_droetker_uk_dv",
  "tescopartnertoolkit_tesco_droetker_uk_dv",

  "retaillink_asda_beiersdorf_uk_dv",
  "msd_morrisons_beiersdorf_uk_dv",
  "tescopartnertoolkit_tesco_beiersdorf_uk_dv",
  "rsi_boots_beiersdorf_uk_dv",

  "retaillink_asda_redbull_uk_dv",
  "msd_morrisons_redbull_uk_dv",
  "horizon_sainsburys_redbull_uk_dv",
  "tescopartnertoolkit_tesco_redbull_uk_dv",

  "retaillink_asda_johnsonandjohnson_uk_dv",
  "msd_morrisons_johnsonandjohnson_uk_dv",
  "horizon_sainsburys_johnsonandjohnson_uk_dv",
  "tescopartnertoolkit_tesco_johnsonandjohnson_uk_dv",

  "retaillink_asda_perfettivanmelle_uk_dv",
  "msd_morrisons_perfettivanmelle_uk_dv",
  "tescopartnertoolkit_tesco_perfettivanmelle_uk_dv",

  "retaillink_asda_brownforman_uk_dv",
  "horizon_sainsburys_brownforman_uk_dv",
  "tescopartnertoolkit_tesco_brownforman_uk_dv"
]

# COMMAND ----------

# Optional: test individual retailer/clients

# dla_epos_dvs = [
#   "retaillink_walmart_bluetritonbrands_us_dv",
#   ## "retaillink_walmart_campbells_us_dv",
#   "retaillink_walmart_danoneusllc_us_dv",
#   # "retaillink_walmart_dominosugar_us_dv",
#   # "retaillink_walmart_gpconsumerproductsoperation_us_dv",
#   "retaillink_walmart_harrysinc_us_dv",
#   # "retaillink_walmart_johnsonandjohnson_us_dv",
#   ## "retaillink_walmart_milos_us_dv",
#   ## "retaillink_walmart_minutemaidcompany_us_dv",
#   "retaillink_walmart_rbusa_us_dv",
#   "retaillink_walmart_sanofi_us_dv",
#   # "retaillink_walmart_trucoenterprisedbaotborder_us_dv",

#   "retaillink_walmart_rbusa_us_dv",

#   "market6_kroger_barilla_us_dv",
#   "market6_kroger_campbells_us_dv",
#   "market6_kroger_cocacola_us_dv",
#   "market6_kroger_danonewave_us_dv",
#   "market6_kroger_tyson_us_dv",
#   "market6_kroger_wildcat_us_dv",

#   "bigred_target_barilla_us_dv",
#   "bigred_target_campbells_us_dv",
#   "bigred_target_wildcat_us_dv",

#   "retaillink_walmart_bandgfoods_ca_dv",
#   "retaillink_walmart_campbells_ca_dv",
#   "retaillink_walmart_catelli_ca_dv",
#   "retaillink_walmart_danone_ca_dv",
#   "retaillink_walmart_lego_ca_dv",
#   "retaillink_walmart_rbhealth_ca_dv",
#   "retaillink_walmart_smuckers_ca_dv",
#   "retaillink_walmart_voortman_ca_dv",

#   "retaillink_asda_kraftheinz_uk_dv",
#   "msd_morrisons_kraftheinz_uk_dv",
#   "horizon_sainsburys_kraftheinz_uk_dv",
#   "tescopartnertoolkit_tesco_kraftheinz_uk_dv",

#   "retaillink_asda_nestlecore_uk_dv",
#   "msd_morrisons_nestlecore_uk_dv",
#   "horizon_sainsburys_nestlecore_uk_dv",
#   "tescopartnertoolkit_tesco_nestlecore_uk_dv",

#   "retaillink_asda_heineken_uk_dv",
#   "msd_morrisons_heineken_uk_dv",
#   "horizon_sainsburys_heineken_uk_dv",
#   "tescopartnertoolkit_tesco_heineken_uk_dv",

#   "retaillink_asda_premier_uk_dv",
#   "msd_morrisons_premier_uk_dv",
#   "tescopartnertoolkit_tesco_premier_uk_dv",

#   "retaillink_asda_droetker_uk_dv",
#   "msd_morrisons_droetker_uk_dv",
#   "horizon_sainsburys_droetker_uk_dv",
#   "tescopartnertoolkit_tesco_droetker_uk_dv",

#   "retaillink_asda_beiersdorf_uk_dv",
#   "msd_morrisons_beiersdorf_uk_dv",
#   "tescopartnertoolkit_tesco_beiersdorf_uk_dv",
#   "rsi_boots_beiersdorf_uk_dv",

#   "retaillink_asda_redbull_uk_dv",
#   "msd_morrisons_redbull_uk_dv",
#   "horizon_sainsburys_redbull_uk_dv",
#   "tescopartnertoolkit_tesco_redbull_uk_dv",

#   "retaillink_asda_johnsonandjohnson_uk_dv",
#   "msd_morrisons_johnsonandjohnson_uk_dv",
#   "horizon_sainsburys_johnsonandjohnson_uk_dv",
#   "tescopartnertoolkit_tesco_johnsonandjohnson_uk_dv",

#   "retaillink_asda_perfettivanmelle_uk_dv",
#   "msd_morrisons_perfettivanmelle_uk_dv",
#   "tescopartnertoolkit_tesco_perfettivanmelle_uk_dv"

#   "retaillink_asda_brownforman_uk_dv",
#   "horizon_sainsburys_brownforman_uk_dv",
#   "tescopartnertoolkit_tesco_brownforman_uk_dv"
# ]

# COMMAND ----------

dla_attributes_by_retailer_client = spark.createDataFrame([], attributes_by_retailer_client_schema)

for dla_epos_dv in dla_epos_dvs:
  dla_sql_attributes_by_retailer_client = """
  select
    '""" + dla_epos_dv + """' as retailer_client,

    count(distinct sales.organization_unit_num) as num_sales_stores,
    count(distinct sales.retailer_item_id) as num_sales_items,
    count(distinct sales.sales_dt) as num_sales_days,
    max(sales.sales_dt) as max_sales_date

  from
    """ + dla_epos_dv + """.vw_latest_sat_epos_summary sales
  
  where
    sales_dt >= '""" + min_date_filter + """'
  """

  dla_df_attributes_by_retailer_client = spark.sql(dla_sql_attributes_by_retailer_client)
  
  dla_attributes_by_retailer_client = dla_attributes_by_retailer_client.union(dla_df_attributes_by_retailer_client)

# COMMAND ----------

display(dla_attributes_by_retailer_client)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###NARS DLA Clients

# COMMAND ----------

attributes_by_retailer_client_schema_all_history = StructType([
  StructField('retailer_client', StringType(), True),
  
  StructField('num_sales_stores', IntegerType(), True), 
  StructField('num_sales_items', IntegerType(), True),
  StructField('num_sales_days', IntegerType(), True),
  StructField('min_sales_date', DateType(), True),
  StructField('max_sales_date', DateType(), True)
  
  ])

# COMMAND ----------

# Optional: test individual retailer/clients

dla_epos_dvs_all_history = [
  # "retaillink_walmart_bluetritonbrands_us_dv",
  # ## "retaillink_walmart_campbells_us_dv",
  # "retaillink_walmart_danoneusllc_us_dv",
  # # "retaillink_walmart_dominosugar_us_dv",
  # # "retaillink_walmart_gpconsumerproductsoperation_us_dv",
  # "retaillink_walmart_harrysinc_us_dv",
  # # "retaillink_walmart_johnsonandjohnson_us_dv",
  # ## "retaillink_walmart_milos_us_dv",
  # ## "retaillink_walmart_minutemaidcompany_us_dv",
  # "retaillink_walmart_rbusa_us_dv",
  # "retaillink_walmart_sanofi_us_dv",
  # # "retaillink_walmart_trucoenterprisedbaotborder_us_dv",

  # "retaillink_walmart_rbusa_us_dv",

  # "market6_kroger_barilla_us_dv",
  # "market6_kroger_campbells_us_dv",
  # "market6_kroger_cocacola_us_dv",
  # "market6_kroger_danonewave_us_dv",
  # "market6_kroger_tyson_us_dv",
  # "market6_kroger_wildcat_us_dv",

  # "bigred_target_barilla_us_dv",
  # "bigred_target_campbells_us_dv",
  # "bigred_target_wildcat_us_dv",

  # "retaillink_walmart_bandgfoods_ca_dv",
  # "retaillink_walmart_campbells_ca_dv",
  # "retaillink_walmart_catelli_ca_dv",
  # "retaillink_walmart_danone_ca_dv",
  # "retaillink_walmart_lego_ca_dv",
  # "retaillink_walmart_rbhealth_ca_dv",
  # "retaillink_walmart_smuckers_ca_dv",
  # "retaillink_walmart_voortman_ca_dv",

  # "retaillink_asda_kraftheinz_uk_dv",
  # "msd_morrisons_kraftheinz_uk_dv",
  # "horizon_sainsburys_kraftheinz_uk_dv",
  # "tescopartnertoolkit_tesco_kraftheinz_uk_dv",

  "retaillink_asda_nestlecore_uk_dv",
  "msd_morrisons_nestlecore_uk_dv",
  "horizon_sainsburys_nestlecore_uk_dv",
  "tescopartnertoolkit_tesco_nestlecore_uk_dv"

  # "retaillink_asda_heineken_uk_dv",
  # "msd_morrisons_heineken_uk_dv",
  # "horizon_sainsburys_heineken_uk_dv",
  # "tescopartnertoolkit_tesco_heineken_uk_dv",

  # "retaillink_asda_premier_uk_dv",
  # "msd_morrisons_premier_uk_dv",
  # "tescopartnertoolkit_tesco_premier_uk_dv",

  # "retaillink_asda_droetker_uk_dv",
  # "msd_morrisons_droetker_uk_dv",
  # "horizon_sainsburys_droetker_uk_dv",
  # "tescopartnertoolkit_tesco_droetker_uk_dv",

  # "retaillink_asda_beiersdorf_uk_dv",
  # "msd_morrisons_beiersdorf_uk_dv",
  # "tescopartnertoolkit_tesco_beiersdorf_uk_dv",
  # "rsi_boots_beiersdorf_uk_dv",

  # "retaillink_asda_redbull_uk_dv",
  # "msd_morrisons_redbull_uk_dv",
  # "horizon_sainsburys_redbull_uk_dv",
  # "tescopartnertoolkit_tesco_redbull_uk_dv",

  # "retaillink_asda_johnsonandjohnson_uk_dv",
  # "msd_morrisons_johnsonandjohnson_uk_dv",
  # "horizon_sainsburys_johnsonandjohnson_uk_dv",
  # "tescopartnertoolkit_tesco_johnsonandjohnson_uk_dv",

  # "retaillink_asda_perfettivanmelle_uk_dv",
  # "msd_morrisons_perfettivanmelle_uk_dv",
  # "tescopartnertoolkit_tesco_perfettivanmelle_uk_dv"

  # "retaillink_asda_brownforman_uk_dv",
  # "horizon_sainsburys_brownforman_uk_dv",
  # "tescopartnertoolkit_tesco_brownforman_uk_dv"
]

# COMMAND ----------

dla_attributes_by_retailer_client_all_history = spark.createDataFrame([], attributes_by_retailer_client_schema_all_history)

for dla_epos_dv_all_history in dla_epos_dvs_all_history:
  dla_sql_attributes_by_retailer_client_all_history = """
  select
    '""" + dla_epos_dv_all_history + """' as retailer_client,

    count(distinct sales.organization_unit_num) as num_sales_stores,
    count(distinct sales.retailer_item_id) as num_sales_items,
    count(distinct sales.sales_dt) as num_sales_days,
    min(sales.sales_dt) as min_sales_date,
    max(sales.sales_dt) as max_sales_date

  from
    """ + dla_epos_dv_all_history + """.vw_latest_sat_epos_summary sales
  """

  dla_df_attributes_by_retailer_client_all_history = spark.sql(dla_sql_attributes_by_retailer_client_all_history)
  
  dla_attributes_by_retailer_client_all_history = dla_attributes_by_retailer_client_all_history.union(dla_df_attributes_by_retailer_client_all_history)

# COMMAND ----------

display(dla_attributes_by_retailer_client_all_history)

# COMMAND ----------


