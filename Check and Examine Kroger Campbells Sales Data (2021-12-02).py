# Databricks notebook source
import uuid
import warnings
import numpy as np
import pandas as pd
from datetime import datetime
from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark.sql import types as pyt
from pyspark.sql import functions as pyf
import acosta
from acosta.alerting.preprocessing import read_pos_data
from acosta.alerting.helpers import check_path_exists


print(acosta.__version__)
auto_model_prefix = 'model'
current_timestamp = datetime.now().strftime('%Y-%m-%d')

# COMMAND ----------

sqlContext.setConf("spark.sql.shuffle.partitions", "800")

# COMMAND ----------

dbutils.widgets.text('retailer', 'walmart', 'Retailer')
dbutils.widgets.text('client', 'clorox', 'Client')
dbutils.widgets.text('country_code', 'us', 'Country Code')

dbutils.widgets.text('store', '', 'Organization Unit Num')
dbutils.widgets.text('item', '', 'Retailer Item ID')
dbutils.widgets.text('runid', '', 'Run ID')
dbutils.widgets.text('timestamp', current_timestamp, 'Timestamp')

dbutils.widgets.text('source_system', 'retaillink', 'Source System')

RETAILER = dbutils.widgets.get('retailer').strip().lower()
CLIENT = dbutils.widgets.get('client').strip().lower()
COUNTRY_CODE = dbutils.widgets.get('country_code').strip().lower()

RUN_ID = dbutils.widgets.get('runid').strip()
TIMESTAMP = dbutils.widgets.get('timestamp').strip()

SOURCE_SYSTEM = dbutils.widgets.get('source_system').strip()

try:
    STORE = int(dbutils.widgets.get('store').strip())
except ValueError:
    STORE = None
try:
    ITEM = str(dbutils.widgets.get('item').strip())
except ValueError:
    ITEM = None

if RETAILER == '':
    raise ValueError('\'retailer\' is a required parameter.  Please provide a value.')

if CLIENT == '':
    raise ValueError('\'client\' is a required parameter.  Please provide a value.')

if RUN_ID == '':
    RUN_ID = str(uuid.uuid4())
elif RUN_ID.lower() == 'auto':
    RUN_ID = '-'.join([auto_model_prefix, CLIENT, TIMESTAMP])

# PATHS
PATH_DATA_VAULT_TRANSFORM_OUTPUT = '/mnt/processed/training/{run_id}/data_vault/'.format(run_id=RUN_ID)

for param in [SOURCE_SYSTEM, RETAILER, CLIENT, COUNTRY_CODE, STORE, ITEM, TIMESTAMP, RUN_ID]:
    print(param)

# COMMAND ----------

# MAGIC %sql
# MAGIC select sales_dt, count(*), sum(POS_ITEM_QTY) from market6_kroger_campbells_us_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt >= '2024-07-01'
# MAGIC group by sales_dt
# MAGIC order by sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC select min(sales_dt), max(sales_dt)
# MAGIC from market6_kroger_campbells_us_dv.vw_latest_sat_epos_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from retaillink_walmart_sanofi_us_dv.vw_latest_sat_epos_summary
# MAGIC where pos_item_qty = 0
# MAGIC and pos_amt = 0
# MAGIC and sales_dt >= '2024-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from market6_kroger_campbells_us_dv.vw_latest_sat_epos_summary
# MAGIC where pos_item_qty = 0
# MAGIC and pos_amt = 0

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from retaillink_walmart_harrysinc_us_dv.vw_latest_sat_epos_summary
# MAGIC where pos_item_qty = 0
# MAGIC and pos_amt = 0
# MAGIC and sales_dt >= '2024-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC select sales_dt, count(*)
# MAGIC from rsi_boots_perfettivanmelle_uk_dv.vw_latest_sat_epos_summary
# MAGIC group by sales_dt
# MAGIC order by sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC select sales_dt, count(*)
# MAGIC from rsi_boots_perfettivanmelle_uk_dv.vw_latest_sat_epos_summary
# MAGIC where pos_item_qty = 0
# MAGIC and pos_amt = 0
# MAGIC group by sales_dt
# MAGIC order by sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC select sales_dt, count(*)
# MAGIC from rsi_boots_beiersdorf_uk_dv.vw_latest_sat_epos_summary
# MAGIC group by sales_dt
# MAGIC order by sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC select sales_dt, count(*)
# MAGIC from rsi_boots_beiersdorf_uk_dv.vw_latest_sat_epos_summary
# MAGIC where pos_item_qty = 0
# MAGIC and pos_amt = 0
# MAGIC group by sales_dt
# MAGIC order by sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC select sales_dt, count(*)
# MAGIC from rsi_boots_beiersdorf_uk_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt >= '2023-01-01'
# MAGIC group by sales_dt
# MAGIC order by sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC select sales_dt, count(*)
# MAGIC from rsi_boots_beiersdorf_uk_dv.vw_latest_sat_epos_summary
# MAGIC where pos_item_qty = 0
# MAGIC and pos_amt = 0
# MAGIC and sales_dt >= '2023-01-01'
# MAGIC group by sales_dt
# MAGIC order by sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC select sales_dt, count(*)
# MAGIC from horizon_sainsburys_droetker_uk_dv.vw_latest_sat_epos_summary
# MAGIC where pos_item_qty = 0
# MAGIC and pos_amt = 0
# MAGIC and sales_dt >= '2024-01-01'
# MAGIC group by sales_dt
# MAGIC order by sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC select sales_dt, count(*)
# MAGIC from horizon_sainsburys_brownforman_uk_dv.vw_latest_sat_epos_summary
# MAGIC where pos_item_qty = 0
# MAGIC and pos_amt = 0
# MAGIC and sales_dt >= '2024-01-01'
# MAGIC group by sales_dt
# MAGIC order by sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from horizon_sainsburys_brownforman_uk_dv.vw_latest_sat_epos_summary
# MAGIC where pos_item_qty = 0
# MAGIC and pos_amt = 0
# MAGIC and sales_dt >= '2024-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from tescopartnertoolkit_tesco_brownforman_uk_dv.vw_latest_sat_epos_summary
# MAGIC where pos_item_qty = 0
# MAGIC and pos_amt = 0
# MAGIC and sales_dt >= '2024-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from msd_morrisons_brownforman_uk_dv.vw_latest_sat_epos_summary
# MAGIC where pos_item_qty = 0
# MAGIC and pos_amt = 0
# MAGIC and sales_dt >= '2024-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from msd_morrisons_beiersdorf_uk_dv.vw_latest_sat_epos_summary
# MAGIC where pos_item_qty = 0
# MAGIC and pos_amt = 0
# MAGIC and sales_dt >= '2024-02-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from retaillink_asda_beiersdorf_uk_dv.vw_latest_sat_epos_summary
# MAGIC where pos_item_qty = 0
# MAGIC and pos_amt = 0
# MAGIC and sales_dt >= '2024-02-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from vendornet_meijer_rbusa_us_dv.vw_latest_sat_epos_summary
# MAGIC where pos_item_qty = 0
# MAGIC and pos_amt = 0
# MAGIC and sales_dt >= '2024-02-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from bigred_target_wildcat_us_dv.vw_latest_sat_epos_summary
# MAGIC where pos_item_qty = 0
# MAGIC and pos_amt = 0
# MAGIC and sales_dt >= '2024-02-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct retailer_item_desc
# MAGIC from bigred_target_wildcat_us_dv.sat_retailer_item

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from bigred_target_wildcat_us_dv.sat_retailer_item
# MAGIC where retailer_item_desc in ('Harry''s Stone Body Wash - 16 fl oz', 'Harry''s Stone Body Wash - 16oz')
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from bigred_target_wildcat_us_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt >= '2024-02-01'
# MAGIC and retailer_item_id in ('049-00-1626', '049-00-1857') -- HARRYS STONE BODY WASH 16 FO
# MAGIC and organization_unit_num = 5
# MAGIC order by organization_unit_num, sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct *
# MAGIC from retaillink_walmart_harrysinc_us_dv.sat_retailer_item
# MAGIC where retailer_item_desc = 'HARRYS STONE BW 16OZ'
# MAGIC order by retailer_item_desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct retailer_item_desc
# MAGIC from market6_kroger_wildcat_us_dv.sat_retailer_item
# MAGIC order by retailer_item_desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from market6_kroger_wildcat_us_dv.sat_retailer_item
# MAGIC where retailer_item_desc in ('HARRYS STONE BODY WASH', 'HARRYS STONE BODY WASH TS')
# MAGIC order by retailer_item_desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from market6_kroger_wildcat_us_dv.sat_retailer_item_upc
# MAGIC where retailer_item_id in ('0085523500748', '0081000647699')

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from retaillink_walmart_harrysinc_us_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt >= '2024-02-01'
# MAGIC and retailer_item_id in ('573656549') -- HARRYS STONE BODY WASH 16 FO
# MAGIC order by organization_unit_num, sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC select organization_unit_num, pos_item_qty, pos_amt, sales_dt
# MAGIC from retaillink_walmart_harrysinc_us_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt >= '2024-02-01'
# MAGIC and retailer_item_id in ('573656549') -- HARRYS STONE BODY WASH 16 FO
# MAGIC and organization_unit_num = 5
# MAGIC order by organization_unit_num, sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC select organization_unit_num, pos_item_qty, pos_amt, sales_dt
# MAGIC from market6_kroger_wildcat_us_dv.vw_latest_sat_epos_summary
# MAGIC where sales_dt >= '2024-02-01'
# MAGIC and retailer_item_id in ('0085523500748') -- HARRYS STONE BODY WASH 16 FO
# MAGIC and organization_unit_num in ('101-620', '100-620')
# MAGIC order by organization_unit_num, sales_dt

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from market6_kroger_wildcat_us_dv.vw_latest_sat_epos_summary
# MAGIC where pos_item_qty = 0
# MAGIC and pos_amt = 0

# COMMAND ----------

# Read POS data
data_vault_data = read_pos_data(SOURCE_SYSTEM, RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')

# COMMAND ----------

# data = data_vault_data

# COMMAND ----------

# Let's only look as 2021 onwards
data = data_vault_data.where(((col("SALES_DT") < lit("2022-01-01")) & (col("SALES_DT") >= lit("2021-01-01"))) & (col("ON_HAND_INVENTORY_QTY") >= 5000))

# COMMAND ----------

data.count()

# COMMAND ----------

# if STORE:
#     data = data.filter(f'ORGANIZATION_UNIT_NUM == "{STORE}"')

# if ITEM:
#     data = data.filter(f'RETAILER_ITEM_ID == "{ITEM}"')

# COMMAND ----------

data = data.orderBy('SALES_DT', ascending=False)

# COMMAND ----------

sales_dates = data.select('SALES_DT').distinct().orderBy('SALES_DT', ascending=False)

# COMMAND ----------

display(sales_dates)

# COMMAND ----------

display(data)

# COMMAND ----------

data.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC use market6_kroger_campbells_us_dv;
# MAGIC
# MAGIC Select sales_dt,  sum(pos_item_qty), sum(pos_amt), sum(on_hand_inventory_qty) 
# MAGIC from vw_latest_sat_epos_summary
# MAGIC Where 
# MAGIC AUDIT_CURRENT_IND = 1
# MAGIC and sales_dt >= '2021-07-01'
# MAGIC and sales_dt <= '2021-12-31'
# MAGIC group by 1
# MAGIC order by 1 

# COMMAND ----------


