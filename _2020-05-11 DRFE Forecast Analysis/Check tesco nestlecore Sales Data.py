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

dbutils.widgets.text('retailer', 'walmart', 'Retailer')
dbutils.widgets.text('client', 'clorox', 'Client')
dbutils.widgets.text('countrycode', 'us', 'Country Code')
dbutils.widgets.text('store', '', 'Organization Unit Num')
dbutils.widgets.text('item', '', 'Retailer Item ID')

RETAILER = dbutils.widgets.get('retailer').strip().lower()
CLIENT = dbutils.widgets.get('client').strip().lower()
COUNTRY_CODE = dbutils.widgets.get('countrycode').strip().lower()

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

if COUNTRY_CODE  == '':
    raise ValueError('\'countrycode\' is a required parameter.  Please provide a value.')

for param in [RETAILER, CLIENT, COUNTRY_CODE, STORE, ITEM]:
    print(param)

# COMMAND ----------

def read_pos_data(retailer, client, country_code, sql_context):
    """
    Reads in POS data from the Data Vault and returns a DataFrame

    The output DataFrame is suitable for use in the pos_to_training_data function.

    Note that when reading from the data vault, we use a pre-defined view that will
    sort out the restatement records for us.  This means we don't have to write our
    own logic for handling duplicate rows within the source dataset.

    :param string retailer: the name of the retailer to pull
    :param string client: the name of the client to pull
    :param string country_code: the two character name of the country code to pull
    :param pyspark.sql.context.HiveContext sql_context: PySparkSQL context that can be used to connect to the Data Vault

    :return DataFrame:
    """

    retailer = retailer.strip().upper()
    client = client.strip().upper()
    country_code = country_code.strip().upper()

    database = '{retailer}_{client}_{country_code}_dv'.format(retailer=retailer.strip().lower(),
                                                              client=client.strip().lower(),
                                                              country_code=country_code.strip().lower())

    sql_statement = """
        SELECT
              \'{retailer}\' AS RETAILER,
              \'{client}\' AS CLIENT,
              \'{country_code}\' AS COUNTRY_CODE,
              HRI.RETAILER_ITEM_ID,
              HOU.ORGANIZATION_UNIT_NUM,
              VSLES.SALES_DT,
              VSLES.POS_ITEM_QTY,
              VSLES.POS_AMT,
              VSLES.ON_HAND_INVENTORY_QTY,
              VSLES.POS_AMT / VSLES.POS_ITEM_QTY AS UNIT_PRICE,
              'DATA VAULT' AS ROW_ORIGIN,
              'COMPUTED' AS PRICE_ORIGIN,
              'IGNORE' AS TRAINING_ROLE
           FROM {database}.vw_sat_link_epos_summary VSLES
           LEFT OUTER JOIN {database}.hub_retailer_item HRI 
                ON HRI.HUB_RETAILER_ITEM_HK = VSLES.HUB_RETAILER_ITEM_HK
           LEFT OUTER JOIN {database}.hub_organization_unit HOU 
                ON HOU.HUB_ORGANIZATION_UNIT_HK = VSLES.HUB_ORGANIZATION_UNIT_HK
        """

    sql_statement = sql_statement.format(database=database, retailer=retailer, client=client, country_code=country_code)

    return sql_context.sql(sql_statement)

# COMMAND ----------

# Read POS data
raw_data_vault_data = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')

# COMMAND ----------

display(raw_data_vault_data)

# COMMAND ----------

STORE = 2797
ITEM = 52162304

# COMMAND ----------

data_vault_data = raw_data_vault_data

if STORE:
    data_vault_data = data_vault_data.filter('ORGANIZATION_UNIT_NUM == "{}"'.format(STORE))
if ITEM:
    data_vault_data = data_vault_data.filter('RETAILER_ITEM_ID == "{}"'.format(ITEM))

# COMMAND ----------

display(data_vault_data.orderBy('SALES_DT', ascending=False))

# COMMAND ----------

STORE = 3031
ITEM = 73445693

# COMMAND ----------

data_vault_data = raw_data_vault_data

if STORE:
    data_vault_data = data_vault_data.filter('ORGANIZATION_UNIT_NUM == "{}"'.format(STORE))
if ITEM:
    data_vault_data = data_vault_data.filter('RETAILER_ITEM_ID == "{}"'.format(ITEM))

# COMMAND ----------

display(data_vault_data.orderBy('SALES_DT', ascending=False))

# COMMAND ----------

STORE = 5851
ITEM = 77300979

# COMMAND ----------

data_vault_data = raw_data_vault_data

if STORE:
    data_vault_data = data_vault_data.filter('ORGANIZATION_UNIT_NUM == "{}"'.format(STORE))
if ITEM:
    data_vault_data = data_vault_data.filter('RETAILER_ITEM_ID == "{}"'.format(ITEM))

# COMMAND ----------

display(data_vault_data.orderBy('SALES_DT', ascending=False))

# COMMAND ----------

tesco_nestlecore_nescafe_sql_statement = """
    select fc.sales_dt, fc.BASELINE_POS_ITEM_QTY, sales.pos_item_qty, sales.pos_amt, sales.on_hand_inventory_qty, hou.organization_unit_num, hri.RETAILER_ITEM_ID, sri.RETAILER_ITEM_DESC
    FROM tesco_nestlecore_uk_retail_alert_im.vw_drfe_forecast_baseline_unit as fc
    INNER JOIN tesco_nestlecore_uk_dv.link_epos_summary as les
      ON fc.hub_organization_unit_hk = les.hub_organization_unit_hk AND fc.hub_retailer_item_hk = les.hub_retailer_item_hk AND fc.sales_dt = les.sales_dt
    INNER JOIN tesco_nestlecore_uk_dv.sat_link_epos_summary as sales
      ON les.LINK_EPOS_SUMMARY_HK = sales.LINK_EPOS_SUMMARY_HK
    INNER JOIN tesco_nestlecore_uk_dv.hub_organization_unit as hou
      ON hou.hub_organization_unit_hk = fc.hub_organization_unit_hk
    INNER JOIN tesco_nestlecore_uk_dv.hub_retailer_item as hri
      ON fc.hub_retailer_item_hk = hri.hub_retailer_item_hk
    INNER JOIN tesco_nestlecore_uk_dv.sat_retailer_item as sri
      ON fc.hub_retailer_item_hk = sri.hub_retailer_item_hk

    where fc.SALES_DT >= '2020-01-01' and fc.SALES_DT <= '2020-02-29'
      and sri.RETAILER_ITEM_DESC like 'NESCAFE%'
"""

df_tesco_nestlecore_nescafe = spark.sql(tesco_nestlecore_nescafe_sql_statement)

# COMMAND ----------

# Save the data set
df_tesco_nestlecore_nescafe.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/tesco-nestlecore-nescafe-20200101-to-20200229')

# COMMAND ----------

df_tesco_nestlecore_nescafe_agg = df_tesco_nestlecore_nescafe.groupBy('sales_dt', 'RETAILER_ITEM_ID', 'RETAILER_ITEM_DESC').agg(pyf.sum("BASELINE_POS_ITEM_QTY").alias('TOTAL_BASELINE_POS_ITEM_QTY'),                                            pyf.sum("POS_ITEM_QTY").alias('TOTAL_POS_ITEM_QTY'),
 pyf.sum("POS_AMT").alias('TOTAL_POS_AMT'),                                                                                            pyf.sum("ON_HAND_INVENTORY_QTY").alias('TOTAL_ON_HAND_INVENTORY_QTY')).orderBy(["RETAILER_ITEM_ID", "SALES_DT"], ascending=True)

# COMMAND ----------

# Save the data set
df_tesco_nestlecore_nescafe_agg.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/tesco-nestlecore-nescafe-20200101-to-20200229-agg')

# COMMAND ----------

tesco_nestlecore_selected_brands_sql_statement = """
    select fc.sales_dt, fc.BASELINE_POS_ITEM_QTY, sales.pos_item_qty, sales.pos_amt, sales.on_hand_inventory_qty, hou.organization_unit_num, hri.RETAILER_ITEM_ID, sri.RETAILER_ITEM_DESC
    FROM tesco_nestlecore_uk_retail_alert_im.vw_drfe_forecast_baseline_unit as fc
    INNER JOIN tesco_nestlecore_uk_dv.link_epos_summary as les
      ON fc.hub_organization_unit_hk = les.hub_organization_unit_hk AND fc.hub_retailer_item_hk = les.hub_retailer_item_hk AND fc.sales_dt = les.sales_dt
    INNER JOIN tesco_nestlecore_uk_dv.sat_link_epos_summary as sales
      ON les.LINK_EPOS_SUMMARY_HK = sales.LINK_EPOS_SUMMARY_HK
    INNER JOIN tesco_nestlecore_uk_dv.hub_organization_unit as hou
      ON hou.hub_organization_unit_hk = fc.hub_organization_unit_hk
    INNER JOIN tesco_nestlecore_uk_dv.hub_retailer_item as hri
      ON fc.hub_retailer_item_hk = hri.hub_retailer_item_hk
    INNER JOIN tesco_nestlecore_uk_dv.sat_retailer_item as sri
      ON fc.hub_retailer_item_hk = sri.hub_retailer_item_hk

    where fc.SALES_DT >= '2020-01-01' and fc.SALES_DT <= '2020-02-29'
      and (sri.RETAILER_ITEM_DESC like '%PURIN%' 
        or sri.RETAILER_ITEM_DESC like '%NIDO%'
        or sri.RETAILER_ITEM_DESC like 'AERO%'
        or sri.RETAILER_ITEM_DESC like 'SAN PELLEGRINO%'
        or sri.RETAILER_ITEM_DESC like 'CARNATION%'
        or sri.RETAILER_ITEM_DESC like '%CHEERIO%')
"""

df_tesco_nestlecore_selected_brands = spark.sql(tesco_nestlecore_selected_brands_sql_statement)

# COMMAND ----------

# Save the data set
df_tesco_nestlecore_selected_brands.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/tesco-nestlecore-selected-brands-20200101-to-20200229')

# COMMAND ----------

df_tesco_nestlecore_selected_brands_agg = df_tesco_nestlecore_selected_brands.groupBy('sales_dt', 'RETAILER_ITEM_ID', 'RETAILER_ITEM_DESC').agg(pyf.sum("BASELINE_POS_ITEM_QTY").alias('TOTAL_BASELINE_POS_ITEM_QTY'),                                            pyf.sum("POS_ITEM_QTY").alias('TOTAL_POS_ITEM_QTY'),
 pyf.sum("POS_AMT").alias('TOTAL_POS_AMT'),                                                                                            pyf.sum("ON_HAND_INVENTORY_QTY").alias('TOTAL_ON_HAND_INVENTORY_QTY')).orderBy(["RETAILER_ITEM_ID", "SALES_DT"], ascending=True)

# COMMAND ----------

# Save the data set
df_tesco_nestlecore_selected_brands_agg.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/tesco-nestlecore-selected-brands-20200101-to-20200229-agg')

# COMMAND ----------

asda_nestlecore_nescafe_sql_statement = """
    select fc.sales_dt, fc.BASELINE_POS_ITEM_QTY, sales.pos_item_qty, sales.pos_amt, sales.on_hand_inventory_qty, hou.organization_unit_num, hri.RETAILER_ITEM_ID, sri.RETAILER_ITEM_DESC
    FROM asda_nestlecore_uk_retail_alert_im.vw_drfe_forecast_baseline_unit as fc
    INNER JOIN asda_nestlecore_uk_dv.link_epos_summary as les
      ON fc.hub_organization_unit_hk = les.hub_organization_unit_hk AND fc.hub_retailer_item_hk = les.hub_retailer_item_hk AND fc.sales_dt = les.sales_dt
    INNER JOIN asda_nestlecore_uk_dv.sat_link_epos_summary as sales
      ON les.LINK_EPOS_SUMMARY_HK = sales.LINK_EPOS_SUMMARY_HK
    INNER JOIN asda_nestlecore_uk_dv.hub_organization_unit as hou
      ON hou.hub_organization_unit_hk = fc.hub_organization_unit_hk
    INNER JOIN asda_nestlecore_uk_dv.hub_retailer_item as hri
      ON fc.hub_retailer_item_hk = hri.hub_retailer_item_hk
    INNER JOIN asda_nestlecore_uk_dv.sat_retailer_item as sri
      ON fc.hub_retailer_item_hk = sri.hub_retailer_item_hk

    where fc.SALES_DT >= '2020-01-01' and fc.SALES_DT <= '2020-02-29'
      and sri.RETAILER_ITEM_DESC like 'NESCAFE%'
"""

df_asda_nestlecore_nescafe = spark.sql(asda_nestlecore_nescafe_sql_statement)

# COMMAND ----------

# Save the data set
df_asda_nestlecore_nescafe.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/asda-nestlecore-nescafe-20200101-to-20200229')

# COMMAND ----------

df_asda_nestlecore_nescafe_agg = df_asda_nestlecore_nescafe.groupBy('sales_dt', 'RETAILER_ITEM_ID', 'RETAILER_ITEM_DESC').agg(pyf.sum("BASELINE_POS_ITEM_QTY").alias('TOTAL_BASELINE_POS_ITEM_QTY'),                                            pyf.sum("POS_ITEM_QTY").alias('TOTAL_POS_ITEM_QTY'),
 pyf.sum("POS_AMT").alias('TOTAL_POS_AMT'),                                                                                            pyf.sum("ON_HAND_INVENTORY_QTY").alias('TOTAL_ON_HAND_INVENTORY_QTY')).orderBy(["RETAILER_ITEM_ID", "SALES_DT"], ascending=True)

# COMMAND ----------

# Save the data set
df_asda_nestlecore_nescafe_agg.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/asda-nestlecore-nescafe-20200101-to-20200229-agg')

# COMMAND ----------

asda_nestlecore_selected_brands_sql_statement = """
    select fc.sales_dt, fc.BASELINE_POS_ITEM_QTY, sales.pos_item_qty, sales.pos_amt, sales.on_hand_inventory_qty, hou.organization_unit_num, hri.RETAILER_ITEM_ID, sri.RETAILER_ITEM_DESC
    FROM asda_nestlecore_uk_retail_alert_im.vw_drfe_forecast_baseline_unit as fc
    INNER JOIN asda_nestlecore_uk_dv.link_epos_summary as les
      ON fc.hub_organization_unit_hk = les.hub_organization_unit_hk AND fc.hub_retailer_item_hk = les.hub_retailer_item_hk AND fc.sales_dt = les.sales_dt
    INNER JOIN asda_nestlecore_uk_dv.sat_link_epos_summary as sales
      ON les.LINK_EPOS_SUMMARY_HK = sales.LINK_EPOS_SUMMARY_HK
    INNER JOIN asda_nestlecore_uk_dv.hub_organization_unit as hou
      ON hou.hub_organization_unit_hk = fc.hub_organization_unit_hk
    INNER JOIN asda_nestlecore_uk_dv.hub_retailer_item as hri
      ON fc.hub_retailer_item_hk = hri.hub_retailer_item_hk
    INNER JOIN asda_nestlecore_uk_dv.sat_retailer_item as sri
      ON fc.hub_retailer_item_hk = sri.hub_retailer_item_hk

    where fc.SALES_DT >= '2020-01-01' and fc.SALES_DT <= '2020-02-29'
      and (sri.RETAILER_ITEM_DESC like '%PURIN%' 
        or sri.RETAILER_ITEM_DESC like '%NIDO%'
        or sri.RETAILER_ITEM_DESC like 'AERO%'
        or sri.RETAILER_ITEM_DESC like 'SAN PELLEGRINO%'
        or sri.RETAILER_ITEM_DESC like 'CARNATION%'
        or sri.RETAILER_ITEM_DESC like '%CHEERIO%')
"""

df_asda_nestlecore_selected_brands = spark.sql(asda_nestlecore_selected_brands_sql_statement)

# COMMAND ----------

# Save the data set
df_asda_nestlecore_selected_brands.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/asda-nestlecore-selected-brands-20200101-to-20200229')

# COMMAND ----------

df_asda_nestlecore_selected_brands_agg = df_asda_nestlecore_selected_brands.groupBy('sales_dt', 'RETAILER_ITEM_ID', 'RETAILER_ITEM_DESC').agg(pyf.sum("BASELINE_POS_ITEM_QTY").alias('TOTAL_BASELINE_POS_ITEM_QTY'),                                            pyf.sum("POS_ITEM_QTY").alias('TOTAL_POS_ITEM_QTY'),
 pyf.sum("POS_AMT").alias('TOTAL_POS_AMT'),                                                                                            pyf.sum("ON_HAND_INVENTORY_QTY").alias('TOTAL_ON_HAND_INVENTORY_QTY')).orderBy(["RETAILER_ITEM_ID", "SALES_DT"], ascending=True)

# COMMAND ----------

# Save the data set
df_asda_nestlecore_selected_brands_agg.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/asda-nestlecore-selected-brands-20200101-to-20200229-agg')

# COMMAND ----------

morrisons_nestlecore_nescafe_sql_statement = """
    select fc.sales_dt, fc.BASELINE_POS_ITEM_QTY, sales.pos_item_qty, sales.pos_amt, sales.on_hand_inventory_qty, hou.organization_unit_num, hri.RETAILER_ITEM_ID, sri.RETAILER_ITEM_DESC
    FROM morrisons_nestlecore_uk_retail_alert_im.vw_drfe_forecast_baseline_unit as fc
    INNER JOIN morrisons_nestlecore_uk_dv.link_epos_summary as les
      ON fc.hub_organization_unit_hk = les.hub_organization_unit_hk AND fc.hub_retailer_item_hk = les.hub_retailer_item_hk AND fc.sales_dt = les.sales_dt
    INNER JOIN morrisons_nestlecore_uk_dv.sat_link_epos_summary as sales
      ON les.LINK_EPOS_SUMMARY_HK = sales.LINK_EPOS_SUMMARY_HK
    INNER JOIN morrisons_nestlecore_uk_dv.hub_organization_unit as hou
      ON hou.hub_organization_unit_hk = fc.hub_organization_unit_hk
    INNER JOIN morrisons_nestlecore_uk_dv.hub_retailer_item as hri
      ON fc.hub_retailer_item_hk = hri.hub_retailer_item_hk
    INNER JOIN morrisons_nestlecore_uk_dv.sat_retailer_item as sri
      ON fc.hub_retailer_item_hk = sri.hub_retailer_item_hk

    where fc.SALES_DT >= '2020-01-01' and fc.SALES_DT <= '2020-02-29'
      and sri.RETAILER_ITEM_DESC like 'NESCAFE%'
"""

df_morrisons_nestlecore_nescafe = spark.sql(morrisons_nestlecore_nescafe_sql_statement)

# COMMAND ----------

# Save the data set
df_morrisons_nestlecore_nescafe.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/morrisons-nestlecore-nescafe-20200101-to-20200229')

# COMMAND ----------

morrisons_nestlecore_selected_brands_sql_statement = """
    select fc.sales_dt, fc.BASELINE_POS_ITEM_QTY, sales.pos_item_qty, sales.pos_amt, sales.on_hand_inventory_qty, hou.organization_unit_num, hri.RETAILER_ITEM_ID, sri.RETAILER_ITEM_DESC
    FROM morrisons_nestlecore_uk_retail_alert_im.vw_drfe_forecast_baseline_unit as fc
    INNER JOIN morrisons_nestlecore_uk_dv.link_epos_summary as les
      ON fc.hub_organization_unit_hk = les.hub_organization_unit_hk AND fc.hub_retailer_item_hk = les.hub_retailer_item_hk AND fc.sales_dt = les.sales_dt
    INNER JOIN morrisons_nestlecore_uk_dv.sat_link_epos_summary as sales
      ON les.LINK_EPOS_SUMMARY_HK = sales.LINK_EPOS_SUMMARY_HK
    INNER JOIN morrisons_nestlecore_uk_dv.hub_organization_unit as hou
      ON hou.hub_organization_unit_hk = fc.hub_organization_unit_hk
    INNER JOIN morrisons_nestlecore_uk_dv.hub_retailer_item as hri
      ON fc.hub_retailer_item_hk = hri.hub_retailer_item_hk
    INNER JOIN morrisons_nestlecore_uk_dv.sat_retailer_item as sri
      ON fc.hub_retailer_item_hk = sri.hub_retailer_item_hk

    where fc.SALES_DT >= '2020-01-01' and fc.SALES_DT <= '2020-02-29'
      and (sri.RETAILER_ITEM_DESC like '%PURIN%' 
        or sri.RETAILER_ITEM_DESC like '%NIDO%'
        or sri.RETAILER_ITEM_DESC like 'AERO%'
        or sri.RETAILER_ITEM_DESC like 'SAN PELLEGRINO%'
        or sri.RETAILER_ITEM_DESC like 'CARNATION%'
        or sri.RETAILER_ITEM_DESC like '%CHEERIO%')
"""

df_morrisons_nestlecore_selected_brands = spark.sql(morrisons_nestlecore_selected_brands_sql_statement)

# COMMAND ----------

# Save the data set
df_morrisons_nestlecore_selected_brands.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/morrisons-nestlecore-selected-brands-20200101-to-20200229')

# COMMAND ----------


