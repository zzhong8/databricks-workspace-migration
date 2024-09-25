# Databricks notebook source
import uuid
import warnings
import numpy as np
import pandas as pd
from datetime import datetime
from pyspark.sql import Window
# from pyspark.sql.functions import *
from pyspark.sql import types as pyt
from pyspark.sql import functions as pyf
import acosta
from acosta.alerting.preprocessing import read_pos_data

print(acosta.__version__)

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
              SRI.RETAILER_ITEM_DESC, 
              HOU.ORGANIZATION_UNIT_NUM,
              SOU.ORGANIZATION_UNIT_NM,
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
           LEFT OUTER JOIN {database}.sat_retailer_item SRI   
                ON SRI.HUB_RETAILER_ITEM_HK = VSLES.HUB_RETAILER_ITEM_HK
           LEFT OUTER JOIN {database}.hub_organization_unit HOU 
                ON HOU.HUB_ORGANIZATION_UNIT_HK = VSLES.HUB_ORGANIZATION_UNIT_HK
           LEFT OUTER JOIN {database}.sat_organization_unit SOU 
                ON SOU.HUB_ORGANIZATION_UNIT_HK = VSLES.HUB_ORGANIZATION_UNIT_HK
        """

    sql_statement = sql_statement.format(database=database, retailer=retailer, client=client, country_code=country_code)

    return sql_context.sql(sql_statement)


# COMMAND ----------

RETAILER = 'walmart'
CLIENT = 'campbells'
COUNTRY_CODE = 'ca'

# COMMAND ----------

# Read POS data
data_vault_data_campbells = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data_campbells = data_vault_data_campbells.where((pyf.col("SALES_DT") < pyf.lit("2020-05-01")) & (pyf.col("SALES_DT") >= pyf.lit("2019-01-01")))

# COMMAND ----------

display(data_vault_data_campbells)

# COMMAND ----------

# Save the data set
data_vault_data_campbells.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/walmart-campbells-20190101-to-20200430')

# COMMAND ----------

# data_vault_data_campbells_one_month = data_vault_data_campbells.where((pyf.col("SALES_DT") < pyf.lit("2020-01-01")) & (pyf.col("SALES_DT") >= pyf.lit("2019-12-01")))

# COMMAND ----------

# display(data_vault_data_campbells_one_month)

# COMMAND ----------

# choice_list = [9278748,
# 9278762,
# 9275647,
# 9278293,
# 9278720,
# 9278776,
# 9278286,
# 30987314,
# 9275633,
# 9275815]

# data_vault_data_campbells_sample = data_vault_data_campbells.where((pyf.col("RETAILER_ITEM_ID").isin(choice_list)))

# COMMAND ----------

# display(data_vault_data_campbells_sample)

# COMMAND ----------

CLIENT = 'catelli'
COUNTRY_CODE = 'ca'

# COMMAND ----------

# Read POS data
data_vault_data_catelli = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data_catelli = data_vault_data_catelli.where((pyf.col("SALES_DT") < pyf.lit("2020-05-01")) & (pyf.col("SALES_DT") >= pyf.lit("2019-01-01")))

# COMMAND ----------

display(data_vault_data_catelli)

# COMMAND ----------

# Save the data set
data_vault_data_catelli.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/walmart-catelli-20190101-to-20200430')

# COMMAND ----------

CLIENT = 'lego'
COUNTRY_CODE = 'ca'

# COMMAND ----------

# Read POS data
data_vault_data_lego = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data_lego = data_vault_data_lego.where((pyf.col("SALES_DT") < pyf.lit("2020-05-01")) & (pyf.col("SALES_DT") >= pyf.lit("2019-01-01")))

# COMMAND ----------

display(data_vault_data_lego)

# COMMAND ----------

# Save the data set
data_vault_data_lego.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/walmart-lego-20190101-to-20200430')

# COMMAND ----------

CLIENT = 'smuckers'
COUNTRY_CODE = 'ca'

# COMMAND ----------

# Read POS data
data_vault_data_smuckers = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data_smuckers = data_vault_data_smuckers.where((pyf.col("SALES_DT") < pyf.lit("2020-05-01")) & (pyf.col("SALES_DT") >= pyf.lit("2019-01-01")))

# COMMAND ----------

display(data_vault_data_smuckers)

# COMMAND ----------

# Save the data set
data_vault_data_smuckers.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/walmart-smuckers-20190101-to-20200430')

# COMMAND ----------

RETAILER = 'walmart'
CLIENT = 'campbellssnack'
COUNTRY_CODE = 'us'

# COMMAND ----------

# Read POS data
data_vault_data_campbellssnack = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data_campbellssnack = data_vault_data_campbellssnack.where((pyf.col("SALES_DT") < pyf.lit("2020-05-01")) & (pyf.col("SALES_DT") >= pyf.lit("2019-01-01")))

# COMMAND ----------

display(data_vault_data_campbellssnack)

# COMMAND ----------

# Save the data set
data_vault_data_campbellssnack.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/walmart-campbellssnack-20190101-to-20200430')

# COMMAND ----------

CLIENT = 'rbusahealth'
COUNTRY_CODE = 'us'

# COMMAND ----------

# Read POS data
data_vault_data_rbusahealth = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data_rbusahealth = data_vault_data_rbusahealth.where((pyf.col("SALES_DT") < pyf.lit("2020-05-01")) & (pyf.col("SALES_DT") >= pyf.lit("2019-01-01")))

# COMMAND ----------

display(data_vault_data_rbusahealth)

# COMMAND ----------

# Save the data set
data_vault_data_rbusahealth.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/walmart-rbusahealth-20190101-to-20200430')

# COMMAND ----------

CLIENT = 'rbusahygiene'
COUNTRY_CODE = 'us'

# COMMAND ----------

# Read POS data
data_vault_data_rbusahygiene = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data_rbusahygiene = data_vault_data_rbusahygiene.where((pyf.col("SALES_DT") < pyf.lit("2020-05-01")) & (pyf.col("SALES_DT") >= pyf.lit("2019-01-01")))

# COMMAND ----------

display(data_vault_data_rbusahygiene)

# COMMAND ----------

# Save the data set
data_vault_data_rbusahygiene.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/walmart-rbusahygiene-20190101-to-20200430')

# COMMAND ----------

CLIENT = 'tysonhillshire'
COUNTRY_CODE = 'us'

# COMMAND ----------

# Read POS data
data_vault_data_tysonhillshire = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data_tysonhillshire = data_vault_data_tysonhillshire.where((pyf.col("SALES_DT") < pyf.lit("2020-05-01")) & (pyf.col("SALES_DT") >= pyf.lit("2019-01-01")))

# COMMAND ----------

display(data_vault_data_tysonhillshire)

# COMMAND ----------

# Save the data set
data_vault_data_tysonhillshire.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/walmart-tysonhillshire-20190101-to-20200430')

# COMMAND ----------

CLIENT = 'nestlewaters'
COUNTRY_CODE = 'us'

# COMMAND ----------

# Read POS data
data_vault_data_nestlewaters = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data_nestlewaters = data_vault_data_nestlewaters.where((pyf.col("SALES_DT") < pyf.lit("2020-05-01")) & (pyf.col("SALES_DT") >= pyf.lit("2019-01-01")))

# COMMAND ----------

display(data_vault_data_nestlewaters)

# COMMAND ----------

# Save the data set
data_vault_data_nestlewaters.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/walmart-nestlewaters-20190101-to-20200430')

# COMMAND ----------

# Read POS data
data_vault_data2 = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data2 = data_vault_data2.where((pyf.col("SALES_DT") < pyf.lit("2020-01-01")) & (pyf.col("SALES_DT") >= pyf.lit("2019-07-01")))

total_sales_by_date2 = data_vault_data2.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()
display(total_sales_by_date2)

# COMMAND ----------

total_inventory_by_date2 = data_vault_data2.select("SALES_DT", "ON_HAND_INVENTORY_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_inventory_by_date2)

# COMMAND ----------

df2b = spark.sql("""
select fc.sales_dt, fc.BASELINE_POS_ITEM_QTY, sales.pos_item_qty, sales.pos_amt, sales.on_hand_inventory_qty, hou.organization_unit_num, hri.RETAILER_ITEM_ID, sri.RETAILER_ITEM_DESC
FROM walmart_campbells_ca_retail_alert_im.vw_drfe_forecast_baseline_unit as fc
INNER JOIN walmart_campbells_ca_dv.link_epos_summary as les
  ON fc.hub_organization_unit_hk = les.hub_organization_unit_hk AND fc.hub_retailer_item_hk = les.hub_retailer_item_hk AND fc.sales_dt = les.sales_dt
INNER JOIN walmart_campbells_ca_dv.vw_sat_link_epos_summary as sales
  ON les.LINK_EPOS_SUMMARY_HK = sales.LINK_EPOS_SUMMARY_HK
INNER JOIN walmart_campbells_ca_dv.hub_organization_unit as hou
  ON hou.hub_organization_unit_hk = fc.hub_organization_unit_hk
INNER JOIN walmart_campbells_ca_dv.hub_retailer_item as hri
  ON fc.hub_retailer_item_hk = hri.hub_retailer_item_hk
INNER JOIN walmart_campbells_ca_dv.sat_retailer_item as sri
  ON fc.hub_retailer_item_hk = sri.hub_retailer_item_hk

where fc.SALES_DT >= '2020-03-01'""")

# COMMAND ----------

totals_by_date2b = df2b.select("SALES_DT", "BASELINE_POS_ITEM_QTY", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()
display(totals_by_date2b)

# COMMAND ----------

inventory_by_date2b = df2b.select("SALES_DT", "on_hand_inventory_qty").orderBy("SALES_DT").groupBy("SALES_DT").sum()
display(inventory_by_date2b)

# COMMAND ----------

CLIENT = 'catelli'
COUNTRY_CODE = 'ca'

# COMMAND ----------

# Read POS data
data_vault_data4 = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data4 = data_vault_data4.where((pyf.col("SALES_DT") < pyf.lit("2020-05-01")) & (pyf.col("SALES_DT") >= pyf.lit("2020-03-01")))

total_sales_by_date4 = data_vault_data4.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()
display(total_sales_by_date4)

# COMMAND ----------

total_inventory_by_date4 = data_vault_data4.select("SALES_DT", "ON_HAND_INVENTORY_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_inventory_by_date4)

# COMMAND ----------

CLIENT = 'lego'
COUNTRY_CODE = 'ca'

# COMMAND ----------

# Read POS data
data_vault_data5 = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data5 = data_vault_data5.where((pyf.col("SALES_DT") < pyf.lit("2020-05-01")) & (pyf.col("SALES_DT") >= pyf.lit("2020-03-01")))

total_sales_by_date5 = data_vault_data5.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()
display(total_sales_by_date5)

# COMMAND ----------

CLIENT = 'smuckers'
COUNTRY_CODE = 'ca'

# COMMAND ----------

# Read POS data
data_vault_data_smuckers = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data_smuckers = data_vault_data_smuckers.where((pyf.col("SALES_DT") < pyf.lit("2020-05-01")) & (pyf.col("SALES_DT") >= pyf.lit("2020-03-01")))

total_sales_by_smuckers = data_vault_data_smuckers.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()
display(total_sales_by_smuckers)

# COMMAND ----------

total_inventory_by_date_smuckers = data_vault_data_smuckers.select("SALES_DT", "ON_HAND_INVENTORY_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_inventory_by_date_smuckers)

# COMMAND ----------

CLIENT = 'voortman'
COUNTRY_CODE = 'ca'

# COMMAND ----------

# Read POS data
data_vault_data_voortman = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data_voortman = data_vault_data_voortman.where((pyf.col("SALES_DT") <= pyf.lit("2020-07-01")) & (pyf.col("SALES_DT") >= pyf.lit("2019-01-01")))

total_sales_by_date_voortman = data_vault_data_voortman.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()
display(total_sales_by_date_voortman)

# COMMAND ----------

total_inventory_by_date_voortman = data_vault_data_voortman.select("SALES_DT", "ON_HAND_INVENTORY_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_inventory_by_date_voortman)

# COMMAND ----------

data_vault_data_voortman_covid = data_vault_data_voortman.where((pyf.col("SALES_DT") <= pyf.lit("2021-07-01")) & (pyf.col("SALES_DT") >= pyf.lit("2020-03-01")))

total_sales_by_date_voortman_covid = data_vault_data_voortman_covid.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()
display(total_sales_by_date_voortman_covid)

# COMMAND ----------

total_inventory_by_date_voortman_covid = data_vault_data_voortman_covid.select("SALES_DT", "ON_HAND_INVENTORY_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_inventory_by_date_voortman_covid)

# COMMAND ----------


