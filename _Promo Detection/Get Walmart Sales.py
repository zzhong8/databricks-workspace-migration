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

RETAILER = 'walmart'
CLIENT = 'campbells'
COUNTRY_CODE = 'ca'

# COMMAND ----------

# Read POS data
data_vault_data2 = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data2 = data_vault_data2.where((pyf.col("SALES_DT") < pyf.lit("2020-05-01")) & (pyf.col("SALES_DT") >= pyf.lit("2019-01-01")))


# COMMAND ----------

data_vault_data2.where(pyf.col("organization_unit_num").isin({2026, 2004, 2024, 2025, 2019, 2003, 2021, 2022, 1152, 2017, 2001, 2023, 2061, 2014, 2015, 2020, 2002, 2016, 2018, 414, 5636})).count()

# COMMAND ----------

organization_unit_nums2 = data_vault_data2.where(pyf.col("organization_unit_num").isin({2026, 2004, 2024, 2025, 2019, 2003, 2021, 2022, 1152, 2017, 2001, 2023, 2061, 2014, 2015, 2020, 2002, 2016, 2018, 414, 5636})).select("organization_unit_num").distinct()
                                                
display(organization_unit_nums2)                     

# COMMAND ----------

organization_unit_nums.count()

# COMMAND ----------

data_vault_data2 = data_vault_data2.where(pyf.col("organization_unit_num").isin({2026, 2004, 2024, 2025, 2019, 2003, 2021, 2022, 1152, 2017, 2001, 2023, 2061, 2014, 2015, 2020, 2002, 2016, 2018,  414, 5636}))

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
data_vault_data4 = data_vault_data4.where((pyf.col("SALES_DT") < pyf.lit("2020-05-01")) & (pyf.col("SALES_DT") >= pyf.lit("2019-01-01")))

# COMMAND ----------

organization_unit_nums4 = data_vault_data4.where(pyf.col("organization_unit_num").isin({2026, 2004, 2024, 2025, 2019, 2003, 2021, 2022, 1152, 2017, 2001, 2023, 2061, 2014, 2015, 2020, 2002, 2016, 2018, 414, 5636})).select("organization_unit_num").distinct()
                                                
organization_unit_nums4.count() 

# COMMAND ----------

display(organization_unit_nums4)    

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

CLIENT = 'rbusahealth'
COUNTRY_CODE = 'us'

# COMMAND ----------

# Read POS data
data_vault_data6 = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data6 = data_vault_data6.where((pyf.col("SALES_DT") < pyf.lit("2020-05-01")) & (pyf.col("SALES_DT") >= pyf.lit("2020-02-16")))

total_sales_by_date6 = data_vault_data6.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()
display(total_sales_by_date6)

# COMMAND ----------

CLIENT = 'rbusahygiene'
COUNTRY_CODE = 'us'

# COMMAND ----------

# Read POS data
data_vault_data7 = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data7 = data_vault_data7.where((pyf.col("SALES_DT") < pyf.lit("2020-05-01")) & (pyf.col("SALES_DT") >= pyf.lit("2020-02-16")))

total_sales_by_date7 = data_vault_data7.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()
display(total_sales_by_date7)

# COMMAND ----------

df7 = spark.sql("""
select fc.sales_dt, fc.BASELINE_POS_ITEM_QTY, sales.pos_item_qty, sales.pos_amt, sales.on_hand_inventory_qty, hou.organization_unit_num, hri.RETAILER_ITEM_ID, sri.RETAILER_ITEM_DESC
FROM walmart_rbusahygiene_us_retail_alert_im.vw_drfe_forecast_baseline_unit as fc
INNER JOIN walmart_rbusahygiene_us_dv.link_epos_summary as les
  ON fc.hub_organization_unit_hk = les.hub_organization_unit_hk AND fc.hub_retailer_item_hk = les.hub_retailer_item_hk AND fc.sales_dt = les.sales_dt
INNER JOIN walmart_rbusahygiene_us_dv.sat_link_epos_summary as sales
  ON les.LINK_EPOS_SUMMARY_HK = sales.LINK_EPOS_SUMMARY_HK
INNER JOIN walmart_rbusahygiene_us_dv.hub_organization_unit as hou
  ON hou.hub_organization_unit_hk = fc.hub_organization_unit_hk
INNER JOIN walmart_rbusahygiene_us_dv.hub_retailer_item as hri
  ON fc.hub_retailer_item_hk = hri.hub_retailer_item_hk
INNER JOIN walmart_rbusahygiene_us_dv.sat_retailer_item as sri
  ON fc.hub_retailer_item_hk = sri.hub_retailer_item_hk

where fc.SALES_DT >= '2020-02-16'""")

# COMMAND ----------

totals_by_date7 = df7.select("SALES_DT", "BASELINE_POS_ITEM_QTY", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()
display(totals_by_date7)

# COMMAND ----------

df7b = spark.sql("""
select fc.sales_dt, fc.BASELINE_POS_ITEM_QTY, sales.pos_item_qty, sales.pos_amt, sales.on_hand_inventory_qty, hou.organization_unit_num, hri.RETAILER_ITEM_ID, sri.RETAILER_ITEM_DESC
FROM walmart_rbusahygiene_us_retail_alert_im.vw_drfe_forecast_baseline_unit as fc
INNER JOIN walmart_rbusahygiene_us_dv.link_epos_summary as les
  ON fc.hub_organization_unit_hk = les.hub_organization_unit_hk AND fc.hub_retailer_item_hk = les.hub_retailer_item_hk AND fc.sales_dt = les.sales_dt
INNER JOIN walmart_rbusahygiene_us_dv.vw_sat_link_epos_summary as sales
  ON les.LINK_EPOS_SUMMARY_HK = sales.LINK_EPOS_SUMMARY_HK
INNER JOIN walmart_rbusahygiene_us_dv.hub_organization_unit as hou
  ON hou.hub_organization_unit_hk = fc.hub_organization_unit_hk
INNER JOIN walmart_rbusahygiene_us_dv.hub_retailer_item as hri
  ON fc.hub_retailer_item_hk = hri.hub_retailer_item_hk
INNER JOIN walmart_rbusahygiene_us_dv.sat_retailer_item as sri
  ON fc.hub_retailer_item_hk = sri.hub_retailer_item_hk

where fc.SALES_DT >= '2020-02-16'""")

# COMMAND ----------

totals_by_date7b = df7b.select("SALES_DT", "BASELINE_POS_ITEM_QTY", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()
display(totals_by_date7b)

# COMMAND ----------

inventory_by_date7b = df7b.select("SALES_DT", "on_hand_inventory_qty").orderBy("SALES_DT").groupBy("SALES_DT").sum()
display(inventory_by_date7b)

# COMMAND ----------

CLIENT = 'smuckers'
COUNTRY_CODE = 'ca'

# COMMAND ----------

# Read POS data
data_vault_data8 = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data8 = data_vault_data8.where((pyf.col("SALES_DT") < pyf.lit("2020-05-01")) & (pyf.col("SALES_DT") >= pyf.lit("2020-03-01")))

total_sales_by_date8 = data_vault_data8.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()
display(total_sales_by_date8)

# COMMAND ----------

total_inventory_by_date8 = data_vault_data8.select("SALES_DT", "ON_HAND_INVENTORY_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_inventory_by_date8)

# COMMAND ----------

CLIENT = 'tysonhillshire'
COUNTRY_CODE = 'us'

# COMMAND ----------

# Read POS data
data_vault_data9 = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data9 = data_vault_data9.where((pyf.col("SALES_DT") < pyf.lit("2020-05-01")) & (pyf.col("SALES_DT") >= pyf.lit("2020-02-16")))

total_sales_by_date9 = data_vault_data9.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()
display(total_sales_by_date9)

# COMMAND ----------

CLIENT = 'nestlewaters'
COUNTRY_CODE = 'us'

# COMMAND ----------

# Read POS data
data_vault_data10 = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data10 = data_vault_data10.where((pyf.col("SALES_DT") < pyf.lit("2020-05-01")) & (pyf.col("SALES_DT") >= pyf.lit("2019-06-01")))

total_sales_by_date10 = data_vault_data10.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()
display(total_sales_by_date10)

# COMMAND ----------

total_inventory_by_date10 = data_vault_data10.select("SALES_DT", "ON_HAND_INVENTORY_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_inventory_by_date10)

# COMMAND ----------

CLIENT = 'rbhealth'
COUNTRY_CODE = 'ca'

# COMMAND ----------

# Read POS data
data_vault_data11a = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data11a = data_vault_data11a.where((pyf.col("SALES_DT") < pyf.lit("2020-05-01")) & (pyf.col("SALES_DT") >= pyf.lit("2019-06-01")))

total_sales_by_date11a = data_vault_data11a.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()
display(total_sales_by_date11a)

# COMMAND ----------

total_inventory_by_date11a = data_vault_data11a.select("SALES_DT", "ON_HAND_INVENTORY_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_inventory_by_date11a)

# COMMAND ----------

# Read POS data
data_vault_data11b = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
# data_vault_data11b = data_vault_data11b.where((pyf.col("SALES_DT") < pyf.lit("2020-05-01")) & (pyf.col("SALES_DT") >= pyf.lit("2019-06-01")))

total_sales_by_date11b = data_vault_data11b.select("SALES_DT", "POS_ITEM_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()
display(total_sales_by_date11b)

# COMMAND ----------

total_inventory_by_date11b = data_vault_data11b.select("SALES_DT", "ON_HAND_INVENTORY_QTY").orderBy("SALES_DT").groupBy("SALES_DT").sum()

display(total_inventory_by_date11b)

# COMMAND ----------


