# Databricks notebook source
from pprint import pprint

import pyspark.sql.functions as pyf

from acosta.measurement import required_columns, process_notebook_inputs

import acosta
from acosta.alerting.preprocessing import read_pos_data

print(acosta.__version__)

# COMMAND ----------

country_id = 1  # US
client_id = 16540 # Harrys Inc
holding_id = 71 # Walmart
banner_id = -1  # default

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc nars_raw.aiau

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct type, CLIENT_ID, RETAILER_ID from nars_raw.aiau
# MAGIC -- where client_id = 16540
# MAGIC -- and retailer_id = 71

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct type, CLIENT_ID, RETAILER_ID from nars_raw.aiau
# MAGIC where client_id = 16540
# MAGIC and retailer_id = 71

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct type, CLIENT_ID, RETAILER_ID, PRODUCT_ID, shelfcode from nars_raw.aiau
# MAGIC where client_id = 16540
# MAGIC and retailer_id = 71

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc nars_raw.dpau

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct type, CLIENT_ID, RETAILER_ID from nars_raw.dpau

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct type, CLIENT_ID, RETAILER_ID from nars_raw.dpau
# MAGIC where client_id = 16540
# MAGIC and retailer_id = 71

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct type, CLIENT_ID, RETAILER_ID, PRODUCT_ID, shelfcode from nars_raw.dpau
# MAGIC where client_id = 16540
# MAGIC and retailer_id = 71

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc retail_alert_walmart_harrysinc_us_im.alert_on_shelf_availability

# COMMAND ----------

# %sql

# SELECT SALES_DT, count(*) FROM retail_alert_kroger_harrysinc_us_im.alert_on_shelf_availability
# group by SALES_DT
# order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) FROM retail_alert_walmart_harrysinc_us_im.alert_on_shelf_availability
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) FROM retail_alert_walmart_harrysinc_us_im.alert_on_shelf_availability
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM retail_alert_walmart_harrysinc_us_im.alert_on_shelf_availability

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SALES_DT, count(*) FROM retail_alert_walmart_harrysinc_us_im.alert_inventory_cleanup
# MAGIC where SALES_DT >= '2022-01-01'
# MAGIC group by SALES_DT
# MAGIC order by SALES_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc nars_raw.dpau

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct retailer_id FROM nars_raw.dpau 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM nars_raw.dpau where retailer_id = 71

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM nars_raw.dpau where retailer_id = 2596

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_retail_analytics_im;
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC   FROM acosta_retail_analytics_im.interventions_response_mapping
# MAGIC where
# MAGIC   mdm_country_id = 1
# MAGIC and
# MAGIC   objective_typ = 'DLA'
# MAGIC and
# MAGIC   standard_response_cd = 'invcorr'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct mdm_client_id, mdm_client_nm FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC call_date >= '2022-06-11'
# MAGIC order by
# MAGIC mdm_client_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC call_date >= '2022-02-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC objective_typ='DLA'
# MAGIC and
# MAGIC epos_retailer_item_id is not NULL
# MAGIC and
# MAGIC actionable_flg is not NULL
# MAGIC and
# MAGIC call_date >= '2022-02-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC objective_typ='Data Led Alerts'
# MAGIC and
# MAGIC call_date >= '2022-02-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC objective_typ='Data Led Alerts'
# MAGIC and
# MAGIC epos_retailer_item_id is NULL
# MAGIC and
# MAGIC actionable_flg is not NULL
# MAGIC and
# MAGIC call_date >= '2022-02-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC objective_typ='Data Led Alerts'
# MAGIC and
# MAGIC epos_retailer_item_id is NULL
# MAGIC and
# MAGIC actionable_flg is not NULL
# MAGIC and
# MAGIC call_date >= '2022-02-01'
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC objective_typ='Data Led Alerts'
# MAGIC and
# MAGIC epos_retailer_item_id is not NULL
# MAGIC and
# MAGIC actionable_flg is not NULL
# MAGIC and
# MAGIC call_date >= '2022-02-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC epos_retailer_item_id is not NULL
# MAGIC and
# MAGIC actionable_flg is not NULL
# MAGIC and
# MAGIC call_date >= '2022-06-11'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT call_date, count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC objective_typ='Data Led Alerts'
# MAGIC and
# MAGIC call_date >= '2022-02-01'
# MAGIC group by call_date
# MAGIC order by call_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC call_date >= '2022-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC call_date >= '2022-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where
# MAGIC mdm_country_id = 1 -- US
# MAGIC and
# MAGIC mdm_client_id = 16540 -- Harrys Inc
# MAGIC and
# MAGIC mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC objective_typ = 'Data Led Alerts'
# MAGIC and
# MAGIC call_date >= '2022-07-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM 
# MAGIC acosta_retail_analytics_im.vw_ds_intervention_input_nars vdiin 
# MAGIC left join
# MAGIC acosta_retail_analytics_im.ds_intervention_summary dis 
# MAGIC on
# MAGIC vdiin.response_id = dis.response_id
# MAGIC where
# MAGIC vdiin.mdm_country_id = 1 -- US
# MAGIC and
# MAGIC vdiin.mdm_client_id = 882 -- Danone
# MAGIC and
# MAGIC vdiin.mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(vdiin.mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC vdiin.objective_typ = 'DLA'
# MAGIC and
# MAGIC vdiin.call_date >= '2022-02-01'
# MAGIC order by vdiin.call_date, vdiin.nars_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM 
# MAGIC acosta_retail_analytics_im.vw_ds_intervention_input_nars vdiin 
# MAGIC left join
# MAGIC acosta_retail_analytics_im.ds_intervention_summary dis 
# MAGIC on
# MAGIC vdiin.response_id = dis.response_id
# MAGIC where
# MAGIC vdiin.mdm_country_id = 1 -- US
# MAGIC and
# MAGIC vdiin.mdm_client_id = 882 -- Danone
# MAGIC and
# MAGIC vdiin.mdm_holding_id = 71 -- Walmart
# MAGIC and
# MAGIC coalesce(vdiin.mdm_banner_id, -1) = -1 -- default
# MAGIC and
# MAGIC vdiin.objective_typ = 'DLA'
# MAGIC and
# MAGIC vdiin.call_date >= '2022-02-01'
# MAGIC order by vdiin.call_date, vdiin.nars_response_text

# COMMAND ----------

# MAGIC %sql
# MAGIC select Companyid, company, lkp_productgroupid, productgroupname, captype, capvalue from bobv2.vw_bobv2_caps where companyid = 577

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where mdm_country_id = 2

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct
# MAGIC mdm_country_id,
# MAGIC mdm_country_nm,
# MAGIC mdm_holding_id,
# MAGIC mdm_holding_nm,
# MAGIC mdm_banner_id,
# MAGIC mdm_banner_nm,
# MAGIC mdm_client_id,
# MAGIC mdm_client_nm,
# MAGIC epos_retailer_item_id,
# MAGIC objective_typ
# MAGIC FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where mdm_country_id = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where mdm_holding_id = 2596 -- Walmart Canada
# MAGIC and epos_retailer_item_id is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc nars_raw.dpau

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where standard_response_text = 'Initiate ISA Scan Process'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) FROM acosta_retail_analytics_im.ds_intervention_summary
# MAGIC where standard_response_text = 'Initiate ISA Scan Process'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct mdm_holding_nm, mdm_holding_id FROM acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct mdm_holding_nm, mdm_holding_id FROM acosta_retail_analytics_im.ds_intervention_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct mdm_client_nm, mdm_client_id FROM acosta_retail_analytics_im.ds_intervention_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM acosta_retail_analytics_im.ds_intervention_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT distinct mdm_client_nm, mdm_client_id FROM acosta_retail_analytics_im.ds_intervention_summary

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Ari's exploration

# COMMAND ----------

import numpy as np
import pandas as pd
import datetime
import matplotlib.pyplot as plt

# COMMAND ----------

# Is Average Lost Sales down after June 5?
query = '''SELECT * FROM retail_alert_walmart_harrysinc_us_im.alert_on_shelf_availability'''
df_ps = spark.sql(query)
df = df_ps.toPandas()
print(f"{len(df):,}")

# COMMAND ----------

# Convert datatypes
df['LOST_SALES_AMT'] = df['LOST_SALES_AMT'].astype('float')
df['ON_HAND_INVENTORY_QTY'] = df['ON_HAND_INVENTORY_QTY'].astype('float')

# COMMAND ----------

df.head()

# COMMAND ----------

# Check mean LOST_SALES_AMT, ON_HAND_INVENTORY_QTY by day
df_gb = df.groupby('SALES_DT').mean().reset_index()
df_gb = df_gb.sort_values(by='SALES_DT')
df_gb.columns

df_gb

# COMMAND ----------

# Check mean lost sales amt by day
df_gb = df.groupby('SALES_DT').mean().reset_index()
df_gb = df_gb.sort_values(by='SALES_DT')

plt.figure(figsize=(12,4))
plt.plot(df_gb['SALES_DT'], df_gb['LOST_SALES_AMT'])
plt.title('Avg LOST_SALES_AMT by date', fontsize=16)
plt.plot()

plt.figure(figsize=(12,4))
plt.plot(df_gb['SALES_DT'], df_gb['ON_HAND_INVENTORY_QTY'])
plt.title('Avg ON_HAND_INVENTORY_QTY by date', fontsize=16)
plt.plot()

# COMMAND ----------

# Check count of lost sales amt by day
df_gb = df.groupby('SALES_DT').count().reset_index()
df_gb = df_gb.sort_values(by='SALES_DT')

plt.figure(figsize=(12,6))
plt.plot(df_gb['SALES_DT'], df_gb['LOST_SALES_AMT'])
plt.title('Count of LOST_SALES_AMT by date', fontsize=16)
plt.plot()

plt.figure(figsize=(12,6))
plt.plot(df_gb['SALES_DT'], df_gb['ON_HAND_INVENTORY_QTY'])
plt.title('Count of ON_HAND_INVENTORY_QTY by date', fontsize=16)
plt.plot()

# COMMAND ----------

# How many stores in total?
len(df['ORGANIZATION_UNIT_NUM'].value_counts()) 

# COMMAND ----------

# Subset by date
df_before = df[(df['SALES_DT'] >= datetime.date(2022, 4, 15)) & (df['SALES_DT'] < datetime.date(2022, 6, 5))]
df_after = df[df['SALES_DT'] >= datetime.date(2022, 6, 5)]
print(f"df_before: {len(df_before):,}")
print(f"df_after: {len(df_after):,}")

# COMMAND ----------

df_before

# COMMAND ----------

# Is there a difference in the stores with the most alerts?

# Subset to "before" vs "after" dates
lsv_counts_before = df_before['ORGANIZATION_UNIT_NUM'].value_counts()
lsv_counts_after = df_after['ORGANIZATION_UNIT_NUM'].value_counts()

# Get number of unique dates
n_unique_dates_before = len(df_before['SALES_DT'].unique())
n_unique_dates_after = len(df_after['SALES_DT'].unique())
print("Before:", n_unique_dates_before, "unique days")
print("After:", n_unique_dates_after, "unique days")

# Scale LSV counts by unique days
lsv_counts_before_per_day = lsv_counts_before/n_unique_dates_before
lsv_counts_after_per_day = lsv_counts_after/n_unique_dates_after

# COMMAND ----------

most_common_before_stores = lsv_counts_before_per_day.index[:40]
most_common_after_stores = lsv_counts_after_per_day.index[:40]

# COMMAND ----------

plt.hist(lsv_counts_before_per_day.values, bins=20)
plt.show()

# COMMAND ----------

plt.hist(lsv_counts_after_per_day.values, bins=20)
plt.show()

# COMMAND ----------

lsv_counts_after_per_day['3534']

# COMMAND ----------

before_stores_only = [store for store in most_common_before_stores if store not in most_common_after_stores]
after_stores_only = [store for store in most_common_after_stores if store not in most_common_before_stores]
both_stores = [store for store in most_common_before_stores if store in most_common_after_stores]

print('before_stores_only:', len(before_stores_only))
print('after_stores_only:', len(after_stores_only))
print('both_stores:', len(both_stores))

# COMMAND ----------

both_stores

# COMMAND ----------

after_stores_only

# COMMAND ----------

index = np.arange(40)
bar_width = 0.35
fig, ax = plt.subplots()
before = ax.bar(index, df_before['ORGANIZATION_UNIT_NUM'].value_counts()[:40], bar_width, label='before')
after = ax.bar(index+bar_width, df_after['ORGANIZATION_UNIT_NUM'].value_counts()[:40], bar_width, label='after')
ax.set_xticks(index+barwidth/2)
ax.set_labels([])

# COMMAND ----------

plt.barh(df_before_top_stores)

# COMMAND ----------

df_gb = df.groupby('SALES_DT').count()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Load tables: lost_sales_value, drfe_forecast_baseline_unit

# COMMAND ----------

query = '''SELECT * FROM retail_alert_walmart_harrysinc_us_im.lost_sales_value'''
df_lsv_ps = spark.sql(query)
df_lsv = df_lsv_ps.toPandas()

query = '''SELECT * FROM retail_alert_walmart_harrysinc_us_im.drfe_forecast_baseline_unit'''
df_baseline_ps = spark.sql(query)
df_baseline = df_baseline_ps.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### df_lsv (lost_sales_value)

# COMMAND ----------

print(len(df_lsv))
df_lsv.head()

# COMMAND ----------

df_lsv['LOST_SALES_AMT'] = df_lsv['LOST_SALES_AMT'].astype('float')

df_lsv_gb = df_lsv.groupby('SALES_DT').mean().reset_index()
plt.figure(figsize=(12,6))
plt.plot(df_lsv_gb['SALES_DT'], df_lsv_gb['LOST_SALES_AMT'])
plt.title('Mean of LOST_SALES_AMT by date', fontsize=16)
plt.plot()

df_lsv_gb = df_lsv.groupby('SALES_DT').count().reset_index()
plt.figure(figsize=(12,6))
plt.plot(df_lsv_gb['SALES_DT'], df_lsv_gb['LOST_SALES_AMT'])
plt.title('Count of LOST_SALES_AMT by date', fontsize=16)
plt.plot()

# COMMAND ----------

# MAGIC %md
# MAGIC #### df_baseline (drfe_forecast_baseline_unit)

# COMMAND ----------

df_baseline.head()

# COMMAND ----------

print(len(df_baseline['HUB_RETAILER_ITEM_HK'].unique()))
print(len(df_baseline['MODEL_ID'].unique()))

# COMMAND ----------

df_baseline['BASELINE_POS_ITEM_QTY'] = df_baseline['BASELINE_POS_ITEM_QTY'].astype('float')

df_baseline_gb = df_baseline.groupby('SALES_DT').mean().reset_index()
plt.figure(figsize=(12,6))
plt.plot(df_baseline_gb['SALES_DT'], df_baseline_gb['BASELINE_POS_ITEM_QTY'])
plt.title('Mean of BASELINE_POS_ITEM_QTY by date', fontsize=16)
plt.plot()

df_baseline_gb = df_baseline.groupby('SALES_DT').count().reset_index()
plt.figure(figsize=(12,6))
plt.plot(df_baseline_gb['SALES_DT'], df_baseline_gb['BASELINE_POS_ITEM_QTY'])
plt.title('Count of BASELINE_POS_ITEM_QTY by date', fontsize=16)
plt.plot()

# COMMAND ----------

# MAGIC %md
# MAGIC ### The jump in counts occurs June 7, 2022

# COMMAND ----------

# HUB_RETAILER_ITEM_HK is each distinct retailer-item combo
cols = ['HUB_ORGANIZATION_UNIT_HK', 'MODEL_ID', 'HUB_RETAILER_ITEM_HK']
g = df_baseline.groupby('SALES_DT').nunique()[cols].reset_index() #.apply(lambda x: len(np.unique(x))) #.agg(['unique'])

for c in cols:
  plt.figure(figsize=(12,6))
  plt.plot(g['SALES_DT'], g[c])
  plt.title(f'Count of unique of {c} by date', fontsize=16)
  plt.plot()

# COMMAND ----------

# MAGIC %md
# MAGIC ### RETAILER_ITEM_HK joins to retaillink_walmart_harrysinc_us_dv.vw_latest_sat_epos_summary

# COMMAND ----------

df_baseline[df_baseline['HUB_ORGANIZATION_UNIT_HK']]

# COMMAND ----------

print(len(df_baseline))
df_baseline.head()

# COMMAND ----------

lsv_by_date = df_lsv.groupby('SALES_DT').sum()
# plt.hist(lsv_by_date)

# COMMAND ----------



# COMMAND ----------


