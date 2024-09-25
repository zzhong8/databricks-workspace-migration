# Databricks notebook source
import pyspark.sql.functions as pyf
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Extract 1. Data on WM Clorox DRT calls and responses from August 2021

# COMMAND ----------

query = f"""
    SELECT
        
    --cl.client_id,
    --cl.description,
    --cl.description_short,

    c.call_id,
    --c.call_type_id, 
    --c.call_type_code,
    --c.call_type_description,
    c.calendar_key,
    --c.date_call_executed AS call_start,
    --c.call_completed_date AS call_end,
    c.call_planned_duration AS planned_duration,
    c.exec_duration,
    --c.call_status,
    --c.holding_id,

    il.upc,
    
    --q.question_id,
    q.question,
    q.category_name,
    --q.question_group,
    q.question_category,
    q.question_type,
    --q.active,
    --q.status,
    --q.nars_source,
    
    qr.response_id,
    qr.response,
    qr.response_value,
    
    qr.store_id,
    qr.store_tdlinx_id,
    --qr.call_complete_status_id,
    --qr.call_completed_date,
    qr.response_type,

    --qr.item_level_id,
    qr.item_dimension_id,
    qr.item_entity_id,
    qr.action_completed,
    
    s.store_name,
    s.channel_description,
    s.store_city,
    s.store_state_or_province,
    s.country_description
    
    FROM acosta_retail_analytics_im.fact_calls AS c
    
    JOIN acosta_retail_analytics_im.fact_questionresponse AS qr ON c.call_id = qr.call_id
    JOIN acosta_retail_analytics_im.dim_question AS q ON qr.question_id = q.question_id
    
    JOIN acosta_retail_analytics_im.vw_dimension_itemlevel il on qr.item_level_id = il.item_level_id
    
    JOIN acosta_retail_analytics_im.vw_dimension_store AS s on c.store_id = s.store_id
    LEFT JOIN acosta_retail_analytics_im.vw_dimension_client AS cl on qr.client_id = cl.client_id

--  ONE MONTH ONLY
    WHERE c.calendar_key BETWEEN 20210801 AND 20210831
    AND qr.calendar_key BETWEEN 20210801 AND 20210831
 
    AND qr.response != "No Response"
 
    and qr.client_id = 869 -- Clorox
    and qr.holding_id = 71 -- Walmart
    and c.call_type_code like '%DRT%' -- DRT

    ORDER BY call_id
    """

df_pyspark = spark.sql(query)
print(f'df count = {df_pyspark.cache().count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC Ari: tell me any columns you've added, thanks!
# MAGIC - Added: qr.action_completed

# COMMAND ----------

display(df_pyspark)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Assign actions

# COMMAND ----------

# Convert from pyspark to Pandas
df = df_pyspark.toPandas()
df['action'] = ''

# How many remaining null actions?
remaining_nulls = len(df[df['action'] == ''])
print(f"{remaining_nulls:,} ({round(100*remaining_nulls/len(df), 2)}%) rows with null actions remaining")

# COMMAND ----------

# MAGIC %md
# MAGIC Update action for action_completed = 2000 (Observed Completion) or 3000 (Not Successfully Completed)

# COMMAND ----------

# action_completed == 2000 -> "Shelf Inspected - No Action Taken"
df['action'].mask(
  (df['action_completed'] == 2000),
  'Shelf Inspected - No Action Taken', inplace=True)

# action_completed == 3000, "Store Refused..." -> "Spoke With Store"
df['action'].mask(
  (df['action_completed'] == 3000) & (df['response'].str.startswith('Store Refused')),
  'Spoke With Store', inplace=True)

# How many remaining null actions?
remaining_nulls = len(df[df['action'] == ''])
print(f"{remaining_nulls:,} ({round(100*remaining_nulls/len(df),2)}%) rows with null actions remaining")

# COMMAND ----------

# MAGIC %md
# MAGIC Update action for category_name = In-Store Intel

# COMMAND ----------

category_name = 'In-Store Intel'

# "In-Store Intel" & "Yes" -> "Photo Submission"
df['action'].mask(
  (df['category_name'] == category_name) & (df['response'] == 'Yes'),
  'Photo Submission', inplace=True)

# How many remaining null actions?
remaining_nulls = len(df[df['action'] == ''])
print(f"{remaining_nulls:,} ({round(100*remaining_nulls/len(df),2)}%) rows with null actions remaining")

# COMMAND ----------

# MAGIC %md
# MAGIC Update action for category_name = Audit

# COMMAND ----------

category_name = 'Audit'

# "Audit" & "Replenishment" -> "stocked"
responses = ['Low Stock - Replenishment', 'Out of Stock - Replenishment']
df['action'].mask(
  (df['category_name'] == category_name) & (df['response'].isin(responses)),
  'stocked', inplace=True)

# "Audit" & "Adjusted Facings"/"Rotated Product" -> "Shelf Maintenance"
responses = ['Adjusted Facings', 'Rotated Product']
df['action'].mask(
  (df['category_name'] == category_name) & (df['response'].isin(responses)),
  'Shelf Maintenance', inplace=True)

# "Audit" & "Built Display" -> "display"
df['action'].mask(
  (df['category_name'] == category_name) & (df['response'] == 'Built Display'),
  'display', inplace=True)

# "Audit" & "Clip Strip Placed" -> "Clip Strip"
df['action'].mask(
  (df['category_name'] == category_name) & (df['response'] == 'Clip Strip Placed'),
  'Clip Strip', inplace=True)

# "Audit" & "Ordered" -> "ordered"
df['action'].mask(
  (df['category_name'] == category_name) & (df['response'] == 'Ordered'),
  'ordered', inplace=True)

# "Audit" & "Replaced Missing Label"/"Placed Rollback Flags"/"IRC Placement"/"Placed New Item Flag" -> "placedpos"
responses = ['Replaced Missing Label', 'Placed Rollback Flags', 'IRC Placement', 'Placed New Item Flag']
df['action'].mask(
  (df['category_name'] == category_name) & (df['response'].isin(responses)),
  'placedpos', inplace=True)

# How many remaining null actions?
remaining_nulls = len(df[df['action'] == ''])
print(f"{remaining_nulls:,} ({round(100*remaining_nulls/len(df),2)}%) rows with null actions remaining")

# COMMAND ----------

# MAGIC %md
# MAGIC Update action for category_name = null

# COMMAND ----------

category_name = 'null'

# "null" & "Inventory Sold Down:No Intervention Made" -> "Shelf Inspected - No Action Taken"
df['action'].mask(
  (df['category_name'] == category_name) & (df['response'].str.startswith('Inventory Sold Down')),
  'Shelf Inspected - No Action Taken', inplace=True)

# "null" & "RCM Stocked Shelf" -> "stocked"
df['action'].mask(
  (df['category_name'] == category_name) & (df['response'] == 'RCM Stocked Shelf'),
  'stocked', inplace=True)

# "null" & "RCM Corrected Facing" -> "Shelf Maintenance"
df['action'].mask(
  (df['category_name'] == category_name) & (df['response'] == 'RCM Corrected Facing'),
  'Shelf Maintenance', inplace=True)

# "null" & "RCM Filled Existing Display" -> "packout"
df['action'].mask(
  (df['category_name'] == category_name) & (df['response'] == 'RCM Filled Existing Display'),
  'packout', inplace=True)

# "null" & "RCM Req. Inv Correction" -> "invcorr"
df['action'].mask(
  (df['category_name'] == category_name) & (df['response'] == 'RCM Req. Inv Correction'),
  'invcorr', inplace=True)

# "null" & "Store Refused Intervention" -> "Spoke With Store"
df['action'].mask(
  (df['category_name'] == category_name) & (df['response'] == 'Store Refused Intervention'),
  'Spoke With Store', inplace=True)

# "null" & "RCM Built Display" -> "display"
df['action'].mask(
  (df['category_name'] == category_name) & (df['response'] == 'RCM Built Display'),
  'display', inplace=True)

# How many remaining null actions?
remaining_nulls = len(df[df['action'] == ''])
print(f"{remaining_nulls:,} ({round(100*remaining_nulls/len(df),2)}%) rows with null actions remaining")

# COMMAND ----------

# MAGIC %md
# MAGIC Update action for category_name = Display Building/Maintaining

# COMMAND ----------

category_name = 'Display Building/Maintaining'

# "Display Building/Maintaining" & "SEC DISPLAY: What locations did you place display(s)? Select all that apply" & "Yes" -> display"
df['action'].mask(
  (df['category_name'] == category_name) & (df['question'].str.startswith('SEC DISPLAY: What locations did you place display(s)?')) & (df['response'].str.startswith('Yes')),
  'display', inplace=True)

# "Display Building/Maintaining" & "SEC DISPLAY: Build a display..." & "Yes" -> display"
df['action'].mask(
  (df['category_name'] == category_name) & (df['question'].str.startswith('SEC DISPLAY: Build a display')) & (df['response'].str.startswith('Yes')),
  'display', inplace=True)

# "Display Building/Maintaining" & "SEC DISPLAY: Build a display..." & "No: Manager Refused" -> "Spoke With Store"
df['action'].mask(
  (df['category_name'] == category_name) & (df['question'].str.startswith('SEC DISPLAY: Build a display')) & (df['response'].str.startswith('No: Manager Refused')),
  'Spoke With Store', inplace=True)

# "Display Building/Maintaining" & "FEATURE: Build..." -> "display"
df['action'].mask(
  (df['category_name'] == category_name) & (df['question'].str.startswith('FEATURE: Build')) & (df['response'].str.startswith('Yes')),
  'display', inplace=True)

# "Display Building/Maintaining" & "FEATURE: Ensure..." -> "Spoke With Store"
df['action'].mask(
  (df['category_name'] == category_name) & (df['question'].str.startswith('FEATURE: Build')) & (df['response'].str.startswith('No: Manager Refused')),
  'Spoke With Store', inplace=True)

# "Display Building/Maintaining" & "FEATURE: Ensure..." -> "Shelf Inspected - No Action Taken"
df['action'].mask(
  (df['category_name'] == category_name) & (df['question'].str.startswith('FEATURE: Ensure')),
  'Shelf Inspected - No Action Taken', inplace=True)

# "Display Building/Maintaining" & "Scan the..." -> "Shelf Inspected - No Action Taken"
df['action'].mask(
  (df['category_name'] == category_name) & (df['question'].str.startswith('Scan the')),
  'display', inplace=True)

# How many remaining null actions?
remaining_nulls = len(df[df['action'] == ''])
print(f"{remaining_nulls:,} ({round(100*remaining_nulls/len(df),2)}%) rows with null actions remaining")

# COMMAND ----------

# MAGIC %md
# MAGIC Update action for category_name = Secondary Display

# COMMAND ----------

category_name = 'Secondary Display'

# "Secondary Display" & "SEC DISPLAY: Were you able..." & "Yes" -> display"
df['action'].mask(
  (df['category_name'] == category_name) & (df['question'].str.startswith('SEC DISPLAY: Were you able')) & (df['response'].str.startswith('Yes')),
  'display', inplace=True)

# "Secondary Display" & "SEC DISPLAY: Were you able..." & "No... Store Refused" -> 'Spoke With Store'
df['action'].mask(
  (df['category_name'] == category_name) & (df['question'].str.startswith('SEC DISPLAY: Were you able')) & (df['response'].str.startswith('No')) & (df['response'].str.contains('Store Refused')),
  'Spoke With Store', inplace=True)

# "Secondary Display" & "SEC DISPLAY: Were you able..." & "No... Str Rfused" -> display"
df['action'].mask(
  (df['category_name'] == category_name) & (df['question'].str.startswith('SEC DISPLAY: Were you able')) & (df['response'].str.startswith('No')) & (df['response'].str.contains('Str Rfused')),
  'Spoke With Store', inplace=True)

# "Secondary Display" & "SEC DISPLAY:Build a secondary display..." & ">0" -> display"
df['action'].mask(
  (df['category_name'] == category_name) & (df['question'].str.startswith('SEC DISPLAY:Build a secondary display')) & (df['response'].str.startswith('Yes')),
  'display', inplace=True)

# How many remaining null actions?
remaining_nulls = len(df[df['action'] == ''])
print(f"{remaining_nulls:,} ({round(100*remaining_nulls/len(df),2)}%) rows with null actions remaining")

# COMMAND ----------

# MAGIC %md
# MAGIC Update action for category_name = Pack Out

# COMMAND ----------

category_name = 'Pack Out'

# "Pack Out" & response_value > 0 -> packout"
df['action'].mask(
  (df['category_name'] == category_name) & (df['response_value'] > 0),
  'packout', inplace=True)

# How many remaining null actions?
remaining_nulls = len(df[df['action'] == ''])
print(f"{remaining_nulls:,} ({round(100*remaining_nulls/len(df),2)}%) rows with null actions remaining")

# COMMAND ----------

# MAGIC %md
# MAGIC Update action for category_name = Promotional Signage

# COMMAND ----------

category_name = 'Promotional Signage'

# "Promotional Signage" & "Yes..." -> "placedpos"
df['action'].mask(
  (df['category_name'] == category_name) & (df['response'].str.startswith('Yes')),
  'placedpos', inplace=True)

# "Promotional Signage" & "No: Mgr refused" -> "Spoke With Store"
df['action'].mask(
  (df['category_name'] == category_name) & (df['response'].str.startswith('No: Mgr refused')),
  'Spoke With Store', inplace=True)

# How many remaining null actions?
remaining_nulls = len(df[df['action'] == ''])
print(f"{remaining_nulls:,} ({round(100*remaining_nulls/len(df),2)}%) rows with null actions remaining")

# COMMAND ----------

# MAGIC %md
# MAGIC Update action for category_name = Clip Strip Placement

# COMMAND ----------

category_name = 'Clip Strip Placement'

# "Clip Strip Placement" & "SEC..." & "Yes..." -> Clip Strip"
df['action'].mask(
  (df['category_name'] == category_name) & (df['question'].str.startswith('SEC')) & (df['response'].str.startswith('Yes')),
  'Clip Strip', inplace=True)

# "Clip Strip Placement" & "FIXTURE..." & "Yes..." -> Clip Strip"
df['action'].mask(
  (df['category_name'] == category_name) & (df['question'].str.startswith('FIXTURE')) & (df['response'].str.startswith('Yes')),
  'Clip Strip', inplace=True)

# "Clip Strip Placement" & "SEC..." & "No: Out of empty clipstrips-Ordered" -> ordered"
df['action'].mask(
  (df['category_name'] == category_name) & (df['question'].str.startswith('FIXTURE')) & (df['response'].str.startswith('No')) & (df['response'].str.startswith('Ordered')),
  'ordered', inplace=True)

# "Clip Strip Placement" & "SEC..." & "No: Manager refused" -> "Spoke With Store"
df['action'].mask(
  (df['category_name'] == category_name) & (df['question'].str.startswith('FIXTURE')) & (df['response'].str.startswith('No: Manager refused')),
  'Spoke With Store', inplace=True)

# How many remaining null actions?
remaining_nulls = len(df[df['action'] == ''])
print(f"{remaining_nulls:,} ({round(100*remaining_nulls/len(df),2)}%) rows with null actions remaining")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add estimated time take for each action

# COMMAND ----------

action_times = {
  'Clip Strip': 9.67,
  'Photo Submission': 4.71,
  'Shelf Inspected': 0.93,
  'Shelf Maintenance': 2.00,
  'Spoke With Store': 3.49,
  'display': 9.89,
  'invcorr': 1.96,
  'ordered': 2.26,
  'packout': 3.94,
  'placedpos': 8.89,
  'stocked': 1.07
}

df['time'] = df['action'].map(action_times)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Extract 2. Time estimates by action type

# COMMAND ----------

df_action_times = pd.DataFrame.from_dict(data=action_times, orient='index').reset_index()
df_action_times = df_action_times.rename(columns={'index': 'action', 0: 'time'})
df_action_times

# COMMAND ----------

display(df_action_times)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Extract 3. Measured Value by action

# COMMAND ----------


