# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC select q.category_name, count(*)
# MAGIC FROM acosta_retail_analytics_im.fact_questionresponse AS qr
# MAGIC LEFT JOIN acosta_retail_analytics_im.dim_question AS q ON qr.question_id = q.question_id
# MAGIC WHERE 
# MAGIC   --item_cap_type = 3000 
# MAGIC   item_entity_id = "PRSK"
# MAGIC   and qr.calendar_key BETWEEN 20210701 AND 20210731
# MAGIC   AND call_type_code NOT IN ("PGCSCAN","PGRAM","PGRAMC","PROJECT","PGRIALERTS","Nielsen")
# MAGIC   and response != "No Response"
# MAGIC   --and action_completed != ""
# MAGIC   group by category_name

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select q.category_name, count(*)
# MAGIC FROM acosta_retail_analytics_im.fact_questionresponse AS qr
# MAGIC LEFT JOIN acosta_retail_analytics_im.dim_question AS q ON qr.question_id = q.question_id
# MAGIC WHERE 
# MAGIC   --item_cap_type = 3000 
# MAGIC   item_entity_id = "PRSK"
# MAGIC   and qr.calendar_key BETWEEN 20210701 AND 20210731
# MAGIC   AND call_type_code NOT IN ("PGCSCAN","PGRAM","PGRAMC","PROJECT","PGRIALERTS","Nielsen")
# MAGIC   and response != "No Response"
# MAGIC   --and action_completed != ""
# MAGIC   group by category_name

# COMMAND ----------

# MAGIC %md
# MAGIC # Time Standards Exploration

# COMMAND ----------

import pyspark.sql.functions as pyf
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime

from itertools import chain

# from sklearn.neighbors import KernelDensity

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load infomart ("clean") data

# COMMAND ----------

# Get infomart calls for Aug 17
query = f"""
    SELECT c.call_id,
    c.call_type_description,
    c.calendar_key,
    c.date_call_executed AS call_start,
    c.call_completed_date AS call_end,
    c.call_planned_duration AS planned_duration,
    c.exec_duration,
    c.call_status,
    c.holding_id,
    
    qr.store_id,
    qr.client_id,
    qr.call_complete_status_id,
--    qr.response_type,
--    qr.last_edit_date,
--    qr.num_attachments,

    q.category_name,
    q.question_category,
    q.question_type,

    s.store_name,
    s.country_description,
--    s.store_address_1,
--    s.store_sqft,
    
    cl.client_id,
    cl.description,
    
    q.question,
    response,
    response_value,
    action_completed,
    system_response,
    action_required,
    channel_description,
    store_city,
    store_state_or_province
    
    FROM acosta_retail_analytics_im.fact_calls AS c
    
    JOIN acosta_retail_analytics_im.fact_questionresponse AS qr ON c.call_id = qr.call_id
    JOIN acosta_retail_analytics_im.dim_question AS q ON qr.question_id = q.question_id
    
    JOIN acosta_retail_analytics_im.vw_dimension_store AS s on c.store_id = s.store_id
    LEFT JOIN acosta_retail_analytics_im.vw_dimension_client AS cl on qr.client_id = cl.client_id
    
--  ONE DAY ONLY
    WHERE c.calendar_key = 20210817
    AND qr.calendar_key = 20210817

--  ONE MONTH ONLY
--    WHERE c.calendar_key BETWEEN 20210701 AND 20210731
--    AND qr.calendar_key BETWEEN 20210701 AND 20210731
 
    ORDER BY call_id
    """
df = spark.sql(query)
print(f'df count = {df.cache().count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join for menaingful holding_id on: acosta_retail_analytics_im.interventions_parameters

# COMMAND ----------

display(df.filter(pyf.col('call_id') == '00Q35BEJA'))

# COMMAND ----------

# MAGIC %md
# MAGIC One option: filter 'question' by action words (e.g. "pack out", "build", "display")
# MAGIC
# MAGIC Create a "valid activity" flowchart with meaningful activity labels as results
# MAGIC
# MAGIC Flowchart as a UDF

# COMMAND ----------

# MAGIC %md
# MAGIC call_status C = 'Complete'
# MAGIC
# MAGIC
# MAGIC Focus on call_type_description == "ZACT DRT"

# COMMAND ----------

df.groupBy('call_type_description').count().collect()

# COMMAND ----------

df.groupBy('call_complete_status_id').count().collect()

# COMMAND ----------

display(df.filter(pyf.col('call_id') == '00ULDVEJE'))

# COMMAND ----------

# Map mdm_holding_id -> retailer_name

holding_id_mapping = {
    71: 'Walmart',
    91: 'Kroger',
    3257: 'Tesco'
}

mapping_expr = pyf.create_map([pyf.lit(x) for x in chain(*holding_id_mapping.items())])

df = df.withColumn('retailer_name', mapping_expr[pyf.col('mdm_holding_id')])
df = df.drop('mdm_holding_id')

# COMMAND ----------

display(df.filter(df.mdm_banner_nm == 'Asda').limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q: What does mdm_banner_nm actually mean? Tesco doesn't own Asda...

# COMMAND ----------

df.groupBy('mdm_banner_nm').count().collect()

# COMMAND ----------

# # Compute time difference
# df = df.withColumn('time_diff', pyf.col('call_end').cast('long') - pyf.col('call_start').cast('long'))
# df = df.withColumn('time_diff_mins', pyf.round(pyf.col('time_diff')/60))
# df = df.drop('time_diff')

# COMMAND ----------

# MAGIC %md
# MAGIC # MONDAY
# MAGIC - Check if you have all the correct fields you need (client_id, store_id - is that a thing you can get?)
# MAGIC - Subset data for a specific client/store?
# MAGIC - Start trying to break down data by task type

# COMMAND ----------

# Convert to pandas
df_pd = df.toPandas()

# COMMAND ----------

df_pd.head(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore call_start, call_end distributions

# COMMAND ----------

# Group by unique call_id
keep_cols = ['call_id', 'call_start', 'call_end', 'planned_duration', 'exec_duration', 'time_diff_mins']
df_unique_calls = df_pd.groupby('call_id').first().reset_index()[keep_cols]
len(df_unique_calls)

# COMMAND ----------

# Drop null times
df_unique_calls = df_unique_calls[df_unique_calls['exec_duration'].notnull()]
len(df_unique_calls)

# COMMAND ----------

# Get start, end times in minutes since midnight
df_unique_calls['call_start_mins'] = df_unique_calls['call_start'].apply(lambda t: t.time().hour * 60 + t.time().minute)
df_unique_calls['call_end_mins'] = df_unique_calls['call_end'].apply(lambda t: t.time().hour * 60 + t.time().minute)

# COMMAND ----------

# Examine distribution of call start/end times
plt.figure(figsize=(6,6))
plt.hist(df_unique_calls['call_start_mins'], bins=20, color='green', alpha=0.4, label='start times')
plt.hist(df_unique_calls['call_end_mins'], bins=20, color='red', alpha=0.4, label='end times')
plt.vlines(8*60, 0, 4000, label='08:00')
plt.vlines(12*60, 0, 4000, label='12:00')
plt.vlines(17*60, 0, 4000, label='17:00')
plt.xlim(0, 24*60)
plt.xlabel('minutes since midnight')
plt.ylabel('count')
plt.legend()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove outliers

# COMMAND ----------

# RESET df_unique_calls
df_unique_calls = df_unique_calls_orig.copy()

# COMMAND ----------

df_unique_calls_orig = df_unique_calls.copy()

# COMMAND ----------

# MAGIC %md
# MAGIC Remove "too-short" planned_duration outliers

# COMMAND ----------

# Remove any "too-short" planned durations (calls should be planned for at least X minutes)
min_planned_duration = 5
temp_len = len(df_unique_calls)
df_unique_calls = df_unique_calls[df_unique_calls['planned_duration'] >= min_planned_duration]

print(f"A minimum planned_duration of {min_planned_duration} removes {temp_len - len(df_unique_calls):,} calls")

# COMMAND ----------

# MAGIC %md
# MAGIC Remove "too short" exec_duration outliers

# COMMAND ----------

# Remove any "too-short" exec durations (call should take at least X minutes)
min_exec_duration = 5
temp_len = len(df_unique_calls)
df_unique_calls = df_unique_calls[df_unique_calls['exec_duration'] >= min_planned_duration]

print(f"A minimum exec_duration of {min_exec_duration} removes an additional {temp_len - len(df_unique_calls):,} calls")

# COMMAND ----------

# MAGIC %md
# MAGIC Remove too-long-exec_duration outliers (exec_duration more than N * planned_duration)

# COMMAND ----------

# Remove any "too-long" exec durations (shouldn't be more than N * planned_duration)
multiplier = 2
temp_len = len(df_unique_calls)
df_unique_calls = df_unique_calls[multiplier * df_unique_calls['planned_duration'] >= df_unique_calls['exec_duration']]

print(f"Calls where exec_duration doesn't exceed {multiplier} * planned_duration removes an additional {temp_len - len(df_unique_calls):,} calls")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Examine total call duration (all inlier data)

# COMMAND ----------

plt.figure(figsize=(6,6))
plt.hist(df_unique_calls['planned_duration'], bins=20, color='green', alpha=0.4, label='planned duration')
plt.hist(df_unique_calls['exec_duration'], bins=20, color='red', alpha=0.4, label='exec duration')
plt.legend()
plt.show()

# COMMAND ----------

# Examine call duration
plt.figure(figsize=(6,6))
plt.hist(df_unique_calls_inliers['exec_duration'], bins=20, color='green', label='call duration')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get KDE

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# Time to get a subset of the data using sql
import pyspark.sql.functions as pyf
query = f"""
    select a.call_id,
    question_id,
    question_answer_id,
    actionitem,
    answer,
    comments,
    init_timestamp,
    done_timestamp,
    sync_timestamp,
    exec_duration,
    duration
    from
    (
        select question_id,
        question_answer_id,
        call_id,
        lower(actionitem) as actionitem,
        lower(answer) as answer,
        lower(comments) as comments
        from nars_raw.aiau
        where question_approved = 2000 and extract_date >= '2021-01-01' and actioncompleted = 1000
    ) a
    inner join
    (
        select *
        from
        (
            select call_id,
            init_timestamp,
            done_timestamp,
            sync_timestamp,
            exec_duration,
            duration,
            row_number() over(partition by call_id order by extract_date desc) as rn
            from nars_raw.agac
            where extract_date >= '2021-01-01'
        )
        where rn = 1
    ) b
    on a.call_id = b.call_id
"""
df = spark.sql(query)
print(f'df count = {df.cache().count():,}') # Used to be 179k before the duration filters

# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

df = df.withColumn('time_diff', pyf.col('done_timestamp').cast('long') - pyf.col('init_timestamp').cast('long'))
df = df.withColumn('time_diff_mins', pyf.round(pyf.col('time_diff')/60))
df = df.drop('time_diff')

# COMMAND ----------

df_pd = df.toPandas()

# COMMAND ----------

# Drop NaN values
df_pd = df_pd[~df_pd['time_diff_mins'].isna()].reset_index(drop=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Examine all times

# COMMAND ----------

df_pd['time_diff_mins']

# COMMAND ----------



# COMMAND ----------

# Examine Probability Density Estimate for times
from sklearn.neighbors import KernelDensity
import numpy as np
import matplotlib.pyplot as plt

# COMMAND ----------

# Get list of time differences
time_diffs = df_pd['time_diff_mins'].values.reshape(1, -1)

print('min:', min(time_diffs[0]))
print('max:', max(time_diffs[0]))
print(f"Total: {len(time_diffs[0]):,}")

# COMMAND ----------

# How many are negative?
print(f"{len([i for i in time_diffs[0] if i < 0]):,}")

# COMMAND ----------

# Explore negative values
df_pd[df_pd['time_diff_mins'] < 0].head()

# COMMAND ----------

z=1

# COMMAND ----------

# Subset positive values
time_diffs_pos = np.array([i for i in time_diffs[0] if i >= 0]).reshape(-1, 1)
print(f"{len(time_diffs_pos[0]):,}")

# COMMAND ----------

# Histogram of all positive time differences (all minutes)
plt.figure(figsize=(8,8))
plt.hist(x=time_diffs_pos, bins=30)
plt.xlim(0, max(time_diffs_pos))
plt.show()

# COMMAND ----------

time_diffs_pos

# COMMAND ----------

# Histogram of all positive time differences (5000 minutes or less)
time_diffs_pos_5kmax = [i for i in time_diffs_pos if i <= 5000]

# COMMAND ----------

plt.figure(figsize=(8,8))
plt.hist(x=time_diffs_pos_5kmax, bins=30)
plt.xlim(0, 5000)
plt.show()

# COMMAND ----------

# Graph all positive time diffs
X_plot = np.linspace(0, max(time_diffs_pos[0]), int(max(time_diffs_pos[0]))).reshape(-1, 1)
X_plot.shape

# COMMAND ----------

kde = KernelDensity(kernel='gaussian', bandwidth=0.2)
kde.fit(time_diffs_pos)
log_dens = kde.score_samples(X_plot)

# COMMAND ----------

X_plot[:,0].shape

# COMMAND ----------

x = X_plot[:,0]
y = log_dens

plt.figure(figsize=(8,8))
plt.plot(x, y)
plt.show()

# COMMAND ----------

df = df.filter(\
    ~((pyf.col('actionitem').rlike('(build|did your store have|did you place)')) & (pyf.col('answer').rlike('no')))
)

# Need to build out logic to transform the df into a format where I can perform the group by
# Create a dictionary of key: tuple of items
# key: (label, mask_and_operation)
actionitem_dict = {'pre-sell': ('task: cases, displays, or promos were pre-sold', pyf.when(pyf.col('actionitem').rlike('pre-sell'), 1).otherwise(0)),
  'pack out': ('task: cases or units were packed out', pyf.when(pyf.col('actionitem').rlike('pack out'), 1).otherwise(0)),
  ' sell': ('task: cases were sold', pyf.when(pyf.col('actionitem').rlike(' sell'), 1).otherwise(0)),
  'adjust': ('task: adjusted balance on hand/computer assisted ordering', pyf.when(pyf.col('actionitem').rlike('adjust'), 1).otherwise(0)),
  'build - yes': ('task: display was built/maintained by rep',
      pyf.when((pyf.col('actionitem').rlike('build')) & (pyf.col('answer').rlike('yes')), 1).otherwise(0)),
  'build - already or acosta built': ('task: display was already built',
      pyf.when((pyf.col('actionitem').rlike('build')) & (~pyf.col('answer').rlike('yes')), 1).otherwise(0)),
  'rotate': ('task: items were rotated', pyf.when(pyf.col('actionitem').rlike('rotate'), 1).otherwise(0)),
  'is the size - 4/8/12 display recorded': ('task: 4, 8, or 12 foot display recorded',
      pyf.when((pyf.col('actionitem').rlike('is the size')) & (pyf.col('answer').rlike('(4|8|12)')), 1).otherwise(0)),
  'is the size - other display recorded': ('task: other display recorded',
      pyf.when((pyf.col('actionitem').rlike('is the size')) & (~pyf.col('answer').rlike('(4|8|12)')), 1).otherwise(0)),
  'you gain': ('task: incremental facing/shelf facings/distributions were gained', pyf.when(pyf.col('actionitem').rlike('you gain'), 1).otherwise(0)),
  'did your store have': ('task: TLC trays were in store when rep arrived', pyf.when(pyf.col('actionitem').rlike('did your store have'), 1).otherwise(0)),
  'this publix': ('task: publix store planned to order a ss shipper', pyf.when(pyf.col('actionitem').rlike('this publix'), 1).otherwise(0)),
  'what action - rotated stock': ('task: stock was rotated',
      pyf.when((pyf.col('actionitem').rlike('what action')) & (pyf.col('answer').rlike('rotated')), 1).otherwise(0)),
  'what action - other': ('task: other misc action performed',
      pyf.when((pyf.col('actionitem').rlike('what action')) & (~pyf.col('answer').rlike('rotated')), 1).otherwise(0)),
  'did you present': ('task: an opportunity was  presented to store management', pyf.when(pyf.col('actionitem').rlike('did you present'), 1).otherwise(0)),
  'did you place': ('task: An IRC, POS, rack, clip strip or tray was placed', pyf.when(pyf.col('actionitem').rlike('did you place'), 1).otherwise(0))}

# Transform
for key in actionitem_dict:
    label, mask_and_operation = actionitem_dict[key]
    df = df.withColumn(label, mask_and_operation)
    
# print(f'df count = {df.cache().count():,}') # Used to be 179k before the duration filters

# COMMAND ----------

df_pd = df.toPandas()

# COMMAND ----------

print(f"{len(df_pd):,}")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# Get a small, workable subset (for testing)
df_small = df.limit(100)
print(f'df_small count = {df_small.cache().count():,}')

# COMMAND ----------

display(df_small.limit(5))

# COMMAND ----------

df_small = df_small.withColumn('time_diff', pyf.col('done_timestamp').cast('long') - pyf.col('init_timestamp').cast('long'))
df_small = df_small.withColumn('time_diff_mins', pyf.round(pyf.col('time_diff')/60))

# COMMAND ----------

display(df_small.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC #### All tasks: Explore time difference between done_timestamp and init_timestamp (in minutes)

# COMMAND ----------

import matplotlib.pyplot as plt

# COMMAND ----------

all_time_diffs_mins = df_small.select('time_diff_mins').toPandas()['time_diff_mins']

plt.figure(figsize=(6,6))
plt.hist(all_time_diffs_mins, bins=40)
plt.show()

# COMMAND ----------



# COMMAND ----------

all_time_diffs_mins

# COMMAND ----------

# Group by call_id, given all tasks were completed
# Sum up actions and durations
agg_dict = {**{col: 'mean' for col in df.columns if 'duration' in col},
            **{col: 'sum' for col in df.columns if 'task' in col}}
df_agg = df.groupby('call_id').agg(agg_dict)
# df_agg = df_agg.withColumn('total completed actions', sum(df_agg[col] for col in df_agg.columns[1:]))
df_agg = df_agg.toPandas()

# Check if there needs to be a filter
print(f'The shape is {df_agg.shape}')

# # Filter
# df_agg = df_agg.filter(pyf.col('total completed actions')>0)

# print(f'After shape is ({df_agg.count()}, {len(df_agg.columns)})')

# COMMAND ----------

# Quick OLS check 
# Regress on `duration`, drop duration/call_id columns
import numpy as np
import statsmodels.api as sm
for duration in [col for col in df_agg.columns if 'duration' in col]:
    y = df_agg[duration]

    X_cols = [col for col in df_agg.columns if 'duration' not in col and 'call_id' not in col]
    X = df_agg[X_cols]
    # X = sm.add_constant(X)

    mod = sm.OLS(y, X)
    results = mod.fit()
    print(results.summary())

# for i, col in enumerate(X_cols):
#     print(f"For col='{col}', the {(1-alpha)*100: .0f}% confidence interval for this parameter is {results.conf_int(0.05)[i+1, :]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## NARS_RAW

# COMMAND ----------

# Time to get a subset of the data using sql

import pyspark.sql.functions as pyf
import re

month, year = 11, 2020

query = f"""
    select a.call_id,
    question_answer_id,
    actionitem,
    answer,
    actioncompleted,
    retailcall,
    done_status,
    novisit,
    done_duration,
    exec_duration,
    duration
    from
    (
        select *
        from
        (
            select question_answer_id,
            call_id,
            actioncompleted,
            lower(actionitem) as actionitem,
            lower(answer) as answer,
            row_number() over(partition by question_answer_id order by extract_date desc) as rn
            from nars_raw.aiau
            where extract(month from extract_date) = 11 and extract(year from extract_date) = 2020
        )
        where rn = 1
    ) a
    inner join
    (
        select *
        from
        (
            select call_id,
            retailcall,
            done_status,
            novisit,
            (unix_timestamp(done_timestamp) - unix_timestamp(init_timestamp))/60 as done_duration,
            exec_duration,
            duration,
            row_number() over(partition by call_id order by extract_date desc) as rn
            from nars_raw.agac
            where extract(month from extract_date) = 11 and extract(year from extract_date) = 2020
        )
        where rn = 1
    ) b
    on a.call_id = b.call_id
"""

df = spark.sql(query)
print(f'df count = {df.cache().count():,}')

# Need to build out logic to transform the df into a format where I can perform the group by
# Create a dictionary of key: tuple of items
# key: (label, mask_and_operation)
actionitem_dict = {'pre-sell': ('task: cases, displays, or promos were pre-sold', pyf.when(pyf.col('actionitem').rlike('pre-sell'), 1).otherwise(0)),
  'pack out': ('task: cases or units were packed out', pyf.when(pyf.col('actionitem').rlike('pack out'), 1).otherwise(0)),
  ' sell': ('task: cases were sold', pyf.when(pyf.col('actionitem').rlike(' sell'), 1).otherwise(0)),
  'adjust': ('task: adjusted balance on hand/computer assisted ordering', pyf.when(pyf.col('actionitem').rlike('adjust'), 1).otherwise(0)),
  'build - yes': ('task: display was built/maintained by rep',
      pyf.when((pyf.col('actionitem').rlike('build')) & (pyf.col('answer').rlike('yes')), 1).otherwise(0)),
  'build - already or acosta built': ('task: display was already built',
      pyf.when((pyf.col('actionitem').rlike('build')) & (~pyf.col('answer').rlike('yes')), 1).otherwise(0)),
  'rotate': ('task: items were rotated', pyf.when(pyf.col('actionitem').rlike('rotate'), 1).otherwise(0)),
  'is the size - 4/8/12 display recorded': ('task: 4, 8, or 12 foot display recorded',
      pyf.when((pyf.col('actionitem').rlike('is the size')) & (pyf.col('answer').rlike('(4|8|12)')), 1).otherwise(0)),
  'is the size - other display recorded': ('task: other display recorded',
      pyf.when((pyf.col('actionitem').rlike('is the size')) & (~pyf.col('answer').rlike('(4|8|12)')), 1).otherwise(0)),
  'you gain': ('task: incremental facing/shelf facings/distributions were gained', pyf.when(pyf.col('actionitem').rlike('you gain'), 1).otherwise(0)),
  'did your store have': ('task: TLC trays were in store when rep arrived', pyf.when(pyf.col('actionitem').rlike('did your store have'), 1).otherwise(0)),
  'this publix': ('task: publix store planned to order a ss shipper', pyf.when(pyf.col('actionitem').rlike('this publix'), 1).otherwise(0)),
  'what action - rotated stock': ('task: stock was rotated',
      pyf.when((pyf.col('actionitem').rlike('what action')) & (pyf.col('answer').rlike('rotated')), 1).otherwise(0)),
  'what action - other': ('task: other misc action performed',
      pyf.when((pyf.col('actionitem').rlike('what action')) & (~pyf.col('answer').rlike('rotated')), 1).otherwise(0)),
  'did you present': ('task: an opportunity was  presented to store management', pyf.when(pyf.col('actionitem').rlike('did you present'), 1).otherwise(0)),
  'did you place': ('task: An IRC, POS, rack, clip strip or tray was placed', pyf.when(pyf.col('actionitem').rlike('did you place'), 1).otherwise(0))}

# Transform
for key in actionitem_dict:
    label, mask_and_operation = actionitem_dict[key]
    df = df.withColumn(label, mask_and_operation)

# Work in Pandas    
df = df.toPandas()

# Column fix... to be used in pymc3
df.columns = [re.sub('/|, | |\-|:', '_', col) for col in df.columns]

# COMMAND ----------

df.info()

# COMMAND ----------

df.head()

# COMMAND ----------

# Try to clean the df for anomalous done_duration values
# Less than a day to execute
threshold = 24*60
df_filtered = df[(df['done_duration']>0) &\
                 (df['done_duration'].notnull()) &\
                 (df['done_duration']<=threshold) &\
                 (df['retailcall']==1) &\
                 (df['actioncompleted']==1000) &\
                 (df['done_status']==1000)]

# filters completed actions only, completed calls only, retail calls only, must have visited store
print(df_filtered.shape)

# COMMAND ----------

# Group by call_id, given all tasks were completed
# Sum up actions and durations

agg_dict = {**{col: 'mean' for col in df_filtered.columns if 'duration' in col},
            **{col: 'sum' for col in df_filtered.columns if 'task' in col}}
df_agg = df_filtered.groupby('call_id').agg(agg_dict)

#If a task occurs in less than 1% of the calls then remove the task
for col in df_agg.columns:
    if 'task' in col:
        check = df_agg[col].value_counts(normalize=True)
        if check.values[0] > 0.99:
            print(f'col to drop: {col}, % share: {check.values[0]: .2f}, value: {check.index[0]}')
            df_agg = df_agg.drop(col, axis=1)

# Filter taskless calls
df_agg = df_agg[df_agg.iloc[:, 3:].sum(axis=1)>0]
print(f'\nThe shape is {df_agg.shape}')

# COMMAND ----------

# Quick OLS check 
# Regress on `duration`, drop duration/call_id columns
import numpy as np
import statsmodels.api as sm

X_cols = [col for col in df_agg.columns if 'task' in col]
X = df_agg[X_cols]

y = df_agg['done_duration']
# X = sm.add_constant(X)

mod = sm.OLS(y, X)
results = mod.fit()
print(results.summary())

# COMMAND ----------

# Run the Beush-Pagan test to test heterskedasticity in the OLS approach
# Pvalues check out so it looks like the OLS approach is not outright rejected
from statsmodels.stats.diagnostic import het_breuschpagan

breushpaga_results = het_breuschpagan(results.resid, X)

print(f'lm_pvalue = {breushpaga_results [1]}')
print(f'lm_pvalue = {breushpaga_results [3]}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Benchmarking NNLS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find out how well the algorithm performs on large N (using bins)

# COMMAND ----------

import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_absolute_error
from scipy import stats
from scipy.optimize import nnls

def nnls_evaluation(N, P, scale=1.5, seed=2021):

    np.random.seed(seed)

    # Randomly instantiating the true coefficients
#     m_true = np.array([np.float(np.random.randint(2)) * stats.norm().rvs(1) for i in range(P)]).reshape((P,)) * scale
    m_true = stats.bernoulli(0.5).rvs(P) * stats.norm().rvs(P) * scale
    
    df_m_and_transforms = pd.DataFrame({'true coefficients': m_true, 
                                        'calculated coefficients': m_true,
                                        'operation': np.repeat('Normal RVS', P)})

    # Adding in noise 
    noise = scale * stats.norm().rvs(N)

    # Instantiate data
    data = stats.norm().rvs((N, P))

    # Anomalous data transforms
    original_list = range(data.shape[1])
    min_transforms = 1 + np.int(data.shape[1]*.01)
    list_index_1 = np.random.choice(original_list, min_transforms)
    list_index_2 = np.random.choice(np.array((list(set(original_list) - set(list_index_1)))), min_transforms)
    list_index_3 = np.random.choice(np.array((list(set(original_list) - set(list_index_1) - set(list_index_2)))), min_transforms)

    for var in list_index_1:
        data[:, var] = data[:, var]*100
        df_m_and_transforms.loc[var, 'operation'] = 'multiply by 100'

    for var in list_index_2:
        data[:, var] = np.exp(np.abs(data[:, var]))
        df_m_and_transforms.loc[var, 'operation'] = 'exponential'

    for var in list_index_3:
        data[:, var] = stats.semicircular().rvs(N)
        df_m_and_transforms.loc[var, 'operation'] = 'semicircular'


    # Create the target
    target = data @ m_true + noise
    target = target - target.min()

    plt.figure()
    plt.hist(target)
    plt.title("Distribution of the target")
    plt.show()

    # Test and evaluate
    X_train, _, y_train, _ = train_test_split(data, target, random_state=seed)

    model = LinearRegression(positive=True, fit_intercept=True)
    model.fit(X_train, y_train)

    df_m_and_transforms['calculated coefficients'] = model.coef_

    display(df_m_and_transforms)
    mae_baseline = mean_absolute_error(m_true, np.zeros(P))
    mae_actual = mean_absolute_error(m_true, model.coef_)

    print(f"The baseline MAE for the coefficients is {mae_baseline: .6f}")
    print(f"The MAE for the coefficients is {mae_actual: .6f}")
#     print(f"The actual/baseline MAPE ratio is for the coefficients is {mape_actual/mape_baseline: .4f}")

# COMMAND ----------

# MAGIC %load_ext memory_profiler

# COMMAND ----------

# MAGIC %memit nnls_evaluation(1000000, 30)

# COMMAND ----------

# MAGIC %memit nnls_evaluation(1000000, 100)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate varying lengths of fields

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nars_raw.dpau

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acosta_retail_analytics_im.vw_ds_intervention_input_nars

# COMMAND ----------

# MAGIC %sql
# MAGIC --- New ---
# MAGIC -- How many unique questions/QnAs/answers per month (in thousands)
# MAGIC --- OLD ---
# MAGIC -- How many distinct questions are there in two weeks: 468,596
# MAGIC -- How many distinct answers are there in two weeks: 495,270
# MAGIC -- How many distinct q&a's are there in two weeks: 495,270
# MAGIC -- how long does it take to find this: 38.09 s
# MAGIC select extract(month from extract_date) as month,
# MAGIC count(distinct dp_id)/1000 as count_unique_questions,
# MAGIC count(distinct dp_answer_id)/1000 as count_unique_answers,
# MAGIC count(distinct concat(dp_id, dp_answer_id))/1000 as count_unique_QnAs,
# MAGIC count(distinct DP_ANSWER_LM)/1000 as count_unique_answer_texts
# MAGIC from nars_raw.dpau
# MAGIC where extract(year from extract_date) = 2020
# MAGIC group by month
# MAGIC order by month

# COMMAND ----------

# MAGIC %sql
# MAGIC --- New ---
# MAGIC
# MAGIC --- OLD --
# MAGIC -- How many interventions are there in two weeks: 222,038
# MAGIC -- how long does it take to find this: 1.47 mins
# MAGIC select extract(month from call_date) as month,
# MAGIC count(distinct response_id) as count_unique_answers,
# MAGIC count(distinct standard_response_text) as count_unique_responses,
# MAGIC count(distinct standard_response_cd) as count_unique_responses_cd
# MAGIC from acosta_retail_analytics_im.vw_ds_intervention_input_nars
# MAGIC where extract(year from call_date) = 2020
# MAGIC group by month
# MAGIC order by month

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distribution of interventions by year and months

# COMMAND ----------

# MAGIC %sql
# MAGIC -- dpau
# MAGIC -- aggregate unique answer_id per retailer_id, client_id & subset by completed task
# MAGIC -- get min, median, max in a given year
# MAGIC select min(count_unique_tasks) as min,
# MAGIC percentile_approx(count_unique_tasks, 0.5) as median,
# MAGIC avg(count_unique_tasks) as avg,
# MAGIC percentile_approx(count_unique_tasks, 0.75) as quantile_75th,
# MAGIC percentile_approx(count_unique_tasks, 0.80) as quantile_80th,
# MAGIC percentile_approx(count_unique_tasks, 0.85) as quantile_85th,
# MAGIC percentile_approx(count_unique_tasks, 0.90) as quantile_90th,
# MAGIC percentile_approx(count_unique_tasks, 0.95) as quantile_95th,
# MAGIC max(count_unique_tasks) as max
# MAGIC from
# MAGIC (
# MAGIC   select 
# MAGIC   retailer_id,
# MAGIC   client_id,
# MAGIC   count(distinct dp_answer_id) as count_unique_tasks
# MAGIC   from nars_raw.dpau
# MAGIC   where extract(year from extract_date) = 2020 and (rank = 1000)
# MAGIC   group by retailer_id, client_id
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- aiau
# MAGIC -- aggregate unique answer_id per retailer_id, client_id & subset by completed task
# MAGIC -- get min, median, max in a given year
# MAGIC select min(count_unique_tasks) as min,
# MAGIC percentile_approx(count_unique_tasks, 0.5) as median,
# MAGIC avg(count_unique_tasks) as avg,
# MAGIC percentile_approx(count_unique_tasks, 0.75) as quantile_75th,
# MAGIC percentile_approx(count_unique_tasks, 0.80) as quantile_80th,
# MAGIC percentile_approx(count_unique_tasks, 0.85) as quantile_85th,
# MAGIC percentile_approx(count_unique_tasks, 0.90) as quantile_90th,
# MAGIC percentile_approx(count_unique_tasks, 0.95) as quantile_95th,
# MAGIC max(count_unique_tasks) as max
# MAGIC from
# MAGIC (
# MAGIC   select 
# MAGIC   retailer_id,
# MAGIC   client_id,
# MAGIC   count(distinct question_answer_id) as count_unique_tasks
# MAGIC   from nars_raw.aiau
# MAGIC   where extract(year from extract_date) = 2020 and (actioncompleted = 1000)
# MAGIC   group by retailer_id, client_id
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- dpau - month
# MAGIC -- aggregate unique answer_id per retailer_id, client_id & subset by completed task
# MAGIC -- get min, median, max in a given year
# MAGIC select month,
# MAGIC min(count_unique_tasks) as min,
# MAGIC percentile_approx(count_unique_tasks, 0.5) as median,
# MAGIC avg(count_unique_tasks) as avg,
# MAGIC percentile_approx(count_unique_tasks, 0.75) as quantile_75th,
# MAGIC percentile_approx(count_unique_tasks, 0.80) as quantile_80th,
# MAGIC percentile_approx(count_unique_tasks, 0.85) as quantile_85th,
# MAGIC percentile_approx(count_unique_tasks, 0.90) as quantile_90th,
# MAGIC percentile_approx(count_unique_tasks, 0.95) as quantile_95th,
# MAGIC max(count_unique_tasks) as max
# MAGIC from
# MAGIC (
# MAGIC   select 
# MAGIC   extract(month from extract_date) as month,
# MAGIC   retailer_id,
# MAGIC   client_id,
# MAGIC   count(distinct dp_answer_id) as count_unique_tasks
# MAGIC   from nars_raw.dpau
# MAGIC   where extract(year from extract_date) = 2020 and (rank = 1000)
# MAGIC   group by month, retailer_id, client_id
# MAGIC )
# MAGIC group by month
# MAGIC order by month

# COMMAND ----------

# MAGIC %sql
# MAGIC -- aiau - month
# MAGIC -- aggregate unique answer_id per retailer_id, client_id & subset by completed task
# MAGIC -- get min, median, max in a given year
# MAGIC select month,
# MAGIC min(count_unique_tasks) as min,
# MAGIC percentile_approx(count_unique_tasks, 0.5) as median,
# MAGIC avg(count_unique_tasks) as avg,
# MAGIC percentile_approx(count_unique_tasks, 0.75) as quantile_75th,
# MAGIC percentile_approx(count_unique_tasks, 0.80) as quantile_80th,
# MAGIC percentile_approx(count_unique_tasks, 0.85) as quantile_85th,
# MAGIC percentile_approx(count_unique_tasks, 0.90) as quantile_90th,
# MAGIC percentile_approx(count_unique_tasks, 0.95) as quantile_95th,
# MAGIC max(count_unique_tasks) as max
# MAGIC from
# MAGIC (
# MAGIC   select 
# MAGIC   extract(month from extract_date) as month,
# MAGIC   retailer_id,
# MAGIC   client_id,
# MAGIC   count(distinct question_answer_id) as count_unique_tasks
# MAGIC   from nars_raw.aiau
# MAGIC   where extract(year from extract_date) = 2020 and (actioncompleted = 1000)
# MAGIC   group by month, retailer_id, client_id
# MAGIC )
# MAGIC group by month
# MAGIC order by month
