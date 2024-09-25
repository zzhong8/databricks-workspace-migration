# Databricks notebook source
# MAGIC %md
# MAGIC # Measurement Results
# MAGIC
# MAGIC ## TODOs
# MAGIC 1. Load intervention data for relative date range vs. current date; intended to limit the join sizes for next step
# MAGIC 2. Join intervention data to measurement data table; keep only rows without a measured value
# MAGIC 3. Apply measurement model to remaining unscored interventions
# MAGIC 4. Write scored data to measurement table

# COMMAND ----------

from pprint import pprint

import itertools

import numpy as np
import pandas as pd

import matplotlib.pyplot as graph
import seaborn as sns

import pyspark.sql.functions as pyf
import pyspark.sql.types as pyt

# COMMAND ----------

# Inputs for Retailer, Client and RunID
dbutils.widgets.text('retailer', '', 'Retailer')
dbutils.widgets.text('client', '', 'Client')
dbutils.widgets.text('country_code', '', 'Country Code')

required = ('retailer', 'client', 'country_code')

def process_notebook_inputs(name):
    value = dbutils.widgets.get(name).strip().lower()
    if name in required and value == '':
        raise ValueError(f'"{name}" is required')
        
    return value

retailer, client, country_code = [process_notebook_inputs(s) for s in ('retailer', 'client', 'country_code')]

_ = [print(inputs) for inputs in (retailer, client, country_code)]

# COMMAND ----------

df = spark.read.format('delta').load(f'/mnt/processed/causal_results/retailer={retailer}/client={client}/country_code={country_code}')
 
df = df.filter(pyf.year('SALES_DT') == 2019)
 
total_baseline = df.agg(pyf.sum('BASELINE')).toPandas().values[0][0]
 
df = df.filter('INTERVENTION_TYPE == "Book Stock Error Corrected"')
 
print(f'{df.cache().count():,}')

# COMMAND ----------

df_spark = df
df = df.toPandas().sort_values(by='SALES_DT')

# COMMAND ----------

display(df_spark)

# COMMAND ----------

# Average IV per intervention if we are measuring All Days

# COMMAND ----------

df_spark.select('INTERVENTION_ID').distinct().count()

# COMMAND ----------

display(df_spark.groupby('INTERVENTION_TYPE').sum())

# COMMAND ----------

display(df_spark.groupby('INTERVENTION_TYPE', 'DIFF_DAY').mean())

# COMMAND ----------

top_items_path = '/mnt/artifacts/reference/KHZ_NC_top_items.csv'

# COMMAND ----------

top_items = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load(top_items_path)

# COMMAND ----------

display(top_items)

# COMMAND ----------

top_items_filtered = top_items.filter(pyf.col('RETAILER') == pyf.lit(retailer)).filter(pyf.col('CLIENT') == pyf.lit(client)).select('RETAILER_ITEM_ID')

display(top_items_filtered)

# COMMAND ----------

df_top_items = df_spark.join(top_items_filtered, "RETAILER_ITEM_ID")

# COMMAND ----------

display(df_top_items.groupby('INTERVENTION_TYPE', 'DIFF_DAY').mean())

# COMMAND ----------

df_top_item = df_top_items.where(pyf.col("RETAILER_ITEM_ID") == 51169241)

# COMMAND ----------

display(df_top_item.groupby('INTERVENTION_TYPE', 'DIFF_DAY').mean())

# COMMAND ----------

# MAGIC %md
# MAGIC ## All Interventions

# COMMAND ----------

results, results_lift, results_lift_total = 3*[np.zeros((2, 2))]
print(f'**{retailer.upper()}** - All Interventions')
for i, (target, rule) in enumerate(itertools.product(['QINTERVENTION_EFFECT', 'INTERVENTION_EFFECT'], ['All', 'Positive Only'])):
    if rule.lower() == 'all':
        estimate = df[target].sum()
    else:
        estimate = np.maximum(df[target], 0).sum()
        
    lift = estimate / df['BASELINE'].sum()
    lift_total = estimate / total_baseline
    print(f'Σ({target} - Baseline) with ({rule}) rule = {estimate:,.0f}\n Lift on invention days = {lift:.2%} @ Total lift = {lift_total:.2%}')
    print()
    
    row, col = i//2, i%2 
    results[row, col] = estimate

# COMMAND ----------

graph.figure(figsize=(6, 6))
graph.title(f'{retailer.title()} All')
sns.heatmap(
    results, 
    cmap='Blues', 
    annot=True, fmt=',.0f',
    xticklabels=['All Days', 'Positive Days Only'],
    yticklabels=['(Actual - RSV Baseline)', '(Smoothed - RSV Baseline)'],
    cbar=False
)
display(graph.show())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Uplift Only

# COMMAND ----------

df = df.query('INTERVENTION_GROUP == "Uplift"')

# COMMAND ----------

results = np.zeros((2, 2))
print(f'**{retailer.upper()}** - Uplift Interventions Only')
for i, (target, rule) in enumerate(itertools.product(['QINTERVENTION_EFFECT', 'INTERVENTION_EFFECT'], ['All', 'Positive Only'])):
    Δ = df[target]
    if rule.lower() == 'all':
        estimate = Δ.sum()
    else:
        estimate = np.maximum(Δ, 0).sum()
        
    lift = estimate / df['BASELINE'].sum()
    lift_total = estimate / total_baseline
    print(f'Σ({target} - Baseline) with ({rule}) rule = {estimate:,.0f}\n Lift on invention days = {lift:.2%} @ Total lift = {lift_total:.2%}')
    print()
    
    row, col = i//2, i%2 
    results[row, col] = estimate

# COMMAND ----------

graph.figure(figsize=(6, 6))
graph.title(f'{retailer.title()} Uplift Only')
sns.heatmap(
    results, 
    cmap='Blues', 
    annot=True, fmt=',.0f',
    xticklabels=['All Days', 'Positive Days Only'],
    yticklabels=['(Actual - RSV Baseline)', '(Smoothed - RSV Baseline)'],
    cbar=False
)
display(graph.show())

# COMMAND ----------

# MAGIC %md
# MAGIC ### TEMP: Save Apr 2019 Subset

# COMMAND ----------

# df_yemi = df_spark.filter(pyf.year('SALES_DT') == 2019)
# display(df_yemi)

# df_yemi.coalesce(1).write.format('csv').save(f'/mnt/processed/yemi/analysis/{client}-{retailer}-2019.csv', header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Extra Summary Statistics

# COMMAND ----------

df_spark = df_spark\
  .withColumn('resid', df_spark['POS_ITEM_QTY'] - df_spark['EXPECTED'])\
  .withColumn('abs_resid', pyf.abs(pyf.col('resid')))

intervention_effects = df_spark.filter('INTERVENTION_EFFECT != 0').select('RETAILER_ITEM_ID', 'INTERVENTION_EFFECT').groupby('RETAILER_ITEM_ID').mean().toPandas()

summary = df_spark\
  .agg(
    pyf.pow(pyf.stddev(pyf.col('POS_ITEM_QTY')), 2).alias('null_var'),
    pyf.pow(pyf.stddev(pyf.col('resid')), 2).alias('res_var'),
    pyf.mean('abs_resid').alias('MAE')
  ).toPandas()

# COMMAND ----------

print(summary)
print('Retailer Level Rsq =', 1 - summary['res_var'].values[0] / summary['null_var'].values[0])

# COMMAND ----------

summary = df_spark.groupby('RETAILER_ITEM_ID').agg(
    pyf.pow(pyf.stddev(pyf.col('POS_ITEM_QTY')), 2).alias('null_var'),
    pyf.pow(pyf.stddev(pyf.col('resid')), 2).alias('res_var'),
    pyf.mean('abs_resid').alias('MAE'),
    pyf.mean('resid').alias('mean_resid'),
    pyf.mean('POS_ITEM_QTY').alias('mean_pos_item_qty'),
    pyf.count('resid').alias('N')
).toPandas()
summary['Rsq'] = 1 - summary['res_var'] / summary['null_var']

print(summary.shape)
display(summary)

# COMMAND ----------

sns.distplot(summary['MAE'], kde=False)
graph.title('Per Retail Item MAE (Smaller is Better)')
display(graph.show())
graph.close()

# COMMAND ----------

# sns.distplot(summary['Rsq'], kde=False)
# graph.title(r'Per Retail Item $R^2$')
# display(graph.show())
# graph.close()

# COMMAND ----------

graph.plot(summary['mean_pos_item_qty'], summary['MAE'], '.')
graph.ylabel('MAE')
graph.xlabel('Mean POS Item Qty')
display(graph.show())
graph.close()

# COMMAND ----------

graph.plot(summary['mean_pos_item_qty'], summary['Rsq'], '.')
graph.ylabel('Rsq')
graph.xlabel('Mean POS Item Qty')
display(graph.show())
graph.close()

# COMMAND ----------

graph.plot(summary['N'], summary['MAE'], '.')
graph.xscale('log')
graph.ylabel('MAE')
graph.xlabel('Sample Size')
display(graph.show())
graph.close()

# COMMAND ----------

graph.title(f'Daily Average Intervention Effect per Item ({len(intervention_effects):,} Items)')
sns.distplot(intervention_effects['avg(INTERVENTION_EFFECT)'])
graph.xlabel('Daily Average Intervention Effect (N Units)')
display(graph.show())
graph.close()

# COMMAND ----------

# Lift overall
display(
    df_spark.groupby(pyf.year('SALES_DT')).agg(
        pyf.sum('INTERVENTION_EFFECT').alias('sum-intervention'), 
        pyf.sum('BASELINE').alias('sum-baseline'),
        pyf.sum('POS_ITEM_QTY').alias('sum-pos')
    ).withColumn(
        'lift',
        pyf.col('sum-intervention') / pyf.col('sum-baseline')
    ).withColumn(
        'lift with actuals',
        pyf.col('sum-intervention') / (pyf.col('sum-pos') - pyf.col('sum-intervention'))
    ).withColumn(
        'proportion attributable',
        pyf.col('sum-intervention') / pyf.col('sum-pos')
    )
)

# COMMAND ----------

# Lift given Intervention
display(
    df_spark.filter(
        'INTERVENTION_TYPE != "None"'
    ).groupby(
        pyf.year('SALES_DT')
    ).agg(
        pyf.sum('INTERVENTION_EFFECT').alias('sum-intervention'),
        pyf.sum('BASELINE').alias('sum-baseline'),
        pyf.sum('POS_ITEM_QTY').alias('sum-pos')
    ).withColumn(
        'lift|Intervention',
        pyf.col('sum-intervention') / pyf.col('sum-baseline')
    ).withColumn(
        'lift with actuals | Intervention',
        pyf.col('sum-intervention') / (pyf.col('sum-pos') - pyf.col('sum-intervention'))
    ).withColumn(
        'proportion attributable',
        pyf.col('sum-intervention') / pyf.col('sum-pos')
    )
)

# COMMAND ----------

# display(df_results.groupby('RETAILER_ITEM_ID', 'INTERVENTION_TYPE').mean())
agg_lift_results = df_spark\
    .withColumn('YEAR', pyf.year('SALES_DT'))\
    .withColumn('LIFT_PERCENT', pyf.col('INTERVENTION_EFFECT') / pyf.col('BASELINE'))\
    .groupby('RETAILER_ITEM_ID', 'INTERVENTION_TYPE', 'YEAR')\
    .agg(
        pyf.mean('POS_ITEM_QTY').alias('mean-pos'),
        pyf.mean('BASELINE').alias('mean-baseline'),
        pyf.mean('EXPECTED').alias('mean-expected'),
        pyf.mean('INTERVENTION_EFFECT').alias('mean-intervention-effect'),
        pyf.sum('BASELINE').alias('sum-baseline'),
        pyf.sum('INTERVENTION_EFFECT').alias('sum-intervention-effect')
    )\
    .toPandas()

agg_lift_results['lift-percent'] = agg_lift_results['sum-intervention-effect'] / agg_lift_results['sum-baseline']

display(agg_lift_results)

# COMMAND ----------

display(
    df_spark.withColumn('LIFT_PERCENT', pyf.col('INTERVENTION_EFFECT') / pyf.col('BASELINE'))\
    .select('RETAILER_ITEM_ID', 'INTERVENTION_TYPE', 'POS_ITEM_QTY', 'BASELINE', 'EXPECTED', 'INTERVENTION_EFFECT', 'LIFT_PERCENT')\
    .groupby('RETAILER_ITEM_ID', 'INTERVENTION_TYPE')\
    .sum()
)
