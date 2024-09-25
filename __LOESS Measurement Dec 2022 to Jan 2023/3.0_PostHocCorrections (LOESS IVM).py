# Databricks notebook source
# MAGIC %md
# MAGIC # Measurement Results
# MAGIC
# MAGIC This notebook computes all the _post hoc_ corrections and busniess rules that needs to occur the measurement results.
# MAGIC
# MAGIC **Corrections made are**
# MAGIC - `nonegative_rule` If a completed intervention has a negative effect, set it to zero.
# MAGIC
# MAGIC **Concerns**
# MAGIC - When the delta table is being updated is there any race conditions issues that could occur if 2 jobs try to update the delta simultaneously?

# COMMAND ----------

spark.conf.set('hive.exec.dynamic.partition.mode', 'nonstrict')
spark.conf.set('hive.exec.dynamic.partition', 'true')
spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

# COMMAND ----------

import pandas as pd

import matplotlib.pyplot as graph
import seaborn as sns

import pyspark.sql.functions as pyf
import pyspark.sql.types as pyt

from delta.tables import DeltaTable

from acosta.measurement import process_notebook_inputs

# COMMAND ----------

# Inputs get required inputs
dbutils.widgets.text('country_id', '-1', 'Country ID')
dbutils.widgets.text('client_id', '-1', 'Client ID')
dbutils.widgets.text('holding_id', '-1', 'Holding ID')
dbutils.widgets.text('banner_id', '-1', 'Banner ID')

input_names = ('country_id', 'client_id', 'holding_id', 'banner_id')

country_id, client_id, holding_id, banner_id = [process_notebook_inputs(dbutils.widgets.get(s)) for s in input_names]

print('Country ID =', country_id)
print('Client ID =', client_id)
print('Holding ID =', holding_id)
print('Banner ID =', banner_id)

# COMMAND ----------

# Load required data
df_results = spark.read.format('delta').load(
    f'/mnt/processed/measurement/cache/{client_id}-{country_id}-{holding_id}-{banner_id}-processed'
)
total_baseline = df_results.agg(pyf.sum('BASELINE')).toPandas().values[0][0]

df_interventions = spark.read.format('delta').load(
    f'/mnt/processed/measurement/cache/{client_id}-{country_id}-{holding_id}-{banner_id}-intervention'
)
df_unique_interventions = df_interventions.select(
    'mdm_country_nm', 'mdm_holding_nm', 'mdm_banner_nm', 'mdm_client_nm',
    'store_acosta_number', 'epos_organization_unit_num', 'epos_retailer_item_id',
    'objective_typ', 'call_id', 'response_id',
    'nars_response_text', 'standard_response_text',
    'duration', 'call_date'
).distinct()

print(f'{df_results.cache().count():,}')
print(f'{df_interventions.cache().count():,}')
print(f'{df_unique_interventions.cache().count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Make _Post Hoc_ Adjustments

# COMMAND ----------

df_results = df_results.filter('standard_response_cd != "none"')
print(f'{df_results.cache().count():,}', 'N')
print(df_results.select('response_id').distinct().count(), 'N interventions')
print(df_results.select('call_id').distinct().count(), 'N visits')


# COMMAND ----------

# value & impact calculation
df_results = df_results.withColumn('INTERVENTION_VALUE', df_results['INTERVENTION_EFFECT'] * df_results['PRICE'])
print(f'{df_results.cache().count():,}')

# COMMAND ----------

# def book_stock_rule_udf(df):
#     if 'Book Stock Error' in df['INTERVENTION_TYPE'].unique()[0]:
#         claimed_selector = (df['DIFF_DAY'] >= 2) & (df['DIFF_DAY'] <= 6)
#
#         # Set to zero
#         df['INTERVENTION_EFFECT'] = 0.0
#         df['QINTERVENTION_EFFECT'] = 0.0
#
#         # Set values
#         df.loc[claimed_selector, 'INTERVENTION_EFFECT'] = df[claimed_selector]['POS_ITEM_QTY']
#         df['QINTERVENTION_EFFECT'] = df['INTERVENTION_EFFECT']
#     return df


def nonnegative_rule_udf(df):
    if df['INTERVENTION_EFFECT'].sum() <= 0 or df['INTERVENTION_VALUE'].sum() <= 0:
        df['INTERVENTION_EFFECT'] = 0.0
        df['QINTERVENTION_EFFECT'] = 0.0
        df['INTERVENTION_VALUE'] = 0.0
    return df


# Execute UDFs
df_results = df_results.groupby('response_id').applyInPandas(nonnegative_rule_udf, schema=df_results.schema)
print(f'{df_results.cache().count():,}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Save Results

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO NOTE Using a shadow database for now for testing only. We need to change it to acosta_retail_analytics_im later before we prmote code to production
# MAGIC create table if not exists acosta_retail_analytics_im.ds_intervention_summary
# MAGIC (
# MAGIC     mdm_country_nm  String,
# MAGIC     mdm_holding_nm  String,
# MAGIC     mdm_banner_nm string,
# MAGIC     mdm_client_nm string,
# MAGIC     store_acosta_number Int,
# MAGIC     epos_organization_unit_num  String,
# MAGIC     epos_retailer_item_id String,
# MAGIC     objective_typ String,
# MAGIC     call_id String,
# MAGIC     response_id String,
# MAGIC     nars_response_text  String,
# MAGIC     standard_response_text  String,
# MAGIC     standard_response_cd  String,
# MAGIC     measurement_duration  Int,
# MAGIC     is_complete   Boolean,
# MAGIC     total_intervention_effect Decimal(15, 2),
# MAGIC     total_qintervention_effect  Decimal(15, 2),
# MAGIC     total_impact  Decimal(15, 2),
# MAGIC     total_qimpact Decimal(15, 2),
# MAGIC     load_ts timestamp,
# MAGIC     mdm_country_id  Int,
# MAGIC     mdm_holding_id  Int,
# MAGIC     mdm_banner_id Int,
# MAGIC     mdm_client_id Int,
# MAGIC     call_date Date
# MAGIC )
# MAGIC
# MAGIC USING delta
# MAGIC tblproperties (delta.autooptimize.optimizewrite = true, delta.autooptimize.autocompact = true)
# MAGIC LOCATION 'abfss://data@eus2psag2dpcoredatalake.dfs.core.windows.net/informationmart/acosta_retail_report/ds_intervention_summary'

# COMMAND ----------

# NOTE: I know this is not the most efficient
intervention_duration_map = df_interventions.select('response_id', 'duration').distinct().toPandas()
intervention_duration_map = dict(zip(
    intervention_duration_map['response_id'],
    intervention_duration_map['duration']
))
print(f'{len(intervention_duration_map):,} Interventions')

# COMMAND ----------

summary_table_schema = pyt.StructType([
    pyt.StructField('epos_organization_unit_num', pyt.StringType()),
    pyt.StructField('epos_retailer_item_id', pyt.StringType()),
    pyt.StructField('response_id', pyt.StringType()),
    pyt.StructField('standard_response_cd', pyt.StringType()),
    pyt.StructField('is_complete', pyt.BooleanType()),
    pyt.StructField('total_intervention_effect', pyt.FloatType()),
    pyt.StructField('total_qintervention_effect', pyt.FloatType()),
    pyt.StructField('total_impact', pyt.FloatType()),
    pyt.StructField('total_qimpact', pyt.FloatType()),
])
summary_table_cols = [col.name for col in summary_table_schema]


def compile_summary_table_udf(df: pd.DataFrame):
    # Required outputs
    response_id = df['response_id'].unique()[0]
    retailer_item_id = df['RETAILER_ITEM_ID'].unique()[0]
    organization_unit_num = df['ORGANIZATION_UNIT_NUM'].unique()[0]
    response_code = df['standard_response_cd'].unique()[0]
    total_intervention_effect = None
    total_qintervention_effect = None
    total_impact = None
    total_qimpact = None

    # Logic
    observed_diff_days = df['diff_day'].unique()
    is_complete = observed_diff_days.max() == intervention_duration_map[response_id]

    if is_complete:
        total_intervention_effect = df['INTERVENTION_EFFECT'].fillna(0).sum()
        total_qintervention_effect = df['QINTERVENTION_EFFECT'].fillna(0).sum()
        total_impact = df['INTERVENTION_VALUE'].fillna(0).sum()
        total_qimpact = (df['PRICE'].fillna(0) * df['QINTERVENTION_EFFECT'].fillna(0)).sum()

    # Results
    df_output = pd.DataFrame({
        'response_id': [response_id],
        'epos_retailer_item_id': [retailer_item_id],
        'epos_organization_unit_num': [organization_unit_num],
        'standard_response_cd': [response_code],
        'is_complete': [is_complete],
        'total_intervention_effect': [total_intervention_effect],
        'total_qintervention_effect': [total_qintervention_effect],
        'total_impact': [total_impact],
        'total_qimpact': [total_qimpact],
    })
    return df_output


# Join identifying information
df_summary_table = df_results \
    .groupby('response_id') \
    .applyInPandas(compile_summary_table_udf, schema=summary_table_schema)
print(f'{df_summary_table.cache().count():,} Interventions')

df_summary_table = df_summary_table.join(
    df_unique_interventions,
    on=['response_id', 'epos_organization_unit_num', 'epos_retailer_item_id'],
    how='left'
)

# Add essential columns
df_summary_table = df_summary_table.withColumn('mdm_country_id', pyf.lit(country_id))
df_summary_table = df_summary_table.withColumn('mdm_client_id', pyf.lit(client_id))
df_summary_table = df_summary_table.withColumn('mdm_holding_id', pyf.lit(holding_id))
df_summary_table = df_summary_table.withColumn('mdm_banner_id', pyf.lit(banner_id))
df_summary_table = df_summary_table.withColumn('load_ts', pyf.current_timestamp())

df_summary_table = df_summary_table.withColumnRenamed('duration', 'measurement_duration')

# Ensure column order
df_summary_table = df_summary_table.select(
    'mdm_country_nm', 'mdm_holding_nm', 'mdm_banner_nm', 'mdm_client_nm',
    'store_acosta_number', 'epos_organization_unit_num', 'epos_retailer_item_id',
    'objective_typ', 'call_id', 'response_id',
    'nars_response_text', 'standard_response_text', 'standard_response_cd',
    'measurement_duration', 'is_complete',
    'total_intervention_effect', 'total_qintervention_effect', 'total_impact', 'total_qimpact', 'load_ts',
    'mdm_country_id', 'mdm_holding_id', 'mdm_banner_id', 'mdm_client_id',
    'call_date'
)

print(f'{df_summary_table.cache().count():,} Interventions')

# COMMAND ----------

display(df_summary_table)

# COMMAND ----------

df_results \
    .write \
    .format('delta') \
    .mode('overwrite') \
    .save(f'/mnt/processed/measurement/results/{client_id}-{country_id}-{holding_id}-{banner_id}')

# COMMAND ----------

# Save without overwriting previously completed
completed_response_ids = spark.sql(f'''
    select response_id, is_complete
    from acosta_retail_analytics_im.ds_intervention_summary
    where is_complete = true
    and mdm_client_id = {client_id}
''')
completed_response_ids = completed_response_ids.select('response_id').distinct().toPandas()['response_id']
completed_response_ids = set(completed_response_ids)

# Filter out completed interventions
df_summary_table = df_summary_table.filter(~df_summary_table['response_id'].isin(completed_response_ids))

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from acosta_retail_analytics_im.ds_intervention_summary

# COMMAND ----------

# Save the new interventions data
df_summary_table = df_summary_table.alias('update')
delta_summary_table = DeltaTable\
    .forName(spark, 'acosta_retail_analytics_im.ds_intervention_summary')\
    .alias('source')

delta_summary_table.merge(
    df_summary_table,
    '''
    source.mdm_banner_id = update.mdm_banner_id and
    source.mdm_client_id = update.mdm_client_id and
    source.mdm_country_id = update.mdm_country_id and
    source.mdm_holding_id = update.mdm_holding_id and
    source.response_id = update.response_id and
    source.standard_response_text = update.standard_response_text
    '''
).whenMatchedUpdateAll()\
.whenNotMatchedInsertAll()\
.execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from acosta_retail_analytics_im.ds_intervention_summary

# COMMAND ----------

# MAGIC %md
# MAGIC # Extra Summary Statistics

# COMMAND ----------

df_results = df_results \
    .withColumn('resid', df_results['POS_ITEM_QTY'] - df_results['EXPECTED']) \
    .withColumn('abs_resid', pyf.abs(pyf.col('resid')))

intervention_effects = df_results.filter('INTERVENTION_EFFECT != 0') \
    .select('RETAILER_ITEM_ID', 'INTERVENTION_EFFECT') \
    .groupby('RETAILER_ITEM_ID') \
    .mean().toPandas()

summary = df_results \
    .agg(
    pyf.pow(pyf.stddev(pyf.col('POS_ITEM_QTY')), 2).alias('null_var'),
    pyf.pow(pyf.stddev(pyf.col('resid')), 2).alias('res_var'),
    pyf.mean('abs_resid').alias('MAE')
).toPandas()

# COMMAND ----------

print(summary)
print('Retailer Level Rsq =', 1 - summary['res_var'].values[0] / summary['null_var'].values[0])

# COMMAND ----------

summary = df_results.groupby('RETAILER_ITEM_ID').agg(
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
    df_results.groupby(pyf.year('SALES_DT')).agg(
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
    df_results.filter(
        'standard_response_cd != "none"'
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
agg_lift_results = df_results \
    .withColumn('YEAR', pyf.year('SALES_DT')) \
    .withColumn('LIFT_PERCENT', pyf.col('INTERVENTION_EFFECT') / pyf.col('BASELINE')) \
    .groupby('RETAILER_ITEM_ID', 'standard_response_cd', 'YEAR') \
    .agg(
        pyf.mean('POS_ITEM_QTY').alias('mean-pos'),
        pyf.mean('BASELINE').alias('mean-baseline'),
        pyf.mean('EXPECTED').alias('mean-expected'),
        pyf.mean('INTERVENTION_EFFECT').alias('mean-intervention-effect'),
        pyf.sum('BASELINE').alias('sum-baseline'),
        pyf.sum('INTERVENTION_EFFECT').alias('sum-intervention-effect')
    ) \
    .toPandas()

agg_lift_results['lift-percent'] = agg_lift_results['sum-intervention-effect'] / agg_lift_results['sum-baseline']

display(agg_lift_results)

# COMMAND ----------

display(
    df_results.withColumn('LIFT_PERCENT', pyf.col('INTERVENTION_EFFECT') / pyf.col('BASELINE')) \
        .select('RETAILER_ITEM_ID', 'standard_response_cd', 'POS_ITEM_QTY', 'BASELINE', 'EXPECTED',
                'INTERVENTION_EFFECT', 'LIFT_PERCENT') \
        .groupby('RETAILER_ITEM_ID', 'standard_response_cd') \
        .sum()
)
