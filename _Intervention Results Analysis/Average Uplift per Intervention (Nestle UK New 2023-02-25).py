# Databricks notebook source
from pprint import pprint

import numpy as np
import pandas as pd

import pyspark.sql.types as pyt
import pyspark.sql.functions as pyf

from acosta.measurement import required_columns, process_notebook_inputs

import acosta
from acosta.alerting.preprocessing import read_pos_data

print(acosta.__version__)

# COMMAND ----------

country_id = 30  # UK
client_id = 16320 # Nestle UK
holding_id = 3257 # AcostaRetailUK

# COMMAND ----------

banner_id = 7743  # Asda

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

# COMMAND ----------

df_results_test = spark.read.format('delta').load(
    f'/mnt/processed/measurement/results/{client_id}-{country_id}-{holding_id}-{banner_id}')

print(f'{df_results_test.cache().count():,} Rows in intervention results table')

# Join identifying information
df_summary_table_test = df_results_test \
    .groupby('response_id') \
    .applyInPandas(compile_summary_table_udf, schema=summary_table_schema)
print(f'{df_summary_table_test.cache().count():,} Interventions')

df_summary_table_test = df_summary_table_test.join(
    df_unique_interventions,
    on=['response_id', 'epos_organization_unit_num', 'epos_retailer_item_id'],
    how='left'
)

# Add essential columns
df_summary_table_test = df_summary_table_test.withColumn('mdm_country_id', pyf.lit(country_id))
df_summary_table_test = df_summary_table_test.withColumn('mdm_client_id', pyf.lit(client_id))
df_summary_table_test = df_summary_table_test.withColumn('mdm_holding_id', pyf.lit(holding_id))
df_summary_table_test = df_summary_table_test.withColumn('mdm_banner_id', pyf.lit(banner_id))
df_summary_table_test = df_summary_table_test.withColumn('load_ts', pyf.current_timestamp())

df_summary_table_test = df_summary_table_test.withColumnRenamed('duration', 'measurement_duration')

# Ensure column order
df_summary_table_test = df_summary_table_test.select(
    'mdm_country_nm', 'mdm_holding_nm', 'mdm_banner_nm', 'mdm_client_nm',
    'store_acosta_number', 'epos_organization_unit_num', 'epos_retailer_item_id',
    'objective_typ', 'call_id', 'response_id',
    'nars_response_text', 'standard_response_text', 'standard_response_cd',
    'measurement_duration', 'is_complete',
    'total_intervention_effect', 'total_qintervention_effect', 'total_impact', 'total_qimpact', 'load_ts',
    'mdm_country_id', 'mdm_holding_id', 'mdm_banner_id', 'mdm_client_id',
    'call_date'
)

print(df_summary_table_test.cache().count())

df_summary_table_test.coalesce(1).write.format('csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save(f'/mnt/artifacts/hugh/{client_id}-{country_id}-{holding_id}-{banner_id}-summary')

# COMMAND ----------

# Let's only look as 2022 interventions
df_summary_table_2022_test = df_summary_table_test.where((pyf.col("call_date") < pyf.lit("2023-01-01")) & (pyf.col("call_date") >= pyf.lit("2022-01-01")))

print(f'{df_summary_table_2022_test.cache().count():,} Interventions in 2022')

df_summary_table_2022_test_all = df_summary_table_2022_test

# COMMAND ----------

banner_id = 7744  # Morrisons

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

intervention_duration_map = df_interventions.select('response_id', 'duration').distinct().toPandas()
intervention_duration_map = dict(zip(
    intervention_duration_map['response_id'],
    intervention_duration_map['duration']
))
print(f'{len(intervention_duration_map):,} Interventions')

# COMMAND ----------

df_results_test = spark.read.format('delta').load(
    f'/mnt/processed/measurement/results/{client_id}-{country_id}-{holding_id}-{banner_id}')

print(f'{df_results_test.cache().count():,} Rows in intervention results table')

# Join identifying information
df_summary_table_test = df_results_test \
    .groupby('response_id') \
    .applyInPandas(compile_summary_table_udf, schema=summary_table_schema)
print(f'{df_summary_table_test.cache().count():,} Interventions')

df_summary_table_test = df_summary_table_test.join(
    df_unique_interventions,
    on=['response_id', 'epos_organization_unit_num', 'epos_retailer_item_id'],
    how='left'
)

# Add essential columns
df_summary_table_test = df_summary_table_test.withColumn('mdm_country_id', pyf.lit(country_id))
df_summary_table_test = df_summary_table_test.withColumn('mdm_client_id', pyf.lit(client_id))
df_summary_table_test = df_summary_table_test.withColumn('mdm_holding_id', pyf.lit(holding_id))
df_summary_table_test = df_summary_table_test.withColumn('mdm_banner_id', pyf.lit(banner_id))
df_summary_table_test = df_summary_table_test.withColumn('load_ts', pyf.current_timestamp())

df_summary_table_test = df_summary_table_test.withColumnRenamed('duration', 'measurement_duration')

# Ensure column order
df_summary_table_test = df_summary_table_test.select(
    'mdm_country_nm', 'mdm_holding_nm', 'mdm_banner_nm', 'mdm_client_nm',
    'store_acosta_number', 'epos_organization_unit_num', 'epos_retailer_item_id',
    'objective_typ', 'call_id', 'response_id',
    'nars_response_text', 'standard_response_text', 'standard_response_cd',
    'measurement_duration', 'is_complete',
    'total_intervention_effect', 'total_qintervention_effect', 'total_impact', 'total_qimpact', 'load_ts',
    'mdm_country_id', 'mdm_holding_id', 'mdm_banner_id', 'mdm_client_id',
    'call_date'
)

print(df_summary_table_test.cache().count())

df_summary_table_test.coalesce(1).write.format('csv')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save(f'/mnt/artifacts/hugh/{client_id}-{country_id}-{holding_id}-{banner_id}-summary')

# COMMAND ----------

# Let's only look as 2022 interventions
df_summary_table_2022_test = df_summary_table_test.where((pyf.col("call_date") < pyf.lit("2023-01-01")) & (pyf.col("call_date") >= pyf.lit("2022-01-01")))

print(f'{df_summary_table_2022_test.cache().count():,} Interventions in 2022')

df_summary_table_2022_test_all = df_summary_table_2022_test_all.union(df_summary_table_2022_test)

# COMMAND ----------

banner_id = 7745  # Sainsburys

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

intervention_duration_map = df_interventions.select('response_id', 'duration').distinct().toPandas()
intervention_duration_map = dict(zip(
    intervention_duration_map['response_id'],
    intervention_duration_map['duration']
))
print(f'{len(intervention_duration_map):,} Interventions')

# COMMAND ----------

df_results_test = spark.read.format('delta').load(
    f'/mnt/processed/measurement/results/{client_id}-{country_id}-{holding_id}-{banner_id}')

print(f'{df_results_test.cache().count():,} Rows in intervention results table')

# Join identifying information
df_summary_table_test = df_results_test \
    .groupby('response_id') \
    .applyInPandas(compile_summary_table_udf, schema=summary_table_schema)
print(f'{df_summary_table_test.cache().count():,} Interventions')

df_summary_table_test = df_summary_table_test.join(
    df_unique_interventions,
    on=['response_id', 'epos_organization_unit_num', 'epos_retailer_item_id'],
    how='left'
)

# Add essential columns
df_summary_table_test = df_summary_table_test.withColumn('mdm_country_id', pyf.lit(country_id))
df_summary_table_test = df_summary_table_test.withColumn('mdm_client_id', pyf.lit(client_id))
df_summary_table_test = df_summary_table_test.withColumn('mdm_holding_id', pyf.lit(holding_id))
df_summary_table_test = df_summary_table_test.withColumn('mdm_banner_id', pyf.lit(banner_id))
df_summary_table_test = df_summary_table_test.withColumn('load_ts', pyf.current_timestamp())

df_summary_table_test = df_summary_table_test.withColumnRenamed('duration', 'measurement_duration')

# Ensure column order
df_summary_table_test = df_summary_table_test.select(
    'mdm_country_nm', 'mdm_holding_nm', 'mdm_banner_nm', 'mdm_client_nm',
    'store_acosta_number', 'epos_organization_unit_num', 'epos_retailer_item_id',
    'objective_typ', 'call_id', 'response_id',
    'nars_response_text', 'standard_response_text', 'standard_response_cd',
    'measurement_duration', 'is_complete',
    'total_intervention_effect', 'total_qintervention_effect', 'total_impact', 'total_qimpact', 'load_ts',
    'mdm_country_id', 'mdm_holding_id', 'mdm_banner_id', 'mdm_client_id',
    'call_date'
)

# Let's only look as 2022 interventions
df_summary_table_2022_test = df_summary_table_test.where((pyf.col("call_date") < pyf.lit("2023-01-01")) & (pyf.col("call_date") >= pyf.lit("2022-01-01")))

print(f'{df_summary_table_2022_test.cache().count():,} Interventions in 2022')

df_summary_table_2022_test_all = df_summary_table_2022_test_all.union(df_summary_table_2022_test)

# COMMAND ----------

banner_id = 7746  # Tesco

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

intervention_duration_map = df_interventions.select('response_id', 'duration').distinct().toPandas()
intervention_duration_map = dict(zip(
    intervention_duration_map['response_id'],
    intervention_duration_map['duration']
))
print(f'{len(intervention_duration_map):,} Interventions')

# COMMAND ----------

df_results_test = spark.read.format('delta').load(
    f'/mnt/processed/measurement/results/{client_id}-{country_id}-{holding_id}-{banner_id}')

print(f'{df_results_test.cache().count():,} Rows in intervention results table')

# Join identifying information
df_summary_table_test = df_results_test \
    .groupby('response_id') \
    .applyInPandas(compile_summary_table_udf, schema=summary_table_schema)
print(f'{df_summary_table_test.cache().count():,} Interventions')

df_summary_table_test = df_summary_table_test.join(
    df_unique_interventions,
    on=['response_id', 'epos_organization_unit_num', 'epos_retailer_item_id'],
    how='left'
)

# Add essential columns
df_summary_table_test = df_summary_table_test.withColumn('mdm_country_id', pyf.lit(country_id))
df_summary_table_test = df_summary_table_test.withColumn('mdm_client_id', pyf.lit(client_id))
df_summary_table_test = df_summary_table_test.withColumn('mdm_holding_id', pyf.lit(holding_id))
df_summary_table_test = df_summary_table_test.withColumn('mdm_banner_id', pyf.lit(banner_id))
df_summary_table_test = df_summary_table_test.withColumn('load_ts', pyf.current_timestamp())

df_summary_table_test = df_summary_table_test.withColumnRenamed('duration', 'measurement_duration')

# Ensure column order
df_summary_table_test = df_summary_table_test.select(
    'mdm_country_nm', 'mdm_holding_nm', 'mdm_banner_nm', 'mdm_client_nm',
    'store_acosta_number', 'epos_organization_unit_num', 'epos_retailer_item_id',
    'objective_typ', 'call_id', 'response_id',
    'nars_response_text', 'standard_response_text', 'standard_response_cd',
    'measurement_duration', 'is_complete',
    'total_intervention_effect', 'total_qintervention_effect', 'total_impact', 'total_qimpact', 'load_ts',
    'mdm_country_id', 'mdm_holding_id', 'mdm_banner_id', 'mdm_client_id',
    'call_date'
)

# Let's only look as 2022 interventions
df_summary_table_2022_test = df_summary_table_test.where((pyf.col("call_date") < pyf.lit("2023-01-01")) & (pyf.col("call_date") >= pyf.lit("2022-01-01")))

print(f'{df_summary_table_2022_test.cache().count():,} Interventions in 2022')

df_summary_table_2022_test_all = df_summary_table_2022_test_all.union(df_summary_table_2022_test)

# COMMAND ----------

df_summary_table_2022_test_all.createOrReplaceTempView("ds_intervention_summary_nestlecore_uk_2022_test")

# COMMAND ----------

sql_number_of_interventions_by_retailer_and_call_year = """
SELECT mdm_banner_nm, substr(call_date, 1, 4) AS call_year_new, count(total_impact) as num_interventions FROM ds_intervention_summary_nestlecore_uk_2022_test
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
(
  (call_date >= '2022-01-01'AND
  call_date <= '2022-12-31')   
)
group by 
mdm_banner_nm, call_year_new
ORDER BY
mdm_banner_nm, call_year_new
"""

df_number_of_interventions_by_retailer_and_call_year = spark.sql(sql_number_of_interventions_by_retailer_and_call_year).cache()

pivot_df_number_of_interventions_by_retailer_and_call_year = df_number_of_interventions_by_retailer_and_call_year.groupBy("call_year_new").pivot("mdm_banner_nm").sum("num_interventions")

display(pivot_df_number_of_interventions_by_retailer_and_call_year)

# COMMAND ----------

sql_number_of_interventions_by_retailer_and_call_period = """
SELECT mdm_banner_nm, 
case
  when call_date < '2022-09-21' then ' Pre-360 Launch'
  else 'Post-360 Launch'
end as call_period_new,
count(total_impact) as num_interventions FROM ds_intervention_summary_nestlecore_uk_2022_test
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
(
  (call_date >= '2022-01-01' AND
  call_date <= '2022-12-31')   
)
group by 
mdm_banner_nm, call_period_new
ORDER BY
mdm_banner_nm, call_period_new
"""

df_number_of_interventions_by_retailer_and_call_period = spark.sql(sql_number_of_interventions_by_retailer_and_call_period).cache()

pivot_df_number_of_interventions_by_retailer_and_call_period = df_number_of_interventions_by_retailer_and_call_period.groupBy("call_period_new").pivot("mdm_banner_nm").sum("num_interventions").orderBy("call_period_new")

display(pivot_df_number_of_interventions_by_retailer_and_call_period)

# COMMAND ----------

sql_number_of_interventions_by_retailer_and_call_month = """
SELECT mdm_banner_nm, substr(call_date, 1, 7) AS call_month_new, count(total_impact) as num_interventions FROM ds_intervention_summary_nestlecore_uk_2022_test
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
(
  (call_date >= '2022-01-01'AND
  call_date <= '2022-12-31')   
)
group by 
mdm_banner_nm, call_month_new
ORDER BY
mdm_banner_nm, call_month_new
"""

df_number_of_interventions_by_retailer_and_call_month = spark.sql(sql_number_of_interventions_by_retailer_and_call_month).cache()

pivot_df_number_of_interventions_by_retailer_and_call_month = df_number_of_interventions_by_retailer_and_call_month.groupBy("call_month_new").pivot("mdm_banner_nm").sum("num_interventions").orderBy("call_month_new")

display(pivot_df_number_of_interventions_by_retailer_and_call_month)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_by_retailer_and_call_year = """
SELECT mdm_banner_nm, substr(call_date, 1, 4) AS call_year_new, SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM ds_intervention_summary_nestlecore_uk_2022_test
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
(
  (call_date >= '2022-01-01'AND
  call_date <= '2022-12-31')   
)
group by 
mdm_banner_nm, call_year_new
ORDER BY
mdm_banner_nm, call_year_new
"""

df_average_sales_uplift_per_measured_intervention_by_retailer_and_call_year = spark.sql(sql_average_sales_uplift_per_measured_intervention_by_retailer_and_call_year).cache()

pivot_df_average_sales_uplift_per_measured_intervention_by_retailer_and_call_year = df_average_sales_uplift_per_measured_intervention_by_retailer_and_call_year.groupBy("call_year_new").pivot("mdm_banner_nm").sum("average_sales_uplift_per_measured_intervention")

display(pivot_df_average_sales_uplift_per_measured_intervention_by_retailer_and_call_year)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_by_call_period = """
SELECT
case
  when call_date < '2022-09-21' then ' Pre-360 Launch'
  else 'Post-360 Launch'
end as call_period_new,
SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM ds_intervention_summary_nestlecore_uk_2022_test
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
(
  call_date >= '2022-01-01' AND
  call_date <= '2022-12-31'   
)
group by 
call_period_new
ORDER BY
call_period_new
"""

df_average_sales_uplift_per_measured_intervention_by_call_period = spark.sql(sql_average_sales_uplift_per_measured_intervention_by_call_period).cache()

display(df_average_sales_uplift_per_measured_intervention_by_call_period)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_by_retailer_and_call_period = """
SELECT mdm_banner_nm, 
case
  when call_date < '2022-09-21' then ' Pre-360 Launch'
  else 'Post-360 Launch'
end as call_period_new,
SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM ds_intervention_summary_nestlecore_uk_2022_test
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
(
  call_date >= '2022-01-01' AND
  call_date <= '2022-12-31'   
)
group by 
mdm_banner_nm, call_period_new
ORDER BY
mdm_banner_nm, call_period_new
"""

df_average_sales_uplift_per_measured_intervention_by_retailer_and_call_period = spark.sql(sql_average_sales_uplift_per_measured_intervention_by_retailer_and_call_period).cache()

pivot_df_average_sales_uplift_per_measured_intervention_by_retailer_and_call_period = df_average_sales_uplift_per_measured_intervention_by_retailer_and_call_period.groupBy("call_period_new").pivot("mdm_banner_nm").sum("average_sales_uplift_per_measured_intervention").orderBy("call_period_new")

display(pivot_df_average_sales_uplift_per_measured_intervention_by_retailer_and_call_period)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_by_retailer_and_call_month = """
SELECT mdm_banner_nm, substr(call_date, 1, 7) AS call_month_new, SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM ds_intervention_summary_nestlecore_uk_2022_test
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
(
  call_date >= '2022-01-01' AND
  call_date <= '2022-12-31'   
)
group by 
mdm_banner_nm, call_month_new
ORDER BY
mdm_banner_nm, call_month_new
"""

df_average_sales_uplift_per_measured_intervention_by_retailer_and_call_month = spark.sql(sql_average_sales_uplift_per_measured_intervention_by_retailer_and_call_month).cache()

pivot_df_average_sales_uplift_per_measured_intervention_by_retailer_and_call_month = df_average_sales_uplift_per_measured_intervention_by_retailer_and_call_month.groupBy("call_month_new").pivot("mdm_banner_nm").sum("average_sales_uplift_per_measured_intervention").orderBy("call_month_new")

display(pivot_df_average_sales_uplift_per_measured_intervention_by_retailer_and_call_month)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_asda_by_call_year_and_standard_response_cd = """
SELECT substr(call_date, 1, 4) AS asda_call_year_new, standard_response_cd, SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM ds_intervention_summary_nestlecore_uk_2022_test
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_banner_id = 7743 -- Asda
and
(
  (call_date >= '2022-01-01'AND
  call_date <= '2022-12-31')   
)
group by 
asda_call_year_new, standard_response_cd
ORDER BY
asda_call_year_new, standard_response_cd
"""

df_average_sales_uplift_per_measured_intervention_asda_by_call_year_and_standard_response_cd = spark.sql(sql_average_sales_uplift_per_measured_intervention_asda_by_call_year_and_standard_response_cd).cache()

pivot_df_average_sales_uplift_per_measured_intervention_asda_by_call_year_and_standard_response_cd = df_average_sales_uplift_per_measured_intervention_asda_by_call_year_and_standard_response_cd.groupBy("asda_call_year_new").pivot("standard_response_cd").sum("average_sales_uplift_per_measured_intervention")

display(pivot_df_average_sales_uplift_per_measured_intervention_asda_by_call_year_and_standard_response_cd)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_asda_by_call_period_and_standard_response_cd = """
SELECT
case
  when call_date < '2022-09-21' then ' Pre-360 Launch'
  else 'Post-360 Launch'
end as asda_call_period_new,
standard_response_cd,
SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM ds_intervention_summary_nestlecore_uk_2022_test
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_banner_id = 7743 -- Asda
and
(
  (call_date >= '2022-01-01'AND
  call_date <= '2022-12-31')   
)
group by 
asda_call_period_new, standard_response_cd
ORDER BY
asda_call_period_new, standard_response_cd
"""

df_average_sales_uplift_per_measured_intervention_asda_by_call_period_and_standard_response_cd = spark.sql(sql_average_sales_uplift_per_measured_intervention_asda_by_call_period_and_standard_response_cd).cache()

pivot_df_average_sales_uplift_per_measured_intervention_asda_by_call_period_and_standard_response_cd = df_average_sales_uplift_per_measured_intervention_asda_by_call_period_and_standard_response_cd.groupBy("asda_call_period_new").pivot("standard_response_cd").sum("average_sales_uplift_per_measured_intervention").orderBy("asda_call_period_new")

display(pivot_df_average_sales_uplift_per_measured_intervention_asda_by_call_period_and_standard_response_cd)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_asda_by_call_month_and_standard_response_cd = """
SELECT substr(call_date, 1, 7) AS asda_call_month_new, standard_response_cd, SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM ds_intervention_summary_nestlecore_uk_2022_test
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_banner_id = 7743 -- Asda
and
(
  (call_date >= '2022-01-01' AND
  call_date <= '2022-12-31')   
)
group by 
asda_call_month_new, standard_response_cd
ORDER BY
asda_call_month_new, standard_response_cd
"""

df_average_sales_uplift_per_measured_intervention_asda_by_call_month_and_standard_response_cd = spark.sql(sql_average_sales_uplift_per_measured_intervention_asda_by_call_month_and_standard_response_cd).cache()

pivot_df_average_sales_uplift_per_measured_intervention_asda_by_call_month_and_standard_response_cd = df_average_sales_uplift_per_measured_intervention_asda_by_call_month_and_standard_response_cd.groupBy("asda_call_month_new").pivot("standard_response_cd").sum("average_sales_uplift_per_measured_intervention").orderBy("asda_call_month_new")

display(pivot_df_average_sales_uplift_per_measured_intervention_asda_by_call_month_and_standard_response_cd)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_morrisons_by_call_year_and_standard_response_cd = """
SELECT substr(call_date, 1, 4) AS morrisons_call_year_new, standard_response_cd, SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM ds_intervention_summary_nestlecore_uk_2022_test
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_banner_id = 7744 -- Morrisons
and
(
  (call_date >= '2022-01-01'AND
  call_date <= '2022-12-31')   
)
group by 
morrisons_call_year_new, standard_response_cd
ORDER BY
morrisons_call_year_new, standard_response_cd
"""

df_average_sales_uplift_per_measured_intervention_morrisons_by_call_year_and_standard_response_cd = spark.sql(sql_average_sales_uplift_per_measured_intervention_morrisons_by_call_year_and_standard_response_cd).cache()

pivot_df_average_sales_uplift_per_measured_intervention_morrisons_by_call_year_and_standard_response_cd = df_average_sales_uplift_per_measured_intervention_morrisons_by_call_year_and_standard_response_cd.groupBy("morrisons_call_year_new").pivot("standard_response_cd").sum("average_sales_uplift_per_measured_intervention")

display(pivot_df_average_sales_uplift_per_measured_intervention_morrisons_by_call_year_and_standard_response_cd)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_morrisons_by_call_period_and_standard_response_cd = """
SELECT
case
  when call_date < '2022-09-21' then ' Pre-360 Launch'
  else 'Post-360 Launch'
end as morrisons_call_period_new,
standard_response_cd,
SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM ds_intervention_summary_nestlecore_uk_2022_test
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_banner_id = 7744 -- Morrisons
and
(
  (call_date >= '2022-01-01'AND
  call_date <= '2022-12-31')   
)
group by 
morrisons_call_period_new, standard_response_cd
ORDER BY
morrisons_call_period_new, standard_response_cd
"""

df_average_sales_uplift_per_measured_intervention_morrisons_by_call_period_and_standard_response_cd = spark.sql(sql_average_sales_uplift_per_measured_intervention_morrisons_by_call_period_and_standard_response_cd).cache()

pivot_df_average_sales_uplift_per_measured_intervention_morrisons_by_call_period_and_standard_response_cd = df_average_sales_uplift_per_measured_intervention_morrisons_by_call_period_and_standard_response_cd.groupBy("morrisons_call_period_new").pivot("standard_response_cd").sum("average_sales_uplift_per_measured_intervention").orderBy("morrisons_call_period_new")

display(pivot_df_average_sales_uplift_per_measured_intervention_morrisons_by_call_period_and_standard_response_cd)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_morrisons_by_call_month_and_standard_response_cd = """
SELECT substr(call_date, 1, 7) AS morrisons_call_mon, standard_response_cd, SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM ds_intervention_summary_nestlecore_uk_2022_test
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_banner_id = 7744 -- Morrisons
and
(
  (call_date >= '2022-01-01' AND
  call_date <= '2022-12-31')   
)
group by 
morrisons_call_mon, standard_response_cd
ORDER BY
morrisons_call_mon, standard_response_cd
"""

df_average_sales_uplift_per_measured_intervention_morrisons_by_call_month_and_standard_response_cd = spark.sql(sql_average_sales_uplift_per_measured_intervention_morrisons_by_call_month_and_standard_response_cd).cache()

pivot_df_average_sales_uplift_per_measured_intervention_morrisons_by_call_month_and_standard_response_cd = df_average_sales_uplift_per_measured_intervention_morrisons_by_call_month_and_standard_response_cd.groupBy("morrisons_call_mon").pivot("standard_response_cd").sum("average_sales_uplift_per_measured_intervention").orderBy("morrisons_call_mon")

display(pivot_df_average_sales_uplift_per_measured_intervention_morrisons_by_call_month_and_standard_response_cd)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_sainsburys_by_call_year_and_standard_response_cd = """
SELECT substr(call_date, 1, 4) AS sainsburys_call_year, standard_response_cd, SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM ds_intervention_summary_nestlecore_uk_2022_test
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_banner_id = 7745 -- Sainsburys
and
(
  (call_date >= '2022-01-01'AND
  call_date <= '2022-12-31')   
)
group by 
sainsburys_call_year, standard_response_cd
ORDER BY
sainsburys_call_year, standard_response_cd
"""

df_average_sales_uplift_per_measured_intervention_sainsburys_by_call_year_and_standard_response_cd = spark.sql(sql_average_sales_uplift_per_measured_intervention_sainsburys_by_call_year_and_standard_response_cd).cache()

pivot_df_average_sales_uplift_per_measured_intervention_sainsburys_by_call_year_and_standard_response_cd = df_average_sales_uplift_per_measured_intervention_sainsburys_by_call_year_and_standard_response_cd.groupBy("sainsburys_call_year").pivot("standard_response_cd").sum("average_sales_uplift_per_measured_intervention")

display(pivot_df_average_sales_uplift_per_measured_intervention_sainsburys_by_call_year_and_standard_response_cd)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_sainsburys_by_call_period_and_standard_response_cd = """
SELECT
case
  when call_date < '2022-09-21' then ' Pre-360 Launch'
  else 'Post-360 Launch'
end as sainsburys_call_period,
standard_response_cd,
SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM ds_intervention_summary_nestlecore_uk_2022_test
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_banner_id = 7745 -- Sainsburys
and
(
  (call_date >= '2022-01-01'AND
  call_date <= '2022-12-31')   
)
group by 
sainsburys_call_period, standard_response_cd
ORDER BY
sainsburys_call_period, standard_response_cd
"""

df_average_sales_uplift_per_measured_intervention_sainsburys_by_call_period_and_standard_response_cd = spark.sql(sql_average_sales_uplift_per_measured_intervention_sainsburys_by_call_period_and_standard_response_cd).cache()

pivot_df_average_sales_uplift_per_measured_intervention_sainsburys_by_call_period_and_standard_response_cd = df_average_sales_uplift_per_measured_intervention_sainsburys_by_call_period_and_standard_response_cd.groupBy("sainsburys_call_period").pivot("standard_response_cd").sum("average_sales_uplift_per_measured_intervention").orderBy("sainsburys_call_period")

display(pivot_df_average_sales_uplift_per_measured_intervention_sainsburys_by_call_period_and_standard_response_cd)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_sainsburys_by_call_month_and_standard_response_cd = """
SELECT substr(call_date, 1, 7) AS sainsburys_call_month, standard_response_cd, SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM ds_intervention_summary_nestlecore_uk_2022_test
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_banner_id = 7745 -- Sainsburys
and
(
  (call_date >= '2022-01-01' AND
  call_date <= '2022-12-31')   
)
group by 
sainsburys_call_month, standard_response_cd
ORDER BY
sainsburys_call_month, standard_response_cd
"""

df_average_sales_uplift_per_measured_intervention_sainsburys_by_call_month_and_standard_response_cd = spark.sql(sql_average_sales_uplift_per_measured_intervention_sainsburys_by_call_month_and_standard_response_cd).cache()

pivot_df_average_sales_uplift_per_measured_intervention_sainsburys_by_call_month_and_standard_response_cd = df_average_sales_uplift_per_measured_intervention_sainsburys_by_call_month_and_standard_response_cd.groupBy("sainsburys_call_month").pivot("standard_response_cd").sum("average_sales_uplift_per_measured_intervention").orderBy("sainsburys_call_month")

display(pivot_df_average_sales_uplift_per_measured_intervention_sainsburys_by_call_month_and_standard_response_cd)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_tesco_by_call_year_and_standard_response_cd = """
SELECT substr(call_date, 1, 4) AS tesco_call_year_new, standard_response_cd, SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM ds_intervention_summary_nestlecore_uk_2022_test
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_banner_id = 7746 -- Tesco
and
(
  (call_date >= '2022-01-01'AND
  call_date <= '2022-12-31')   
)
group by 
tesco_call_year_new, standard_response_cd
ORDER BY
tesco_call_year_new, standard_response_cd
"""

df_average_sales_uplift_per_measured_intervention_tesco_by_call_year_and_standard_response_cd = spark.sql(sql_average_sales_uplift_per_measured_intervention_tesco_by_call_year_and_standard_response_cd).cache()

pivot_df_average_sales_uplift_per_measured_intervention_tesco_by_call_year_and_standard_response_cd = df_average_sales_uplift_per_measured_intervention_tesco_by_call_year_and_standard_response_cd.groupBy("tesco_call_year_new").pivot("standard_response_cd").sum("average_sales_uplift_per_measured_intervention")

display(pivot_df_average_sales_uplift_per_measured_intervention_tesco_by_call_year_and_standard_response_cd)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_tesco_by_call_period_and_standard_response_cd = """
SELECT
case
  when call_date < '2022-09-21' then ' Pre-360 Launch'
  else 'Post-360 Launch'
end as tesco_call_period_new,
standard_response_cd,
SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM ds_intervention_summary_nestlecore_uk_2022_test
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_banner_id = 7746 -- Tesco
and
(
  (call_date >= '2022-01-01'AND
  call_date <= '2022-12-31')   
)
group by 
tesco_call_period_new, standard_response_cd
ORDER BY
tesco_call_period_new, standard_response_cd
"""

df_average_sales_uplift_per_measured_intervention_tesco_by_call_period_and_standard_response_cd = spark.sql(sql_average_sales_uplift_per_measured_intervention_tesco_by_call_period_and_standard_response_cd).cache()

pivot_df_average_sales_uplift_per_measured_intervention_tesco_by_call_period_and_standard_response_cd = df_average_sales_uplift_per_measured_intervention_tesco_by_call_period_and_standard_response_cd.groupBy("tesco_call_period_new").pivot("standard_response_cd").sum("average_sales_uplift_per_measured_intervention").orderBy("tesco_call_period_new")

display(pivot_df_average_sales_uplift_per_measured_intervention_tesco_by_call_period_and_standard_response_cd)

# COMMAND ----------

sql_average_sales_uplift_per_measured_intervention_tesco_by_call_month_and_standard_response_cd = """
SELECT substr(call_date, 1, 7) AS tesco_call_month_new, standard_response_cd, SUM(total_impact) / count(total_impact) as average_sales_uplift_per_measured_intervention FROM ds_intervention_summary_nestlecore_uk_2022_test
where
mdm_country_id = 30 -- UK
and
mdm_client_id = 16320 -- Nestle UK (there are 4 retailers, Asda, Morrisons, Sainsburys, and Tesco)
and
mdm_holding_id = 3257 -- AcostaRetailUK
and
mdm_banner_id = 7746 -- Tesco
and
(
  (call_date >= '2022-01-01' AND
  call_date <= '2022-12-31')   
)
group by 
tesco_call_month_new, standard_response_cd
ORDER BY
tesco_call_month_new, standard_response_cd
"""

df_average_sales_uplift_per_measured_intervention_tesco_by_call_month_and_standard_response_cd = spark.sql(sql_average_sales_uplift_per_measured_intervention_tesco_by_call_month_and_standard_response_cd).cache()

pivot_df_average_sales_uplift_per_measured_intervention_tesco_by_call_month_and_standard_response_cd = df_average_sales_uplift_per_measured_intervention_tesco_by_call_month_and_standard_response_cd.groupBy("tesco_call_month_new").pivot("standard_response_cd").sum("average_sales_uplift_per_measured_intervention").orderBy("tesco_call_month_new")

display(pivot_df_average_sales_uplift_per_measured_intervention_tesco_by_call_month_and_standard_response_cd)

# COMMAND ----------


