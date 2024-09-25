# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime

# COMMAND ----------

# schema = StructType([
#     StructField('CLIENT', StringType(), False),\
#     StructField('RETAILER', StringType(), False),\
#     StructField('ORGANIZATION_UNIT_NUM', IntegerType(), False),\
#     StructField('RETAILER_ITEM_ID', StringType(), False),\
#     StructField('BASELINE_POS_ITEM_QTY', DoubleType(), False),\
#     StructField('SALES_DT', DateType(), False)])

# COMMAND ----------

dbutils.widgets.text('retailer', 'asda', 'Retailer')
dbutils.widgets.text('client', 'kraftheinz', 'Client')
dbutils.widgets.text('countrycode', 'uk', 'Country Code')

# COMMAND ----------

RETAILER = dbutils.widgets.get('retailer').strip().lower()
CLIENT = dbutils.widgets.get('client').strip().lower()
COUNTRY_CODE = dbutils.widgets.get('countrycode').strip().lower()

# COMMAND ----------

# PATHS
PATH_CAUSAL_RESULTS = '/mnt/processed/causal_results/retailer={retailer}/client={client}/country_code={country_code}/'.format(
    retailer=RETAILER,
    client=CLIENT,
    country_code=COUNTRY_CODE
)

data_vault_data = spark.read.format('delta').load(PATH_CAUSAL_RESULTS)
print(data_vault_data.dtypes)

# COMMAND ----------

# # Save the data set
# data2.coalesce(1) \
#     .write.mode('overwrite').format('csv').option('header', 'true') \
#     .save('/mnt/artifacts/hugh/sainsburys-kraftheinz-uk-20190301-20190630')

# data_vault_data = spark.read.format('csv')\
#     .options(header='true', inferSchema='true')\
#     .load('/mnt/artifacts/hugh/tesco-kraftheinz-uk-20190401-20190430')
  
# print(data_vault_data.dtypes)

# COMMAND ----------

data_vault_data.columns

# COMMAND ----------

data_vault_data.dtypes

# COMMAND ----------

# df1 = df.selectExpr('CLIENT',
#                     'RETAILER',
#                     'ORGANIZATION_UNIT_NUM',
#                     'RETAILER_ITEM_ID',
#                     'BASELINE_POS_ITEM_QTY',
#                     'SALES_DT')

# COMMAND ----------

data_vault_data.count()

# COMMAND ----------

data_vault_data = data_vault_data.withColumn('RETAILER', lit(RETAILER))
data_vault_data = data_vault_data.withColumn('CLIENT', lit(CLIENT))
data_vault_data = data_vault_data.withColumn('COUNTRY_CODE', lit(COUNTRY_CODE))

# COMMAND ----------

display(data_vault_data)

# COMMAND ----------

data_vault_data2 = data_vault_data.where(col("INTERVENTION_TYPE") != 'None')
# data_vault_data2 = data_vault_data
data_vault_data2 = data_vault_data2.where(col("RETAILER_ITEM_ID") == 51169241)

# COMMAND ----------

# data_vault_data2.count()

# COMMAND ----------

data_vault_data_june2019 = data_vault_data.where((col("SALES_DT") <= lit("2019-06-30")) & (col("SALES_DT") >= lit("2019-06-01")))
data_vault_data2_june2019 = data_vault_data2.where((col("SALES_DT") <= lit("2019-06-30")) & (col("SALES_DT") >= lit("2019-06-01")))

# COMMAND ----------

# data_vault_data_june2019.count()

# COMMAND ----------

df_list_of_all_stores = data_vault_data_june2019.select('ORGANIZATION_UNIT_NUM').distinct()
df_list_of_covered_stores = data_vault_data2_june2019.select('ORGANIZATION_UNIT_NUM').distinct()

# COMMAND ----------

df_list_of_all_stores.count()

# COMMAND ----------

df_list_of_covered_stores.count()

# COMMAND ----------

display(df_list_of_covered_stores)

# COMMAND ----------

display(df_list_of_uncovered_stores.where(col("ORGANIZATION_UNIT_NUM") == 3036))

# COMMAND ----------

df_list_of_uncovered_stores = df_list_of_all_stores.join(df_list_of_covered_stores, "ORGANIZATION_UNIT_NUM", how='left_anti')

# COMMAND ----------

df_list_of_uncovered_stores.count()

# COMMAND ----------

display(datajune2019)

# COMMAND ----------

# data_vault_data2 = data_vault_data2.withColumn('INTERVENTION_DATE', expr("date_sub(SALES_DT, DIFF_DAY)"))
# data1 = data_vault_data2.where((col("INTERVENTION_DATE") <= lit("2019-04-30")) & (col("INTERVENTION_DATE") >= lit("2019-04-01")))

# data1 = data_vault_data2.where((col("SALES_DT") <= lit("2019-05-21")) & (col("SALES_DT") >= lit("2019-04-22")))
data1 = data_vault_data2.where((col("SALES_DT") <= lit("2019-06-30")) & (col("SALES_DT") >= lit("2019-06-01")))

# COMMAND ----------

data1.count()

# COMMAND ----------

display(data1)

# COMMAND ----------

agg_data = data1.groupBy('RETAILER', 
                                      'CLIENT',
                                      'COUNTRY_CODE',
                                      'RETAILER_ITEM_ID',
                                      'ORGANIZATION_UNIT_NUM'
                                     ).agg(count("POS_ITEM_QTY").alias('TOTAL_DAYS'))

# COMMAND ----------

display(agg_data)

# COMMAND ----------

agg_data_filter = agg_data.where(col("TOTAL_DAYS") >= 30).select('ORGANIZATION_UNIT_NUM')

# COMMAND ----------

agg_data_filter.count()

# COMMAND ----------

display(agg_data_filter)

# COMMAND ----------

data1_no_interventions = data1.join(agg_data_filter, "ORGANIZATION_UNIT_NUM")

# COMMAND ----------

display(data1_no_interventions)

# COMMAND ----------

agg_data1_no_interventions = data1_no_interventions.groupBy('RETAILER', 
                                      'CLIENT',
                                      'COUNTRY_CODE',
                                      'RETAILER_ITEM_ID',
                                      'SALES_DT',
                                      'INTERVENTION_TYPE'
                                     ).agg(avg("POS_ITEM_QTY").alias('AVG_POS_ITEM_QTY'), 
                                           avg("BASELINE").alias('AVG_BASELINE'),
                                           avg("EXPECTED").alias('AVG_EXPECTED'),
                                           avg("INTERVENTION_EFFECT").alias('AVG_INTERVENTION_EFFECT'),
                                           sum("POS_ITEM_QTY").alias('TOTAL_POS_ITEM_QTY'), 
                                           sum("BASELINE").alias('TOTAL_BASELINE'),
                                           sum("EXPECTED").alias('TOTAL_EXPECTED'),
                                           sum("INTERVENTION_EFFECT").alias('TOTAL_INTERVENTION_EFFECT'))

# COMMAND ----------

display(agg_data1_no_interventions)

# COMMAND ----------

agg_data1_no_interventions.count()

# COMMAND ----------

# # Save the data set
# data1.coalesce(1) \
#     .write.mode('overwrite').format('csv').option('header', 'true') \
#     .save('/mnt/artifacts/hugh/sainsburys-kraftheinz-uk-20190401-20190430')

# COMMAND ----------

# # Save the data set
# data1.coalesce(1) \
#     .write.mode('overwrite').format('csv').option('header', 'true') \
#     .save('/mnt/artifacts/hugh/asda-kraftheinz-uk-20190401-20190430')

# COMMAND ----------

# # Save the data set
# data1.coalesce(1) \
#     .write.mode('overwrite').format('csv').option('header', 'true') \
#     .save('/mnt/artifacts/hugh/morrisons-kraftheinz-uk-20190401-20190430')

# COMMAND ----------

# # Save the data set
# data1.coalesce(1) \
#     .write.mode('overwrite').format('csv').option('header', 'true') \
#     .save('/mnt/artifacts/hugh/tesco-kraftheinz-uk-20190401-20190430')

# COMMAND ----------

# # Save the data set
# data2.coalesce(1) \
#     .write.mode('overwrite').format('csv').option('header', 'true') \
#     .save('/mnt/artifacts/hugh/asda-kraftheinz-uk-20190301-20190630')

# COMMAND ----------

# # Save the data set
# data2.coalesce(1) \
#     .write.mode('overwrite').format('csv').option('header', 'true') \
#     .save('/mnt/artifacts/hugh/morrisons-kraftheinz-uk-20190301-20190630')

# COMMAND ----------

# # Save the data set
# data2.coalesce(1) \
#     .write.mode('overwrite').format('csv').option('header', 'true') \
#     .save('/mnt/artifacts/hugh/sainsburys-kraftheinz-uk-20190301-20190630')

# COMMAND ----------

# # Save the data set
# data1.coalesce(1) \
#     .write.mode('overwrite').format('csv').option('header', 'true') \
#     .save('/mnt/artifacts/hugh/tesco-kraftheinz-uk-spotchecks-book-stock-error')

# COMMAND ----------

data_vault_data3 = data_vault_data.where(col("INTERVENTION_TYPE") == 'Promotional Price Sites - Including SEL, Beanies, A4 Posters, Barkers')
data_vault_data3 = data_vault_data3.where(col("RETAILER_ITEM_ID") == 51169241)

data_vault_data3 = data_vault_data3.withColumn('INTERVENTION_DATE', expr("date_sub(SALES_DT, DIFF_DAY)"))
data3 = data_vault_data3.where((col("INTERVENTION_DATE") <= lit("2019-06-08")) & (col("INTERVENTION_DATE") >= lit("2019-06-01")))

# COMMAND ----------

data3 = data3.where((col("SALES_DT") <= lit("2019-06-30")) & (col("SALES_DT") >= lit("2019-06-01")))

# COMMAND ----------

display(data3)

# COMMAND ----------

agg_data3_interventions = data3.groupBy('RETAILER', 
                                      'CLIENT',
                                      'COUNTRY_CODE',
                                      'RETAILER_ITEM_ID',
                                      'ORGANIZATION_UNIT_NUM',
                                      'INTERVENTION_DATE'
                                     ).agg(count("SALES_DT").alias('COUNT'))

# COMMAND ----------

display(agg_data3_interventions)

# COMMAND ----------

unique_stores3 = data3.select('ORGANIZATION_UNIT_NUM').distinct()

# COMMAND ----------

unique_stores3.count()

# COMMAND ----------

# data_vault_data4 = data_vault_data.where(col("INTERVENTION_TYPE").isin(['None', 'Locally Agreed Display – Ladder Rack']))

data1_interventions = data_vault_data.join(unique_stores3, "ORGANIZATION_UNIT_NUM")

data1_interventions = data1_interventions.where(col("RETAILER_ITEM_ID") == 51169241)
data1_interventions = data1_interventions.where((col("SALES_DT") <= lit("2019-06-30")) & (col("SALES_DT") >= lit("2019-06-01")))

# COMMAND ----------

display(data1_interventions)

# COMMAND ----------

data1_interventions.count()

# COMMAND ----------

agg_data1_interventions = data1_interventions.groupBy('RETAILER', 
                                      'CLIENT',
                                      'COUNTRY_CODE',
                                      'RETAILER_ITEM_ID',
                                      'SALES_DT'
                                     ).agg(avg("POS_ITEM_QTY").alias('AVG_POS_ITEM_QTY'), 
                                           avg("BASELINE").alias('AVG_BASELINE'),
                                           avg("EXPECTED").alias('AVG_EXPECTED'),
                                           avg("INTERVENTION_EFFECT").alias('AVG_INTERVENTION_EFFECT'),
                                           sum("POS_ITEM_QTY").alias('TOTAL_POS_ITEM_QTY'), 
                                           sum("BASELINE").alias('TOTAL_BASELINE'),
                                           sum("EXPECTED").alias('TOTAL_EXPECTED'),
                                           sum("INTERVENTION_EFFECT").alias('TOTAL_INTERVENTION_EFFECT'))

# COMMAND ----------

display(agg_data1_interventions)

# COMMAND ----------

distinct_intervention_types = data_vault_data3.select('INTERVENTION_TYPE').distinct()

# COMMAND ----------

display(distinct_intervention_types)

# COMMAND ----------

data_vault_data5 = data_vault_data.where(col("INTERVENTION_TYPE") == 'Book Stock Error Corrected')
data_vault_data5 = data_vault_data5.where(col("RETAILER_ITEM_ID") == 51169241)

data_vault_data5 = data_vault_data5.withColumn('INTERVENTION_DATE', expr("date_sub(SALES_DT, DIFF_DAY)"))
data5 = data_vault_data5.where((col("INTERVENTION_DATE") <= lit("2019-06-08")) & (col("INTERVENTION_DATE") >= lit("2019-06-01")))

# COMMAND ----------

data5 = data5.where((col("SALES_DT") <= lit("2019-06-30")) & (col("SALES_DT") >= lit("2019-06-01")))

# COMMAND ----------

display(data5)

# COMMAND ----------

agg_data5_interventions = data5.groupBy('RETAILER', 
                                      'CLIENT',
                                      'COUNTRY_CODE',
                                      'RETAILER_ITEM_ID',
                                      'ORGANIZATION_UNIT_NUM',
                                      'INTERVENTION_DATE'
                                     ).agg(count("SALES_DT").alias('COUNT'))

# COMMAND ----------

display(agg_data5_interventions)

# COMMAND ----------

unique_stores5 = data5.select('ORGANIZATION_UNIT_NUM').distinct()

# COMMAND ----------

unique_stores5.count()

# COMMAND ----------

# data_vault_data4 = data_vault_data.where(col("INTERVENTION_TYPE").isin(['None', 'Locally Agreed Display – Ladder Rack']))

data5_interventions = data_vault_data.join(unique_stores5, "ORGANIZATION_UNIT_NUM")

data5_interventions = data5_interventions.where(col("RETAILER_ITEM_ID") == 51169241)
data5_interventions = data5_interventions.where((col("SALES_DT") <= lit("2019-06-30")) & (col("SALES_DT") >= lit("2019-06-01")))

# COMMAND ----------

display(data5_interventions)

# COMMAND ----------

data5_interventions.count()

# COMMAND ----------

agg_data5_interventions = data5_interventions.groupBy('RETAILER', 
                                      'CLIENT',
                                      'COUNTRY_CODE',
                                      'RETAILER_ITEM_ID',
                                      'SALES_DT'
                                     ).agg(avg("POS_ITEM_QTY").alias('AVG_POS_ITEM_QTY'), 
                                           avg("BASELINE").alias('AVG_BASELINE'),
                                           avg("EXPECTED").alias('AVG_EXPECTED'),
                                           avg("INTERVENTION_EFFECT").alias('AVG_INTERVENTION_EFFECT'),
                                           sum("POS_ITEM_QTY").alias('TOTAL_POS_ITEM_QTY'), 
                                           sum("BASELINE").alias('TOTAL_BASELINE'),
                                           sum("EXPECTED").alias('TOTAL_EXPECTED'),
                                           sum("INTERVENTION_EFFECT").alias('TOTAL_INTERVENTION_EFFECT'))

# COMMAND ----------

display(agg_data5_interventions)

# COMMAND ----------

PATH_COMBINED_DRE_IV_RESULTS = '/mnt/artifacts/hugh/dre-combined-kraftheinz-uk-20190301-20190630'

# COMMAND ----------

combined_dre_iv_data = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load(PATH_COMBINED_DRE_IV_RESULTS)

combined_dre_iv_data.dtypes

# COMMAND ----------

combined_dre_iv_data = combined_dre_iv_data.where(col("RETAILER_ITEM_ID") == 51169241)
combined_dre_iv_data = combined_dre_iv_data.where((col("SALES_DT") <= lit("2019-05-31")) & (col("SALES_DT") >= lit("2019-04-01")))

# COMMAND ----------

combined_dre_iv_data.count()

# COMMAND ----------

# Save the data set
combined_dre_iv_data.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/dre-kraftheinz-uk-20190401-20190531-retailer-item-id-51169241')

# COMMAND ----------


