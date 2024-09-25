# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime

# COMMAND ----------

PATH_LEGACY_IV_RESULTS = '/mnt/artifacts/hugh/legacy-kraftheinz-uk-20180701-20190630/'

# COMMAND ----------

legacy_iv_data = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load(PATH_LEGACY_IV_RESULTS)

legacy_iv_data.dtypes

# COMMAND ----------

legacy_iv_data.count()

# COMMAND ----------

display(legacy_iv_data)

# COMMAND ----------

data1 = legacy_iv_data.withColumn('InterventionDate', to_date(unix_timestamp('InterventionDate', 'dd/MM/yyyy').cast('timestamp')))
data1.count()

# COMMAND ----------

data2 = data1.where((col("InterventionDate") <= lit("2019-04-30")) & (col("InterventionDate") >= lit("2019-04-01")))
data2.count()

# COMMAND ----------

# CHANGE

PATH_DRE_IV_RESULTS = '/mnt/artifacts/hugh/dre-kraftheinz-uk-20190301-20190630-new/'

# COMMAND ----------

dre_iv_data = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load(PATH_DRE_IV_RESULTS)

print(dre_iv_data.dtypes)

# COMMAND ----------

dre_iv_data.count()

# COMMAND ----------

display(dre_iv_data)

# COMMAND ----------

# Save the data set
dre_iv_data.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/dre-combined-kraftheinz-uk-20190301-20190630')

# COMMAND ----------

agg_dre_iv_data = dre_iv_data.groupBy('RETAILER', 
                                      'CLIENT',
                                      'COUNTRY_CODE',
                                      'RETAILER_ITEM_ID',
                                      'ORGANIZATION_UNIT_NUM',
                                      'INTERVENTION_DATE',
                                      'INTERVENTION_TYPE',
                                      'INTERVENTION_GROUP'
                                     ).agg(sum("POS_ITEM_QTY").alias('TOTAL_POS_ITEM_QTY'), 
                                           sum("BASELINE").alias('TOTAL_BASELINE'),
                                           sum("EXPECTED").alias('TOTAL_EXPECTED'),
                                           sum("INTERVENTION_EFFECT").alias('TOTAL_INTERVENTION_EFFECT'),
                                           min("DIFF_DAY").alias('DIFF_DAY_START'),
                                           max("DIFF_DAY").alias('DIFF_DAY_END'))

# COMMAND ----------

agg_dre_iv_data.count()

# COMMAND ----------

display(agg_dre_iv_data)

# COMMAND ----------

# joined_data = data2.join(agg_dre_iv_data, ((data2.ChainRefExternal == agg_dre_iv_data.ORGANIZATION_UNIT_NUM) & (data2.ProductNumber == agg_dre_iv_data.RETAILER_ITEM_ID)) & (data2.InterventionDate == agg_dre_iv_data.INTERVENTION_DATE))

# COMMAND ----------

joined_data = data2.join(agg_dre_iv_data, ((data2.ChainRefExternal == agg_dre_iv_data.ORGANIZATION_UNIT_NUM) & (data2.ProductNumber == agg_dre_iv_data.RETAILER_ITEM_ID)) & ((data2.InterventionDate == agg_dre_iv_data.INTERVENTION_DATE) & (data2.Intervention == agg_dre_iv_data.INTERVENTION_TYPE)))

# COMMAND ----------

joined_data.count()

# COMMAND ----------

display(joined_data)

# COMMAND ----------

# Save the data set
joined_data.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/joined-kraftheinz-uk-20190401-20190430')

# COMMAND ----------

PATH_DRE_IV_RESULTS_2 = '/mnt/artifacts/hugh/dre-kraftheinz-uk-20190401-20190430/'

# COMMAND ----------

dre_iv_data_2 = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load(PATH_DRE_IV_RESULTS_2)

print(dre_iv_data_2.dtypes)

# COMMAND ----------

agg_dre_iv_data_2 = dre_iv_data_2.groupBy('RETAILER', 
                                      'CLIENT',
                                      'COUNTRY_CODE',
                                      'RETAILER_ITEM_ID',
                                      'ORGANIZATION_UNIT_NUM',
                                      'CALLFILE_VISIT_ID',
                                      'INTERVENTION_DATE',
                                      'INTERVENTION_TYPE',
                                      'INTERVENTION_GROUP'
                                     ).agg(sum("POS_ITEM_QTY").alias('TOTAL_POS_ITEM_QTY'), 
                                           sum("BASELINE").alias('TOTAL_BASELINE'),
                                           sum("EXPECTED").alias('TOTAL_EXPECTED'),
                                           sum("INTERVENTION_EFFECT").alias('TOTAL_INTERVENTION_EFFECT'),
                                           min("DIFF_DAY").alias('DIFF_DAY_START'),
                                           max("DIFF_DAY").alias('DIFF_DAY_END'))

# COMMAND ----------

agg_dre_iv_data.count()

# COMMAND ----------

joined_data_2 = legacy_iv_data.join(agg_dre_iv_data_2, (legacy_iv_data.CallfileVisitId == agg_dre_iv_data_2.CALLFILE_VISIT_ID) & (legacy_iv_data.ProductNumber == agg_dre_iv_data_2.RETAILER_ITEM_ID))

# COMMAND ----------

joined_data_2.count()

# COMMAND ----------


