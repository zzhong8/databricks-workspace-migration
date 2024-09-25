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

data_vault_data = spark.read.format('csv').load(PATH_CAUSAL_RESULTS)
print(data_vault_data.dtypes)

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

# data_vault_data2 = data_vault_data.where(col("INTERVENTION_TYPE") != 'None')

# COMMAND ----------

# data_vault_data2.count()

# COMMAND ----------

# data_vault_data2 = data_vault_data2.withColumn('INTERVENTION_DATE', expr("date_sub(SALES_DT, DIFF_DAY)"))
# data1 = data_vault_data2.where((col("INTERVENTION_DATE") <= lit("2019-04-30")) & (col("INTERVENTION_DATE") >= lit("2019-04-01")))

# COMMAND ----------

# data1.count()

# COMMAND ----------

# display(data1)

# COMMAND ----------

# data2 = data_vault_data2.where((col("SALES_DT") <= lit("2019-06-30")) & (col("SALES_DT") >= lit("2018-03-01")))

# data2.count()

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

# Save the data set
data_vault_data.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/tesco-nestlecereals-uk-20190301-20190630')
