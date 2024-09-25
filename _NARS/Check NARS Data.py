# Databricks notebook source
from pyspark.sql import functions as pyf
from pyspark.sql import types as pyt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from nars_raw.dpau       
# MAGIC  where       
# MAGIC PRODUCT_COUNTRY_ID = 30 -- UK
# MAGIC and client_id = 16320 -- NestleCore    
# MAGIC and dpau.type = 'DLA'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from nars_raw.dpau where       
# MAGIC PRODUCT_COUNTRY_ID = 30 -- UK
# MAGIC and client_id = 16320 -- NestleCore    
# MAGIC and dpau.type = 'DLA'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select DISTINCT RETAILER_ID from nars_raw.dpau where       
# MAGIC PRODUCT_COUNTRY_ID = 30 -- UK
# MAGIC and client_id = 16320 -- NestleCore    
# MAGIC and dpau.type = 'DLA'

# COMMAND ----------

distribution_response_table = spark.sql('SELECT * FROM nars.distribution_response')

# COMMAND ----------

distribution_response_table.printSchema()

# COMMAND ----------

distribution_response_table.count()

# COMMAND ----------

display(distribution_response_table)

# COMMAND ----------

prjsch_table = spark.sql('SELECT * FROM nars.prjsch')

# COMMAND ----------

prjsch_table.printSchema()

# COMMAND ----------

prjsch_table.count()

# COMMAND ----------

display(prjsch_table)

# COMMAND ----------

start_dates = prjsch_table.select('START_DATE').distinct().orderBy('START_DATE', ascending=False)

# COMMAND ----------

display(start_dates)

# COMMAND ----------

prjsch_table_2019_05_06 = prjsch_table.filter(prjsch_table.START_DATE == '2019-05-06')

# COMMAND ----------

prjsch_table_2019_05_06.count()

# COMMAND ----------

display(prjsch_table_2019_05_06)

# COMMAND ----------

# RETAILER = 'walmart'
# CLIENT = 'clorox'
# COUNTRY = 'us'

# COMMAND ----------

# # LOOK UP HASH KEYS
# databaseName = '{}_{}_{}_dv'.format(RETAILER.lower(), CLIENT.lower(), COUNTRY.lower())
# itemMasterTableName = '{}.hub_retailer_item'.format(databaseName)
# storeMasterTableName = '{}.hub_organization_unit'.format(databaseName)

# converted_dataframe = drfe_forecast_baseline_unit_table.alias('PRDF') \
#     .join(sqlContext.read.table(storeMasterTableName).alias('OUH'),
#           pyf.col('OUH.HUB_ORGANIZATION_UNIT_HK') == pyf.col('PRDF.HUB_ORGANIZATION_UNIT_HK'), 'inner') \
#     .join(sqlContext.read.table(itemMasterTableName).alias('RIH'),
#           pyf.col('RIH.HUB_RETAILER_ITEM_HK') == pyf.col('PRDF.HUB_RETAILER_ITEM_HK'), 'inner') \
#     .select('PRDF.*', 'OUH.ORGANIZATION_UNIT_NUM', 'RIH.RETAILER_ITEM_ID')

# COMMAND ----------

# display(converted_dataframe)

# COMMAND ----------

# display(converted_dataframe.select('RETAILER_ITEM_ID').distinct())

# COMMAND ----------


