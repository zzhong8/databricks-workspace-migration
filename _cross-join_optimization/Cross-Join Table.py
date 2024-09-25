# Databricks notebook source
# PSEUDOCODE TO CREATE REFERENCE ARTIFACT

# To create process, thinking to create tables for 'all known products' 'all known stores' that represent data loaded into the date
# reference table. Then do a join against them to identify new elements in each, and generate all dates going forward. A slightly more
# elegant way would be to store 'first selling date' per product, but it's more maintenance.

import math
import datetime
from pyspark.sql import Row
from pyspark.sql.types import *
import pyspark.sql.functions as pyf

# COMMAND ----------

dbutils.widgets.text('retailer', 'walmart', 'Retailer')
dbutils.widgets.text('client', 'clorox', 'Client')
dbutils.widgets.text('start', '20180701', 'Starting Date')
dbutils.widgets.text('end', '20190630', '_Ending Date')

RETAILER = dbutils.widgets.get('retailer').strip().upper()
CLIENT = dbutils.widgets.get('client').strip().upper()
START = datetime.datetime.strptime(dbutils.widgets.get('start'), "%Y%m%d").date()
END = datetime.datetime.strptime(dbutils.widgets.get('end'), "%Y%m%d").date()

if RETAILER == '':
    raise ValueError('\'retailer\' is a required parameter.  Please provide a value.')

if CLIENT == '':
    raise ValueError('\'client\' is a required parameter.  Please provide a value.')

for param in [RETAILER, CLIENT, START, END]:
    print(param)

# COMMAND ----------

database = '{retailer}_{client}_dv'.format(retailer=RETAILER, client=CLIENT)
print('Accessing', database)

stores_query =  '''
    SELECT HOU.ORGANIZATION_UNIT_NUM
       FROM {database}.hub_organization_unit HOU
    '''

stores_query = stores_query.format(database=database)
stores = spark.sql(stores_query)

items_query =  '''
    SELECT HRI.RETAILER_ITEM_ID
       FROM {database}.hub_retailer_item HRI
    '''
items_query = items_query.format(database=database)
items = spark.sql(items_query)

stores_items = items.crossJoin(stores)
print("Number of stores: {:,}".format(stores.count()))
print("Number of items: {:,}".format(items.count()))
print("Number of store/items: {:,}".format(stores_items.count()))

# COMMAND ----------

number_of_days = (END - START).days + 1
all_days_list = [START + datetime.timedelta(days=x) for x in range(0, number_of_days)]
all_days = spark.createDataFrame(all_days_list, DateType()).withColumnRenamed("value", "SALES_DT")
print("Number of days: {:,}".format(all_days.count()))

# COMMAND ----------

#after final cross-join add {client} and {retailer} params as constants for partitioning and joining
stores_items_dates = all_days\
                      .crossJoin(stores_items)\
                      .withColumn("CLIENT", pyf.lit(CLIENT))\
                      .withColumn("RETAILER", pyf.lit(RETAILER))\
                      .withColumn("YEAR", pyf.year(pyf.col('SALES_DT')))\
                      .withColumn("MONTH", pyf.month(pyf.col('SALES_DT')))\
                      .withColumn("RETAILER", pyf.lit(RETAILER))\

print("Number of store/item/dates: {:,}".format(stores_items_dates.count()))
print("Number of partitions: {:,}".format(stores_items_dates.rdd.getNumPartitions()))

# COMMAND ----------

#stores_items_dates_coalesced = stores_items_dates.coalesce(25) # HZ: To make this notebook run faster we may wish to comment out this line
#print("Number of partitions: {:,}".format(stores_items_dates_coalesced.rdd.getNumPartitions()))

# COMMAND ----------

date_reference_path = '/mnt/artifacts/reference/store_item_dates/retailer={retailer}/client={client}/'

# COMMAND ----------


# ADD Year and Month Columns
#pyf.Month(col("SALES_DT"))

# stores_items_dates_coalesced\
#   .write\
#   #.partitionBy("YEAR","MONTH")
#   .format("delta")\
#   .mode("overwrite")\
#   .save(date_reference_path.format(retailer=RETAILER, client=CLIENT))

stores_items_dates\
  .write\
  .partitionBy("YEAR","MONTH")\
  .format("delta")\
  .mode("overwrite")\
  .save(date_reference_path.format(retailer=RETAILER, client=CLIENT))
  
#stores_items_dates.write.format("delta").mode("overwrite").partitionBy("Retailer","Client").save(date_reference_path)
# DO we need to optimize for the read queries used in inference notebook?

#I think it makes sense to use a delta table to hold this because we are theoretically tacking items onto it regularly after initialization. 
#Not sure how critical it is to start it as a delta table, but this felt like as good a time as any to get started with it.

# COMMAND ----------

# Use the SQL API to apply Z-Order optimization to the delta table
optimize_sql_statement = "OPTIMIZE delta.`{}` ZORDER BY (SALES_DT)".format(date_reference_path.format(retailer=RETAILER, client=CLIENT))
spark.sql(optimize_sql_statement)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/artifacts/reference/store_item_dates/retailer=WALMART/client=CLOROX/

# COMMAND ----------

# df_store_item_dates = spark.read.format("delta").load(date_reference_path.format(retailer=RETAILER, client=CLIENT))

# COMMAND ----------

# display(df_store_item_dates.groupBy("SALES_DT").count().orderBy("SALES_DT"))

# COMMAND ----------

# %fs
# mounts

# COMMAND ----------

# sql_statement = """
#     SELECT DISTINCT
#           HRI.RETAILER_ITEM_ID,
#           HOU.ORGANIZATION_UNIT_NUM
#        FROM {database}.vw_sat_link_epos_summary VSLES
#        INNER JOIN {database}.hub_retailer_item HRI 
#             ON HRI.HUB_RETAILER_ITEM_HK = VSLES.HUB_RETAILER_ITEM_HK
#        INNER JOIN {database}.hub_organization_unit HOU 
#             ON HOU.HUB_ORGANIZATION_UNIT_HK = VSLES.HUB_ORGANIZATION_UNIT_HK
#     """

# sql_statement = sql_statement.format(database=database, retailer=RETAILER, client=CLIENT)

# df = spark.sql(sql_statement)

# COMMAND ----------

# df_store_item_dates.count()
# 3583211     9.44 minutes

# COMMAND ----------

# sql_statement = """
#     SELECT DISTINCT
#           HRI.RETAILER_ITEM_ID,
#           HOU.ORGANIZATION_UNIT_NUM
#        FROM {database}.vw_sat_link_epos_summary VSLES
#        INNER JOIN {database}.hub_retailer_item HRI 
#             ON HRI.HUB_RETAILER_ITEM_HK = VSLES.HUB_RETAILER_ITEM_HK
#        INNER JOIN {database}.hub_organization_unit HOU 
#             ON HOU.HUB_ORGANIZATION_UNIT_HK = VSLES.HUB_ORGANIZATION_UNIT_HK
#     """

# sql_statement = sql_statement.format(database=database, retailer=RETAILER, client=CLIENT)

# df2 = spark.sql(sql_statement)

# df2.count()
# # 3583211     9.44 minutes

# COMMAND ----------

# df_store_item_dates.select(pyf.min("SALES_DT")).show()

# COMMAND ----------


