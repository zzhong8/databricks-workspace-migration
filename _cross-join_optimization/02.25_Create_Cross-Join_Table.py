# Databricks notebook source
# PSEUDOCODE TO CREATE REFERENCE ARTIFACT

# To create process, thinking to create tables for 'all known products' 'all known stores' that represent data loaded into the date
# reference table. Then do a join against them to identify new elements in each, and generate all dates going forward.

import math
import datetime
from pyspark.sql import Row
from pyspark.sql.types import *
import pyspark.sql.functions as pyf

# COMMAND ----------

dbutils.widgets.text('retailer', 'walmart', 'Retailer')
dbutils.widgets.text('client', 'clorox', 'Client')
dbutils.widgets.text('start', '20190101', 'Starting Date')
dbutils.widgets.text('end', '20191231', '_Ending Date')

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

# Get the store/item combinations that we have champion models for
stores_items = spark.read.parquet("/mnt/artifacts/champion_models/")\
.filter("RETAILER = '{}' and CLIENT = '{}'".format(RETAILER.upper(), CLIENT.upper()))\
.select(['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM']).distinct()

print("Number of store/items: {:,}".format(stores_items.count()))

# COMMAND ----------

from pyspark.sql.utils import AnalysisException

def path_exists(path):
    try:
        x = spark.read.format("delta").load(path)
        return True
      
    except AnalysisException as e:
        return False

# COMMAND ----------

RETAILER = RETAILER.lower()
CLIENT = CLIENT.lower()

# Import approved product list for this client
approved_product_list_reference_path = '/mnt/artifacts/reference/approved_product_list/retailer={retailer}/client={client}/'

# If the approved product list exists for this client
if path_exists(approved_product_list_reference_path.format(retailer=RETAILER, client=CLIENT)): 
    approved_product_list = spark.read.format("delta").load(approved_product_list_reference_path.format(retailer=RETAILER, client=CLIENT)).select(["RetailProductCode"])
    
    stores_items = stores_items.join(approved_product_list, stores_items.RETAILER_ITEM_ID == approved_product_list.RetailProductCode)
    
else:
    print("Approved product list not found for client")
    
RETAILER = RETAILER.upper()
CLIENT = CLIENT.upper()

# COMMAND ----------

number_of_days = (END - START).days + 1
all_days_list = [START + datetime.timedelta(days=x) for x in range(0, number_of_days)]
all_days = spark.createDataFrame(all_days_list, DateType()).withColumnRenamed("value", "SALES_DT")
print("Number of days: {:,}".format(all_days.count()))

# COMMAND ----------

#after final cross-join add {client} and {retailer} params as constants for partitioning and joining
stores_items_dates = all_days.crossJoin(stores_items).withColumn("CLIENT", pyf.lit(CLIENT)).withColumn("RETAILER", pyf.lit(RETAILER))

print("Number of store/item/dates: {:,}".format(stores_items_dates.count()))
print("Number of partitions: {:,}".format(stores_items_dates.rdd.getNumPartitions()))

# COMMAND ----------

date_reference_path = '/mnt/artifacts/reference/store_item_dates/retailer={retailer}/client={client}/'

# COMMAND ----------

stores_items_dates\
  .write\
  .partitionBy("SALES_DT")\
  .format("delta")\
  .mode("overwrite")\
  .save(date_reference_path.format(retailer=RETAILER, client=CLIENT))
  
# It makes sense to use a delta table to hold this because we are theoretically tacking items onto it regularly after initialization. 

# COMMAND ----------

# Use the SQL API to apply Z-Order optimization to the delta table
optimize_sql_statement = "OPTIMIZE delta.`{}` ZORDER BY (ORGANIZATION_UNIT_NUM, RETAILER_ITEM_ID)".format(date_reference_path.format(retailer=RETAILER, client=CLIENT))
spark.sql(optimize_sql_statement)
