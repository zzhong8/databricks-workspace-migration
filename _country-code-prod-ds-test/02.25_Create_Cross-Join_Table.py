# Databricks notebook source
# PSEUDOCODE TO CREATE REFERENCE ARTIFACT

# To create process, thinking to create tables for 'all known products' 'all known stores' that represent data loaded into the date
# reference table. Then do a join against them to identify new elements in each, and generate all dates going forward.

import math
import datetime
import pandas as pd
from pyspark.sql import Row
from pyspark.sql.types import *
import pyspark.sql.functions as pyf

from acosta.alerting.helpers import check_path_exists

# COMMAND ----------

dbutils.widgets.text('retailer', 'walmart', 'Retailer')
dbutils.widgets.text('client', 'clorox', 'Client')
dbutils.widgets.text('country_code', 'us', 'Country Code')

dbutils.widgets.text('start', '20190101', 'Starting Date')
dbutils.widgets.text('end', '20191231', '_Ending Date')

RETAILER = dbutils.widgets.get('retailer').strip().lower()
CLIENT = dbutils.widgets.get('client').strip().lower()
COUNTRY_CODE = dbutils.widgets.get('country_code').strip().lower()

START = datetime.datetime.strptime(dbutils.widgets.get('start'), '%Y%m%d').date()
END = datetime.datetime.strptime(dbutils.widgets.get('end'), '%Y%m%d').date()

if RETAILER == '':
    raise ValueError('\'retailer\' is a required parameter.  Please provide a value.')

if CLIENT == '':
    raise ValueError('\'client\' is a required parameter.  Please provide a value.')

if COUNTRY_CODE == '':
    raise ValueError('\'country_code\' is a required parameter.  Please provide a value.')
    
for param in [RETAILER, CLIENT, COUNTRY_CODE, START, END]:
    print(param)

# COMMAND ----------

# TODO add path exists check after the format has changed to delta.
# Get the store/item combinations that we have champion models for
stores_items = spark.read.parquet('/mnt/artifacts/country_code/champion_models/') \
    .filter("RETAILER = '{}' and CLIENT = '{}' and COUNTRY_CODE = '{}'".format(RETAILER, CLIENT, COUNTRY_CODE)) \
    .select(['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM']) \
    .distinct()

print('Number of items: {:,}'.format(stores_items.select('RETAILER_ITEM_ID').distinct().count()))

# COMMAND ----------

# NOTE: Can this UDF be replaced with something in native spark?
expansion_return_schema = StructType([
    StructField('RETAILER_ITEM_ID', StringType()),
    StructField('ORGANIZATION_UNIT_NUM', IntegerType())
])

@pyf.pandas_udf(expansion_return_schema, pyf.PandasUDFType.GROUPED_MAP)
def expand_organization_number(df):
    out = pd.DataFrame(df['ORGANIZATION_UNIT_NUM'].values[0], columns=['ORGANIZATION_UNIT_NUM'])
    out['RETAILER_ITEM_ID'] = df['RETAILER_ITEM_ID'].values[0]
    return out[['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM']]  # Ensure return order

stores_items = stores_items.groupby('RETAILER_ITEM_ID').apply(expand_organization_number)  

# COMMAND ----------

# Import approved product list for this client
approved_product_list_reference_path = '/mnt/artifacts/country_code/reference/approved_product_list/retailer={retailer}/client={client}/country_code={country_code}/'\
    .format(retailer=RETAILER, client=CLIENT, country_code=COUNTRY_CODE)

# If the approved product list exists for this client
if check_path_exists(approved_product_list_reference_path, 'delta', 'ignore'):
    approved_product_list = spark.read.format('delta')\
        .load(approved_product_list_reference_path)\
        .select(['RetailProductCode'])
    
    stores_items = stores_items.join(
        approved_product_list,
        stores_items.RETAILER_ITEM_ID == approved_product_list.RetailProductCode
    )
    
else:
    print('Approved product list not found for client')

# COMMAND ----------

number_of_days = (END - START).days + 1
all_days_list = [START + datetime.timedelta(days=x) for x in range(0, number_of_days)]
all_days = spark.createDataFrame(all_days_list, DateType()).withColumnRenamed('value', 'SALES_DT')
print('Number of days: {:,}'.format(all_days.count()))

# COMMAND ----------

#after final cross-join add {client} and {retailer} params as constants for partitioning and joining
stores_items_dates = all_days.crossJoin(stores_items)\
                     .withColumn("CLIENT", pyf.lit(CLIENT))\
                     .withColumn("RETAILER", pyf.lit(RETAILER))\
                     .withColumn("COUNTRY_CODE", pyf.lit(COUNTRY_CODE))

print("Number of store/item/dates: {:,}".format(stores_items_dates.count()))
print("Number of partitions: {:,}".format(stores_items_dates.rdd.getNumPartitions()))

# COMMAND ----------

date_reference_path = '/mnt/artifacts/country_code/reference/store_item_dates/retailer={retailer}/client={client}/country_code={country_code}/'

# COMMAND ----------

stores_items_dates\
    .write\
    .partitionBy('SALES_DT')\
    .format('delta')\
    .mode('overwrite')\
    .save(date_reference_path.format(retailer=RETAILER, client=CLIENT, country_code=COUNTRY_CODE))
  
# It makes sense to use a delta table to hold this because we are theoretically tacking items onto it regularly after initialization. 

# COMMAND ----------

# Use the SQL API to apply Z-Order optimization to the delta table
optimize_sql_statement = 'OPTIMIZE delta.`{}` ZORDER BY (ORGANIZATION_UNIT_NUM, RETAILER_ITEM_ID)'.format(
    date_reference_path.format(retailer=RETAILER, client=CLIENT, country_code=COUNTRY_CODE)
)
spark.sql(optimize_sql_statement)
