# Databricks notebook source
import uuid
import warnings
import numpy as np
import pandas as pd
from datetime import datetime, date
from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark.sql import types as pyt
from pyspark.sql import functions as pyf
import acosta
from acosta.alerting.preprocessing import read_pos_data
from acosta.alerting.helpers import check_path_exists

print(acosta.__version__)
auto_model_prefix = 'model'
current_timestamp = datetime.now().strftime('%Y-%m-%d')

# COMMAND ----------

sqlContext.setConf("spark.sql.shuffle.partitions", "800")

# COMMAND ----------

dbutils.widgets.text('retailer', 'walmart', 'Retailer')
dbutils.widgets.text('client', 'clorox', 'Client')
dbutils.widgets.text('country_code', 'us', 'Country Code')

RETAILER = dbutils.widgets.get('retailer').strip().lower()
CLIENT = dbutils.widgets.get('client').strip().lower()
COUNTRY_CODE = dbutils.widgets.get('country_code').strip().lower()

if RETAILER == '':
    raise ValueError('\'retailer\' is a required parameter.  Please provide a value.')

if CLIENT == '':
    raise ValueError('\'client\' is a required parameter.  Please provide a value.')

for param in [RETAILER, CLIENT, COUNTRY_CODE]:
    print(param)

# COMMAND ----------

# Read POS data
data_vault_data = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')

# COMMAND ----------

sales_dates = data_vault_data.select('SALES_DT').distinct().orderBy('SALES_DT', ascending=False)

# COMMAND ----------

sales_dates.count()

# COMMAND ----------

display(sales_dates)

# COMMAND ----------

data_vault_data.count()

# COMMAND ----------

display(data_vault_data)

# COMMAND ----------

# This filter requires at least 365 days sales history (zero or non-zero) in the entire datset
subset_meets_threshold = data_vault_data\
    .select('RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM', 'POS_ITEM_QTY') \
    .groupBy('RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM') \
    .count() \
    .filter('count >= 365') \
    .drop('count')

data_to_be_trained = data_vault_data.join(
    subset_meets_threshold,
    ['RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'],
    'inner'
)

# COMMAND ----------

# Filter only 2018 & 2019
# date_filtered = data_to_be_trained.filter(data_to_be_trained['SALES_DT'] >= date(2018, 1, 1))

date_filtered = data_to_be_trained

# COMMAND ----------

retail_item_ids_to_be_trained = date_filtered.select('RETAILER_ITEM_ID').distinct()

# retail_item_ids_to_be_trained.count()

# COMMAND ----------

organization_unit_nums_to_be_trained = date_filtered.select('ORGANIZATION_UNIT_NUM').distinct()

# organization_unit_nums_to_be_trained.count()

# COMMAND ----------

data_to_be_trained = date_filtered.withColumn("DAYOFMONTH", pyf.dayofmonth("SALES_DT"))

# COMMAND ----------

# Check if the input files exist and if not raise a value error
store_states_names = '/mnt/prod-ro/artifacts/country_code/reference/orgunitnum_to_state.csv'
pay_cycles_path = '/mnt/prod-ro/artifacts/country_code/reference/pay_cycle_profiles_by_state.csv'

check_path_exists(store_states_names, 'csv', 'raise')
check_path_exists(pay_cycles_path, 'csv', 'raise')

snap_pay_cycle_path = '/mnt/artifacts/country_code/reference/snap_paycycle/retailer={retailer}/client={client}/country_code={country_code}/'\
    .format(retailer=RETAILER, client=CLIENT, country_code=COUNTRY_CODE)

# COMMAND ----------

# Generate and save snap index for the data set
database = '{retailer}_{client}_us_dv'.format(retailer=RETAILER, client=CLIENT)
print('Accessing', database)

store_names_query = '''
    SELECT SOU.ORGANIZATION_UNIT_NM, HOU.ORGANIZATION_UNIT_NUM
       FROM {database}.sat_organization_unit SOU 
       INNER JOIN {database}.hub_organization_unit HOU 
       ON SOU.HUB_ORGANIZATION_UNIT_HK = HOU.HUB_ORGANIZATION_UNIT_HK
    '''

store_names_query = store_names_query.format(database=database)
store_names = spark.sql(store_names_query)

print(store_names.count())

mapped_stores_states = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load(store_states_names)

stores_with_state = store_names\
    .join(mapped_stores_states, store_names.ORGANIZATION_UNIT_NUM == mapped_stores_states.ORGANIZATION_UNIT_NUM, 'left_outer')\
    .select(store_names['ORGANIZATION_UNIT_NUM'], mapped_stores_states['STATE'])

# Puerto Rico currently does not have a SNAP pay-cycle profile - use DEFAULT profile
stores_with_state = stores_with_state.withColumn('STATE', when(col('STATE') == 'PR', 'DEFAULT').otherwise(col('STATE')))

# Replace non-matches with DEFAULT profile
stores_with_state = stores_with_state.withColumn('STATE', when(col('STATE').isNull(), 'DEFAULT').otherwise(col('STATE')))

# Replace non-US-stores with DEFAULT profile
if (COUNTRY_CODE != 'us'):
    stores_with_state = stores_with_state.withColumn("STATE", lit("DEFAULT"))
    
# Remove any duplicates and take the first state alphabetically
windowSpec = Window.partitionBy('ORGANIZATION_UNIT_NUM').orderBy('STATE')
unique_store_names = stores_with_state\
    .withColumn('column_rank', row_number().over(windowSpec)) \
    .filter('column_rank == 1')\
    .drop('column_rank')

# TODO: Instead of reading from CSV, calculate these values
pay_cycles = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load(pay_cycles_path)

# If client is non-US
if (COUNTRY_CODE != 'us'):
    pay_cycles = pay_cycles.filter(pay_cycles.STATE == 'DEFAULT')
    
    # Cross-join table
    stores_with_pc = unique_store_names\
        .crossJoin(pay_cycles)\
        .select(unique_store_names['ORGANIZATION_UNIT_NUM'], pay_cycles['*'])

else:
    stores_with_pc = unique_store_names\
        .join(pay_cycles, unique_store_names.STATE == pay_cycles.STATE)\
        .select(unique_store_names['ORGANIZATION_UNIT_NUM'], pay_cycles['*'])

print(stores_with_pc.count() / 31, 'stores')

# COMMAND ----------

display(stores_with_pc)

# COMMAND ----------

# Check if the input files exist and if not raise a value error
store_states_names = '/mnt/prod-ro/artifacts/reference/WMS_Active_Products_campbellssnack.csv'

mapped_stores_categories = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load(store_states_names)

# COMMAND ----------

display(mapped_stores_categories)

# COMMAND ----------

store_categories = mapped_stores_categories.select('FullName', 'ProductGroupName', 'Retail Product Code')
store_categories = store_categories.withColumnRenamed('Retail Product Code', 'RETAILER_ITEM_ID')

# COMMAND ----------

full_dataset = data_to_be_trained.join(
    stores_with_pc,
    ['DAYOFMONTH', 'ORGANIZATION_UNIT_NUM'],
    'inner'
)

# COMMAND ----------

full_dataset = full_dataset.join(
    store_categories,
    ['RETAILER_ITEM_ID'],
    'inner'
)

# COMMAND ----------

full_dataset = full_dataset.drop('ROW_ORIGIN', 'PRICE_ORIGIN', 'TRAINING_ROLE', 'RETAILER', 'CLIENT', 'CONTRY_CODE', 'POS_AMT', 'UNIT_PRICE', 'STATE', 'NONSNAPINDEX', 'SNAPINDEX')

# COMMAND ----------

full_dataset.count()

# COMMAND ----------

full_dataset.printSchema()

# COMMAND ----------

# This filter requires at least 365 days sales history (zero or non-zero) in the entire datset
average_daily_item_sales_by_product_group = full_dataset\
    .groupBy('ProductGroupName', 'RETAILER_ITEM_ID', 'FullName', 'SALES_DT')\
    .agg(avg('POS_ITEM_QTY'), avg('ON_HAND_INVENTORY_QTY'))\
    .orderBy('ProductGroupName', 'SALES_DT', ascending=True)

# COMMAND ----------

output_path = '/mnt/artifacts/Monitoring/average_daily_item_sales_by_product_group.csv'

average_daily_item_sales_by_product_group.coalesce(1).write.mode('overwrite').option('header', 'true').csv(output_path)

# COMMAND ----------

display(average_daily_item_sales_by_product_group)

# COMMAND ----------

# This filter requires at least 365 days sales history (zero or non-zero) in the entire datset
average_daily_sales_by_product_group = full_dataset\
    .groupBy('ProductGroupName', 'SALES_DT')\
    .agg(avg('POS_ITEM_QTY'), avg('ON_HAND_INVENTORY_QTY'))\
    .orderBy('ProductGroupName', 'SALES_DT', ascending=True)

# COMMAND ----------

display(average_daily_sales_by_product_group)

# COMMAND ----------

output_path = '/mnt/artifacts/Monitoring/average_daily_sales_by_product_group.csv'

average_daily_sales_by_product_group.coalesce(1).write.mode('overwrite').option('header', 'true').csv(output_path)

# COMMAND ----------

# This filter requires at least 365 days sales history (zero or non-zero) in the entire datset
product_category_counts = full_dataset\
    .groupBy('ProductGroupName') \
    .count()

# COMMAND ----------

product_categories = full_dataset.select('ProductGroupName').distinct()

# COMMAND ----------

display(product_category_counts)

# COMMAND ----------

output_path = '/mnt/artifacts/Monitoring/campbellssnack_unfiltered_dataset.csv'

full_dataset.coalesce(1).write.mode('overwrite').option('header', 'true').csv(output_path)

# COMMAND ----------

filtered_dataset = full_dataset.where(pyf.col('ProductGroupName').isin({'Chips', 'Nuts'}))

filtered_dataset.count()

# COMMAND ----------

output_path = '/mnt/artifacts/Monitoring/campbellssnack_sales_dataset.csv'

filtered_dataset.coalesce(1).write.mode('overwrite').option('header', 'true').csv(output_path)

# COMMAND ----------


