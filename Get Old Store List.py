# Databricks notebook source
dbutils.widgets.text('RETAILER_PARAM', 'walmart')
dbutils.widgets.text('CLIENT_PARAM', '')
DATE_FIELD = 'SALES_DT'

RETAILER = dbutils.widgets.get('RETAILER_PARAM').strip().lower()
CLIENT = dbutils.widgets.get('CLIENT_PARAM').strip().lower()

# COMMAND ----------

from pyspark.sql.functions import substring
from pyspark.sql.functions import *
from pyspark.sql import Window

database = '{retailer}_{client}_dv'.format(retailer=RETAILER, client=CLIENT)

store_names_query = '''
    SELECT SOU.ORGANIZATION_UNIT_NM, HOU.ORGANIZATION_UNIT_NUM
       FROM {database}.sat_organization_unit SOU 
       INNER JOIN {database}.hub_organization_unit HOU 
       ON SOU.HUB_ORGANIZATION_UNIT_HK = HOU.HUB_ORGANIZATION_UNIT_HK
    '''

store_names_query = store_names_query.format(database=database)
store_names = spark.sql(store_names_query)
store_names = spark.sql(store_names_query).withColumn('STATE', store_names.ORGANIZATION_UNIT_NM.substr(-2, 2))

# Remove any duplicates and take the first state alphabetically
windowSpec = Window.partitionBy('ORGANIZATION_UNIT_NUM').orderBy('STATE')
unique_store_names = store_names\
    .withColumn('column_rank', row_number().over(windowSpec)) \
    .filter('column_rank == 1')\
    .drop('column_rank')

# TODO: Instead of reading from CSV, calculate these values
pay_cycles = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load('/mnt/prod-ro/artifacts/reference/pay_cycle_profiles_by_state.csv')

stores_with_pc = unique_store_names\
    .join(pay_cycles, unique_store_names.STATE == pay_cycles.STATE)\
    .select(unique_store_names['ORGANIZATION_UNIT_NUM'])

windowSpec2 = Window.partitionBy('ORGANIZATION_UNIT_NUM').orderBy('ORGANIZATION_UNIT_NUM')
stores_with_pc = stores_with_pc\
    .withColumn('column_rank', row_number().over(windowSpec2)) \
    .filter('column_rank == 1')\
    .drop('column_rank')

snap_pay_cycle_path = '/mnt/artifacts/reference/snap_paycycle/retailer={retailer}/client=tysonhillshire_store_list_old/'
stores_with_pc.coalesce(1).write.mode('append').option("header", "true").csv(snap_pay_cycle_path.format(retailer=RETAILER, client=CLIENT))
