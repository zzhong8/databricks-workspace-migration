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

mapped_stores_states = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load('/mnt/prod-ro/artifacts/reference/orgunitnum_to_state.csv')

# COMMAND ----------

store_names.count()

# COMMAND ----------

stores_with_state = store_names\
    .join(mapped_stores_states, store_names.ORGANIZATION_UNIT_NUM == mapped_stores_states.ORGANIZATION_UNIT_NUM, 'left_outer')\
    .select(store_names['ORGANIZATION_UNIT_NUM'], mapped_stores_states['STATE'])

# COMMAND ----------

stores_with_state = stores_with_state.withColumn("STATE", when(col("STATE") == "PR", "DEFAULT").otherwise(col("STATE")))
stores_with_state = stores_with_state.withColumn("STATE", when(col("STATE").isNull(), "DEFAULT").otherwise(col("STATE")))

# COMMAND ----------

display(stores_with_state.filter(stores_with_state.STATE == "DEFAULT"))

# COMMAND ----------



# COMMAND ----------

#store_names = spark.sql(store_names_query).withColumn('STATE', store_names.ORGANIZATION_UNIT_NM.substr(-2, 2))

# Remove any duplicates and take the first state alphabetically
windowSpec = Window.partitionBy('ORGANIZATION_UNIT_NUM').orderBy('STATE')
unique_store_names = stores_with_state\
    .withColumn('column_rank', row_number().over(windowSpec)) \
    .filter('column_rank == 1')\
    .drop('column_rank')

# TODO: Instead of reading from CSV, calculate these values
pay_cycles = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load('/mnt/prod-ro/artifacts/reference/pay_cycle_profiles_by_state.csv')

stores_with_pc = unique_store_names\
    .join(pay_cycles, unique_store_names.STATE == pay_cycles.STATE)\
    .select(unique_store_names['ORGANIZATION_UNIT_NUM'], pay_cycles['*'])

# COMMAND ----------

stores_with_pc.count() / 31

# COMMAND ----------

display(unique_store_names.filter(unique_store_names.STATE == "DEFAULT").orderBy(unique_store_names.ORGANIZATION_UNIT_NUM))

# COMMAND ----------

store_with_default_snap_index = unique_store_names.filter(unique_store_names.STATE == "DEFAULT").select(unique_store_names.ORGANIZATION_UNIT_NUM).orderBy(unique_store_names.ORGANIZATION_UNIT_NUM)

# COMMAND ----------

# snap_pay_cycle_path = '/mnt/artifacts/reference/snap_paycycle/retailer={retailer}/client=clorox_test/'
# stores_with_pc.coalesce(1).write.mode('overwrite').option("header", "true").csv(snap_pay_cycle_path.format(retailer=RETAILER, client=CLIENT))

# COMMAND ----------

snap_pay_cycle_path = '/mnt/artifacts/reference/snap_paycycle/retailer={retailer}/client={client}/'
stores_with_pc.write.mode('overwrite').parquet(snap_pay_cycle_path.format(retailer=RETAILER, client=CLIENT))

# COMMAND ----------


