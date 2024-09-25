# Databricks notebook source
from pprint import pprint
import warnings
from datetime import datetime
from pyspark.sql import Window
import pyspark.sql.functions as pyf
import acosta
import pyarrow
from acosta.alerting.preprocessing import read_pos_data, pos_to_training_data
from acosta.alerting.helpers import check_path_exists

print(acosta.__version__)
print(pyarrow.__version__)

current_timestamp = datetime.now().strftime('%Y-%m-%d')

# COMMAND ----------

try:
    is_nowcast
except NameError:
    is_nowcast = False

auto_model_prefix = 'data' if is_nowcast else 'model'

if not is_nowcast:
    dbutils.widgets.text('retailer', 'walmart', 'Retailer')
    dbutils.widgets.text('client', 'clorox', 'Client')
    dbutils.widgets.text('countrycode', 'us', 'Country Code')

    dbutils.widgets.text('store', '', 'Organization Unit Num')
    dbutils.widgets.text('item', '', 'Retailer Item ID')

    dbutils.widgets.text('runid', '', 'Run ID')
    dbutils.widgets.text('timestamp', current_timestamp, 'Timestamp')

    dbutils.widgets.dropdown('MODEL_SOURCE', 'local', ['local', 'prod'], 'Model Source')
    dbutils.widgets.dropdown('INCLUDE_DISCOUNT_FEATURES', 'No', ['Yes', 'No'], 'Include Discount Features')

    RETAILER = dbutils.widgets.get('retailer').strip().lower()
    CLIENT = dbutils.widgets.get('client').strip().lower()
    COUNTRY_CODE = dbutils.widgets.get('countrycode').strip().lower()

    RUN_ID = dbutils.widgets.get('runid').strip()
    TIMESTAMP = dbutils.widgets.get('timestamp').strip()

    MODEL_SOURCE = dbutils.widgets.get('MODEL_SOURCE').upper()
    MODEL_SOURCE = 'LOCAL' if MODEL_SOURCE.startswith('LOCAL') else 'PROD'
    INCLUDE_DISCOUNT_FEATURES = dbutils.widgets.get('INCLUDE_DISCOUNT_FEATURES').strip().lower() == 'yes'

    try:
        STORE = int(dbutils.widgets.get('store').strip())
    except ValueError:
        STORE = None
    try:
        ITEM = str(dbutils.widgets.get('item').strip())
    except ValueError:
        ITEM = None

    if RETAILER == '':
        raise ValueError('\'retailer\' is a required parameter.  Please provide a value.')

    if CLIENT == '':
        raise ValueError('\'client\' is a required parameter.  Please provide a value.')

    if COUNTRY_CODE == '':
        raise ValueError('\'countrycode\' is a required parameter. Please provide a value.')

    if RUN_ID == '':
        RUN_ID = '-'.join([auto_model_prefix, RETAILER, CLIENT, COUNTRY_CODE, TIMESTAMP])

    elif RUN_ID.lower() == 'auto':
        RUN_ID = '-'.join([auto_model_prefix, RETAILER, CLIENT, COUNTRY_CODE, TIMESTAMP])

# PATHS
PATH_ENGINEERED_FEATURES_OUTPUT = '/mnt/processed/training/{run_id}/engineered/'.format(run_id=RUN_ID)

for param in [RETAILER, CLIENT, COUNTRY_CODE, STORE, ITEM, RUN_ID]:
    print(param)

if CLIENT.lower() not in RUN_ID.lower():
    warnings.warn('Potentially uninformative RUN_ID, {} is not in the RUN_ID'.format(CLIENT))

# COMMAND ----------

if not is_nowcast: 
    # Check if the input files exist and if not raise a value error
    store_states_names = '/mnt/prod-ro/artifacts/country_code/reference/orgunitnum_to_state.csv'
    pay_cycles_path = '/mnt/prod-ro/artifacts/country_code/reference/pay_cycle_profiles_by_state.csv'

    check_path_exists(store_states_names, 'csv', 'raise')
    check_path_exists(pay_cycles_path, 'csv', 'raise')

    snap_pay_cycle_path = f'/mnt/artifacts/country_code/reference/snap_paycycle/retailer={RETAILER}/client={CLIENT}/country_code={COUNTRY_CODE}/'

# COMMAND ----------

if not is_nowcast: 
    # Generate and save snap index for the data set
    database = f'{RETAILER}_{CLIENT}_{COUNTRY_CODE}_dv'
    print('Accessing', database)

    store_names_query = '''
    SELECT HOU.ORGANIZATION_UNIT_NUM
       FROM {database}.hub_organization_unit HOU 
       LEFT OUTER JOIN {database}.sat_organization_unit SOU
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
    stores_with_state = stores_with_state.withColumn('STATE', pyf.when(pyf.col('STATE') == 'PR', 'DEFAULT').otherwise(pyf.col('STATE')))

    # Replace non-matches with DEFAULT profile
    stores_with_state = stores_with_state.withColumn('STATE', pyf.when(pyf.col('STATE').isNull(), 'DEFAULT').otherwise(pyf.col('STATE')))

    # Replace non-US-stores with DEFAULT profile
    if COUNTRY_CODE != 'us':
        stores_with_state = stores_with_state.withColumn("STATE", pyf.lit("DEFAULT"))

    # Remove any duplicates and take the first state alphabetically
    windowSpec = Window.partitionBy('ORGANIZATION_UNIT_NUM').orderBy('STATE')
    unique_store_names = stores_with_state\
        .withColumn('column_rank', pyf.row_number().over(windowSpec)) \
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

if not is_nowcast: 
    stores_with_pc.write.mode('overwrite').parquet(snap_pay_cycle_path)

# COMMAND ----------

# Read POS data
data = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data = data.where(pyf.col("SALES_DT") <= pyf.lit("2020-02-27"))

pprint(dict(data.dtypes))

# COMMAND ----------

if STORE:
    data = data.filter(f'ORGANIZATION_UNIT_NUM == "{STORE}"')

if ITEM:
    data = data.filter(f'RETAILER_ITEM_ID == "{ITEM}"')

# COMMAND ----------

if is_nowcast:
    # Get 30 days of sales data before the first actual day that we wish to nowcast
    data = data.where((pyf.col("SALES_DT") <= END_DATE) & (pyf.date_add(pyf.col("SALES_DT"), 30) >= START_DATE))

# COMMAND ----------

# Replace negative POS_ITEM_QTY and POS_AMT values with 0
data = data.withColumn('POS_ITEM_QTY', pyf.when(pyf.col('POS_ITEM_QTY') >= 0, pyf.col('POS_ITEM_QTY')).otherwise(0))
data = data.withColumn('POS_AMT', pyf.when(pyf.col('POS_AMT') >= 0, pyf.col('POS_AMT')).otherwise(0))

# COMMAND ----------

# Import approved product list for this client
approved_product_list_reference_path = f'/mnt/artifacts/country_code/reference/approved_product_list/retailer={RETAILER}/client={CLIENT}/country_code={COUNTRY_CODE}/'

# If the approved product list exists for this client
if check_path_exists(approved_product_list_reference_path, 'delta', 'ignore'):
    approved_product_list = spark.read.format('delta').load(approved_product_list_reference_path) \
        .select(['RetailProductCode'])

    data = data.join(
        approved_product_list,
        data.RETAILER_ITEM_ID == approved_product_list.RetailProductCode)

else:
    print('Approved product list not found for client')

# COMMAND ----------

if not is_nowcast: 
    # This filter requires at least 84 days of non-zero sales in the entire dataset
    subset_meets_threshold = data\
        .select('RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM', 'POS_ITEM_QTY') \
        .filter('POS_ITEM_QTY > 0') \
        .groupBy('RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM') \
        .count() \
        .filter('count >= 84') \
        .drop('count')

    data = data.join(
        subset_meets_threshold,
        ['RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'],
        'inner'
    )

# COMMAND ----------

output_data = pos_to_training_data(
    df=data,
    retailer=RETAILER,
    client=CLIENT,
    country_code=COUNTRY_CODE,
    model_source=MODEL_SOURCE,
    spark=spark,
    spark_context=sc,
    include_discount_features=INCLUDE_DISCOUNT_FEATURES,
    item_store_cols=['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM']
)

data = None
print(f'N = {output_data.cache().count():,}')

# COMMAND ----------

output_data \
    .write.format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .save(PATH_ENGINEERED_FEATURES_OUTPUT)

# COMMAND ----------


