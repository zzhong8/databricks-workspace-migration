# Databricks notebook source
from pyspark.sql import functions as pyf
from pyspark.sql.types import *
from pyspark.sql.window import Window
import datetime as dtm
import uuid

from acosta.alerting.helpers import check_path_exists

# CONSTANTS
current_datetime = dtm.datetime.now()

# COMMAND ----------

# Loading all variables and constants
dbutils.widgets.text('RETAILER_PARAM', 'walmart', 'Retailer(s)')
dbutils.widgets.text('CLIENT_PARAM', '', 'Client(s)')
dbutils.widgets.text('COUNTRY_CODE_PARAM', 'us', 'Country Code')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Rules
# MAGIC <p>
# MAGIC * You can run all clients for any one retailer
# MAGIC * You can run all clients for all retailers
# MAGIC * You can NOT list multiple retailers and multiple clients
# MAGIC * To run multiple clients you must have only ONE retailer
# MAGIC * For now we allow only one country to be processed at a time,
# MAGIC * or Country Code is blank than we process all country codes 

# COMMAND ----------

client_list = dbutils.widgets.get('CLIENT_PARAM').lower().split(',')
CLIENT_PARAM = [client.strip() for client in client_list if client.strip() != '']

RETAILER_PARAM = dbutils.widgets.get('RETAILER_PARAM').strip()
COUNTRY_CODE_PARAM = dbutils.widgets.get('COUNTRY_CODE_PARAM').strip()

retailer_client_country_list = []

if COUNTRY_CODE_PARAM == '':
    if RETAILER_PARAM != '' and len(CLIENT_PARAM) > 0:
        raise Exception('This option is currently not available')

    elif RETAILER_PARAM == '' and len(CLIENT_PARAM) > 0:
        raise Exception('You must have a retailer specified for your client(s)')

    elif RETAILER_PARAM == '' and len(CLIENT_PARAM) == 0:
        # Get a list of all the clients under each retailer in the training_results directory in order
        # to run every retailer/client combination
        all_training_results = spark.read.parquet('/mnt/artifacts/country_code/training_results/')
        retailer_client_country_list.extend(
            [(row.RETAILER, row.CLIENT, row.COUNTRY_CODE) for row in all_training_results.select('RETAILER', 'CLIENT', 'COUNTRY_CODE').distinct().collect()]
        )

    elif RETAILER_PARAM != '' and len(CLIENT_PARAM) == 0:
        # Get a list of all the clients under this retailer in order to run them
        all_training_results = spark.read.parquet('/mnt/artifacts/country_code/training_results/')\
            .filter('RETAILER == "{}"'.format(RETAILER_PARAM))
        retailer_client_country_list.extend(
            [(row.RETAILER, row.CLIENT, row.COUNTRY_CODE) for row in all_training_results.select('RETAILER', 'CLIENT', 'COUNTRY_CODE').distinct().collect()]
        )

    else:
        raise Exception('Something went wrong here!')

else:
    if RETAILER_PARAM != '' and len(CLIENT_PARAM) > 0:
        # Create list of retailer/client/country combinations and run these combinations
        retailer_client_country_list.extend([(RETAILER_PARAM, client, COUNTRY_CODE_PARAM) for client in CLIENT_PARAM])

    elif RETAILER_PARAM == '' and len(CLIENT_PARAM) > 0:
        raise Exception('You must have a retailer specified for your client(s)')

    elif RETAILER_PARAM == '' and len(CLIENT_PARAM) == 0:
        # Get a list of all the clients under each retailer in the training_results directory in order
        # to run every retailer/client combination
        all_training_results = spark.read.parquet('/mnt/artifacts/country_code/training_results/')\
            .filter('COUNTRY_CODE == "{}"'.format(COUNTRY_CODE_PARAM))
        retailer_client_country_list.extend(
            [(row.RETAILER, row.CLIENT, row.COUNTRY_CODE) for row in all_training_results.select('RETAILER', 'CLIENT', 'COUNTRY_CODE').distinct().collect()]
        )

    elif RETAILER_PARAM != '' and len(CLIENT_PARAM) == 0:
        # Get a list of all the clients under this retailer in order to run them
        all_training_results = spark.read.parquet('/mnt/artifacts/country_code/training_results/')\
            .filter('RETAILER == "{}" AND COUNTRY_CODE == "{}"'.format(RETAILER_PARAM, COUNTRY_CODE_PARAM))
        retailer_client_country_list.extend(
            [(row.RETAILER, row.CLIENT, row.COUNTRY_CODE) for row in all_training_results.select('RETAILER', 'CLIENT', 'COUNTRY_CODE').distinct().collect()]
        )

    else:
        raise Exception('Something went wrong here!')

print(retailer_client_country_list)

# COMMAND ----------

# Collect all current champions and extract the schema for controlling the fields made available
current_champions = spark.sql('SELECT * FROM retail_forecast_engine.champion_models limit 1')
schema_champion = [element.name for element in current_champions.schema if element.name != 'MODEL_PATH']

# COMMAND ----------

def create_champion_logic(schema_champion, retailer, client, country_code):
    working_data = spark.read.format('parquet')\
                    .load('/mnt/artifacts/country_code/training_results/') \
        .filter('RETAILER = "{}" and CLIENT = "{}" and COUNTRY_CODE = "{}"'.format(retailer, client, country_code)) \
        .withColumn('DATE_MADE_CHAMPION', pyf.lit(current_datetime)) \
        .withColumn('COLUMN_NAMES', pyf.array_join('COLUMN_NAMES', ',')) \
        .filter('MODEL_ID is not null') \
        .select(*schema_champion) \
        .withColumn('RETAILER', pyf.lower(pyf.col('RETAILER'))) \
        .withColumn('CLIENT', pyf.lower(pyf.col('CLIENT'))) \
        .withColumn('COUNTRY_CODE', pyf.lower(pyf.col('COUNTRY_CODE')))

    # Select the most recent models
    window_recency = Window.partitionBy('RETAILER_ITEM_ID', 'RETAILER', 'CLIENT', 'COUNTRY_CODE').orderBy(
        pyf.desc('DATE_MODEL_TRAINED')
    )
    df_most_recent_champions = working_data[
        ['RETAILER_ITEM_ID', 'TRAINING_ID', 'RETAILER', 'CLIENT', 'COUNTRY_CODE',
         'DATE_MODEL_TRAINED']].distinct()
    df_most_recent_champions = df_most_recent_champions.withColumn(
        'RECENCY',
        pyf.row_number().over(window_recency)
    ).drop('DATE_MODEL_TRAINED')

    df_new_champion = working_data.join(
        df_most_recent_champions,
        ['RETAILER_ITEM_ID', 'TRAINING_ID', 'RETAILER', 'CLIENT', 'COUNTRY_CODE']
    )
    df_new_champion = df_new_champion.filter(pyf.col('RECENCY') == 1)

    # Select model with the best out-of-sample mse
    window_metric_rank = Window.partitionBy('RETAILER_ITEM_ID',
                                            'RETAILER',
                                            'CLIENT',
                                            'COUNTRY_CODE') \
                        .orderBy('METRICS_RMSE_TEST')
    df_new_champion = df_new_champion \
        .withColumn('RMSE_RANK', pyf.row_number().over(window_metric_rank)) \
        .filter(pyf.col('RMSE_RANK') == 1) \
        .select(*schema_champion) \
        .alias('new')

    return df_new_champion

# COMMAND ----------

MODEL_SOURCE = 'prod'
CLIENT = 'milos'

#load up the existing champions models to identify which items do not have models
champions_path = '/mnt{mod}/artifacts/country_code/champion_models/retailer={retailer}/client={client}/country_code={country_code}'.format(
    mod='' if MODEL_SOURCE == 'local' else '/prod-ro',
    retailer=RETAILER_PARAM,
    client=CLIENT,
    country_code=COUNTRY_CODE_PARAM
)

check_path_exists(champions_path, 'delta')

champions = spark.read.format('delta') \
    .load(champions_path) \
    .drop('RETAILER', 'CLIENT', 'COUNTRY_CODE') \
    .withColumnRenamed('ORGANIZATION_UNIT_NUM', 'ORGANIZATION_UNIT_LIST')

champions2 = champions.drop("ORGANIZATION_UNIT_LIST", "MODEL_OBJECT", "COLUMN_NAMES")

champions2.count()

# COMMAND ----------

for retailer_i, client_i, country_code_i in retailer_client_country_list:
    df_new_champions = create_champion_logic(schema_champion, retailer_i, client_i, country_code_i)
    # TODO: Do we enable auditing - i.e. recording which champions were added, changed, or deleted?

    df_new_champions\
        .write\
        .mode('overwrite')\
        .format('delta')\
        .save('/mnt/artifacts/country_code/champion_models/retailer={}/client={}/country_code={}'\
              .format(retailer_i, client_i, country_code_i))

# COMMAND ----------

for retailer_i, client_i, country_code_i in retailer_client_country_list:
    results = spark.read.format('delta')\
        .load('/mnt/artifacts/country_code/champion_models/retailer={}/client={}/country_code={}'\
              .format(retailer_i, client_i, country_code_i))

    print('=== {} ==='.format(client_i))
    print(results[['MODEL_METADATA']].distinct().toPandas())
    print()
    print(results[['DATE_MODEL_TRAINED']].distinct()\
          .sort(pyf.col('DATE_MODEL_TRAINED')).toPandas())
    print()
    print('N items: {:,}'.format(results[['RETAILER_ITEM_ID']]\
                                 .distinct().count()))
    print('=' * 60)

# COMMAND ----------

#load up the existing champions models to identify which items do not have models
champions_path = '/mnt{mod}/artifacts/country_code/champion_models/retailer={retailer}/client={client}/country_code={country_code}'.format(
    mod='' if MODEL_SOURCE == 'local' else '/prod-ro',
    retailer=RETAILER_PARAM,
    client=CLIENT,
    country_code=COUNTRY_CODE_PARAM
)

check_path_exists(champions_path, 'delta')

champions = spark.read.format('delta') \
    .load(champions_path) \
    .drop('RETAILER', 'CLIENT', 'COUNTRY_CODE') \
    .withColumnRenamed('ORGANIZATION_UNIT_NUM', 'ORGANIZATION_UNIT_LIST')

champions2 = champions.drop("ORGANIZATION_UNIT_LIST", "MODEL_OBJECT", "COLUMN_NAMES")

champions2.count()

# COMMAND ----------

display(champions2)

# COMMAND ----------


