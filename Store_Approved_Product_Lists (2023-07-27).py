# Databricks notebook source
from acosta.alerting.helpers import check_path_exists

# COMMAND ----------

dbutils.widgets.text('SOURCE_SYSTEM_PARAM', 'retaillink')
dbutils.widgets.text('RETAILER_PARAM', 'walmart')
dbutils.widgets.text('COUNTRY_CODE_PARAM', 'us')

source_system = dbutils.widgets.get('SOURCE_SYSTEM_PARAM').strip().lower()
retailer = dbutils.widgets.get('RETAILER_PARAM').strip().lower()
country_code = dbutils.widgets.get('COUNTRY_CODE_PARAM').strip().lower()

# COMMAND ----------

WMS_active_products = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load('/mnt/prod-ro/artifacts/country_code/reference/WMS_Active_Products.csv')

# COMMAND ----------

WMS_active_products.columns

# COMMAND ----------

WMS_active_products = WMS_active_products.withColumnRenamed("Retail Product Code", "RetailProductCode")
WMS_active_products = WMS_active_products.withColumnRenamed("Client Name", "ClientName")

# COMMAND ----------

display(WMS_active_products)

# COMMAND ----------

WMS_active_products = WMS_active_products.select(
    WMS_active_products.FullName,
    WMS_active_products.UniversalProductCode.cast("Long"), 
    WMS_active_products.RetailProductCode, 
    WMS_active_products.ClientName,
    WMS_active_products.Client
)

# COMMAND ----------

WMS_active_products.printSchema()

# COMMAND ----------

client_list = []

client_list.extend([(row.Client) for row in WMS_active_products.select("Client").distinct().collect()])

# COMMAND ----------

APL_reference_path = '/mnt/artifacts/country_code/reference/approved_product_list/retailer={retailer}/client={client}/country_code={country_code}/'

# COMMAND ----------

for client in client_list:
    print(client)
    
    client_filter = "client == '{client_name}'".format(client_name=client)
    
    WMS_active_products\
      .filter(client_filter)\
      .write\
      .mode("overwrite")\
      .format("delta")\
      .save(APL_reference_path.format(retailer=retailer, client=client, country_code=country_code))

# COMMAND ----------

total_items_for_all_clients = 0
total_championed_items = 0
total_approved_items = 0

for client in client_list:
    
    database = '{source_system}_{retailer}_{client}_{country_code}_dv' \
        .format(source_system=source_system, retailer=retailer, client=client, country_code=country_code)
    print('Accessing', database)

    # Get the total number of items in the 'items' table
    total_items_query =  '''
        SELECT HRI.RETAILER_ITEM_ID
           FROM {database}.vw_latest_sat_retailer_item HRI
        '''
    total_items_query = total_items_query.format(database=database)
    total_items = spark.sql(total_items_query)

    total_items_for_all_clients += total_items.count()

    champions_path = '/mnt/artifacts/country_code/champion_models/retailer={retailer}/client={client}/country_code={country_code}'.format(
        retailer=retailer,
        client=client,
        country_code=country_code
    )

    check_path_exists(champions_path, 'delta')

    championed_items = spark.read.format('delta') \
        .load(champions_path) \
        .select(['RETAILER_ITEM_ID']).distinct()

    total_championed_items += championed_items.count()

    # Get the items in the approved product list (APL)
    approved_items = spark.read.format("delta").load(APL_reference_path.format(retailer=retailer, client=client, country_code=country_code))
    
    total_approved_items += approved_items.count()
    
    print("Total number of items: {:,}".format(total_items.count()))
    print("Total championed items: {:,}".format(championed_items.count()))
    print("Num of approved items: {:,}".format(approved_items.count()))
    print("")

# COMMAND ----------

print("Total number of items in database for all clients: {:,}".format(total_items_for_all_clients))
print("Total number of championed items for all clients: {:,}".format(total_championed_items))
print("Total number of approved items for all clients: {:,}".format(total_approved_items))
