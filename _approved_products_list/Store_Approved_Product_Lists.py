# Databricks notebook source
WMS_active_products = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load('/mnt/prod-ro/artifacts/reference/WMS_Active_Products.csv')

# COMMAND ----------

WMS_active_products.columns

# COMMAND ----------

WMS_active_products = WMS_active_products.withColumnRenamed("Retail Product Code", "RetailProductCode")
WMS_active_products = WMS_active_products.withColumnRenamed("Client Name", "ClientName")

# COMMAND ----------

display(WMS_active_products)

# COMMAND ----------

client_list = []

client_list.extend([(row.Client) for row in WMS_active_products.select("Client").distinct().collect()])

# COMMAND ----------

APL_reference_path = '/mnt/artifacts/reference/approved_product_list/retailer={retailer}/client={client}/'

# COMMAND ----------

retailer = 'walmart'

for client in client_list:
    client_filter = "client == '{client_name}'".format(client_name=client)
    
    WMS_active_products\
      .filter(client_filter)\
      .write\
      .mode("overwrite")\
      .format("delta")\
      .save(APL_reference_path.format(retailer=retailer, client=client))

# COMMAND ----------

total_items_for_all_clients = 0
total_championed_items = 0
total_approved_items = 0

for client in client_list:
  
    database = '{retailer}_{client}_dv'.format(retailer=retailer, client=client)
    print('Accessing', database)

    # Get the total number of items in the 'items' table
    total_items_query =  '''
        SELECT HRI.RETAILER_ITEM_ID
           FROM {database}.hub_retailer_item HRI
        '''
    total_items_query = total_items_query.format(database=database)
    total_items = spark.sql(total_items_query)

    total_items_for_all_clients += total_items.count()

    # Get the items that we have champion models for
    championed_items = spark.read.parquet("/mnt/artifacts/champion_models/")\
    .filter("RETAILER = '{}' and CLIENT = '{}'".format(retailer.upper(), client.upper()))\
    .select(['RETAILER_ITEM_ID']).distinct()

    total_championed_items += championed_items.count()

    # Get the items in the approved product list (APL)
    approved_items = spark.read.format("delta").load(APL_reference_path.format(retailer=retailer, client=client))
    
    total_approved_items += approved_items.count()
    
    print("Total number of items: {:,}".format(total_items.count()))
    print("Total championed items: {:,}".format(championed_items.count()))
    print("Num of approved items: {:,}".format(approved_items.count()))
    print("")

# COMMAND ----------

print("Total number of items in database for all clients: {:,}".format(total_items_for_all_clients))
print("Total number of championed items for all clients: {:,}".format(total_championed_items))
print("Total number of approved items for all clients: {:,}".format(total_approved_items))
