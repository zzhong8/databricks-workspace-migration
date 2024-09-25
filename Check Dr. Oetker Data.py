# Databricks notebook source
retailer = 'walmart'

client_list = ['droetker']

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
    
     # Get the items that we have trained models for
    modelled_items = spark.read.parquet("/mnt/artifacts/training_results/")\
    .filter("RETAILER = '{}' and CLIENT = '{}'".format(retailer.upper(), client.upper()))\
    .filter("MODEL_ID is not null")\
    .select(['RETAILER_ITEM_ID']).distinct()
    
    print("Total number of items: {:,}".format(total_items.count()))
    print("Total modelled items:  {:,}".format(modelled_items.count()))
    print("")

# COMMAND ----------


