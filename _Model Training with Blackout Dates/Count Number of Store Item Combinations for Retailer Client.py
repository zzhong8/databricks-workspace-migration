# Databricks notebook source
import acosta
from acosta.alerting.preprocessing import pos_to_training_data, read_pos_data

# COMMAND ----------

# Interface
dbutils.widgets.text('RETAILER_PARAM', 'walmart', 'Retailer')
dbutils.widgets.text('CLIENT_PARAM', 'clorox', 'Client')
dbutils.widgets.text('COUNTRY_CODE_PARAM', 'us', 'Country Code')

# Process input parameters
RETAILER = dbutils.widgets.get('RETAILER_PARAM').strip().casefold()
CLIENT = dbutils.widgets.get('CLIENT_PARAM').strip().casefold()
COUNTRY_CODE = dbutils.widgets.get('COUNTRY_CODE_PARAM').strip().casefold()

# COMMAND ----------

for param in [RETAILER, CLIENT, COUNTRY_CODE]:
    print(param)

# COMMAND ----------

data_vault_data = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).filter('POS_ITEM_QTY > 0').repartition('SALES_DT')
data_vault_data.select("ORGANIZATION_UNIT_NUM","RETAILER_ITEM_ID").distinct().count()

# COMMAND ----------

# A few reference counts:

# walmart-milos-us: 25908
# walmart-atkinsnutritional-us: 343516
# walmart-minutemaidcompany-us: 489517
# walmart-gpconsumerproductsoperation-us: 496017
# walmart-tysonhillshire-us: 2760507
# walmart-starbucks-us: 8769645

# asda-premier-uk: 443378
# morrisons-premier-uk: 252292
# sainburys-premier-uk: 57189
# tesco-premier-uk: 800412
