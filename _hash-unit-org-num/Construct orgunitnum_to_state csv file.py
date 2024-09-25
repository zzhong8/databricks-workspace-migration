# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text('retailer', 'walmart', 'Retailer')
dbutils.widgets.text('client', 'clorox', 'Client')
dbutils.widgets.text('countrycode', 'us', 'Country Code')

RETAILER = dbutils.widgets.get('retailer').strip().lower()
CLIENT = dbutils.widgets.get('client').strip().lower()
COUNTRY_CODE = dbutils.widgets.get('countrycode').strip().lower()
    
if RETAILER == '':
    raise ValueError('\'retailer\' is a required parameter.  Please provide a value.')

if CLIENT == '':
    raise ValueError('\'client\' is a required parameter.  Please provide a value.')

if COUNTRY_CODE == '':
    raise ValueError('\'countrycode\' is a required parameter. Please provide a value.')
    
for param in [RETAILER, CLIENT, COUNTRY_CODE]:
    print(param)

# COMMAND ----------

# Generate and save snap index for the data set
database = '{retailer}_{client}_{country_code}_dv'.format(retailer=RETAILER, client=CLIENT, country_code=COUNTRY_CODE)
print('Accessing', database)

store_names_query = '''
    SELECT HOU.ORGANIZATION_UNIT_NUM, SOU.STATE
       FROM {database}.hub_organization_unit HOU 
       LEFT OUTER JOIN {database}.sat_organization_unit SOU
       ON SOU.HUB_ORGANIZATION_UNIT_HK = HOU.HUB_ORGANIZATION_UNIT_HK
    '''

store_names_query = store_names_query.format(database=database)
store_names = spark.sql(store_names_query)

print(store_names.count())

# COMMAND ----------

display(store_names)

# COMMAND ----------


