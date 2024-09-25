# Databricks notebook source
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

database = '{retailer}_{client}_{country_code}_dv'.format(retailer=RETAILER.strip().lower(),
                                                          client=CLIENT.strip().lower(),
                                                          country_code=COUNTRY_CODE.strip().lower())
sql_statement = """
    SELECT
          VSLES.SALES_DT,
          VSLES.POS_ITEM_QTY,
          VSLES.POS_AMT,
          VSLES.ON_HAND_INVENTORY_QTY,
          VSLES.POS_AMT / VSLES.POS_ITEM_QTY AS UNIT_PRICE
       FROM {database}.vw_sat_link_epos_summary VSLES
    """

sql_statement = sql_statement.format(database=database, retailer=RETAILER, client=CLIENT, country_code=COUNTRY_CODE)

TEST_DF = sqlContext.sql(sql_statement)

# COMMAND ----------

print(TEST_DF.filter('POS_ITEM_QTY is NULL').count())
print(TEST_DF.filter('POS_ITEM_QTY is not NULL').count())
