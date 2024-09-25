# Databricks notebook source
dbutils.widgets.text('retailer', 'walmart', 'Retailer')
dbutils.widgets.text('client', 'clorox', 'Client')
dbutils.widgets.text('countrycode', 'us', 'Country Code')

retailer = dbutils.widgets.get('retailer').strip().lower()
client = dbutils.widgets.get('client').strip().lower()
country_code = dbutils.widgets.get('countrycode').strip().lower()

if retailer == '':
    raise ValueError('\'retailer\' is a required parameter.  Please provide a value.')

if client == '':
    raise ValueError('\'client\' is a required parameter.  Please provide a value.')

if country_code == '':
    raise ValueError('\'countrycode\' is a required parameter. Please provide a value.')

for param in [retailer, client, country_code]:
    print(param)

# COMMAND ----------

# database = '{retailer}_{client}_{country_code}_dv'.format(retailer=RETAILER.strip().lower(),
#                                                           client=CLIENT.strip().lower(),
#                                                           country_code=COUNTRY_CODE.strip().lower())
# sales_sql_statement = """
#     SELECT
#           VSLES.SALES_DT,
#           VSLES.POS_ITEM_QTY,
#           VSLES.POS_AMT,
#           VSLES.ON_HAND_INVENTORY_QTY,
#           VSLES.POS_AMT / VSLES.POS_ITEM_QTY AS UNIT_PRICE
#        FROM {database}.vw_sat_link_epos_summary VSLES
#     """

# sales_sql_statement = sales_sql_statement.format(database=database, retailer=RETAILER, client=CLIENT, country_code=COUNTRY_CODE)

# sales_df = sqlContext.sql(sales_sql_statement)

# COMMAND ----------

# print(sales_df.filter('POS_ITEM_QTY is NULL').count())
# print(sales_df.filter('POS_ITEM_QTY is not NULL').count())

# COMMAND ----------

database_im = '{retailer}_{client}_{country_code}_retail_alert_im' \
    .format(retailer=retailer.strip().lower(),
            client=client.strip().lower(),
            country_code=country_code.strip().lower()
            )

loess_forecast_sql_statement = """
    SELECT
       LFBU.HUB_ORGANIZATION_UNIT_HK,
       LFBU.HUB_RETAILER_ITEM_HK,
       LFBU.BASELINE_POS_ITEM_QTY,
       LFBU.SALES_DT
          
       FROM {database}.loess_forecast_baseline_unit LFBU
    """

loess_forecast_sql_statement = loess_forecast_sql_statement.format(database=database_im, retailer=retailer, client=client, country_code=country_code)

loess_forecast_df = sqlContext.sql(loess_forecast_sql_statement)

# COMMAND ----------

forecast_dates = loess_forecast_df.select('SALES_DT').distinct().orderBy('SALES_DT', ascending=False)

# COMMAND ----------

forecast_dates.count()

# COMMAND ----------

display(forecast_dates)

# COMMAND ----------


