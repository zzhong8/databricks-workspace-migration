# Databricks notebook source
from pyspark.sql import functions as pyf
from pyspark.sql import types as pyt

# COMMAND ----------

dbutils.widgets.text('retailer', 'walmart', 'Retailer')
dbutils.widgets.text('client', 'clorox', 'Client')

RETAILER = dbutils.widgets.get('retailer').strip().lower()
CLIENT = dbutils.widgets.get('client').strip().lower()

if RETAILER == '':
    raise ValueError('"retailer" is a required parameter. Please provide a value.')

if CLIENT == '':
    raise ValueError('"client" is a required parameter. Please provide a value.')

for param in [RETAILER, CLIENT]:
    print(param)

# COMMAND ----------

readDatabaseName = '{}_{}_retail_alert_im'.format(RETAILER.lower(), CLIENT.lower())

readTableName = '{}.DRFE_FORECAST_BASELINE_UNIT'.format(readDatabaseName)

readTablequery =  '''SELECT * FROM {readTableName}'''
readTablequery = readTablequery.format(readTableName=readTableName)
    
drfe_forecast_baseline_unit_table = spark.sql(readTablequery)

# COMMAND ----------

drfe_forecast_baseline_unit_table.printSchema()

# COMMAND ----------

sales_dates = drfe_forecast_baseline_unit_table.select('SALES_DT').distinct().orderBy('SALES_DT', ascending=False)

# COMMAND ----------

display(sales_dates)

# COMMAND ----------

display(
    drfe_forecast_baseline_unit_table
        .groupBy(pyf.col('SALES_DT') )
        .count()
        .orderBy('SALES_DT', ascending=False)
)

# COMMAND ----------

drfe_forecast_baseline_unit_table_2019_06_30 = drfe_forecast_baseline_unit_table.filter(drfe_forecast_baseline_unit_table.SALES_DT == '2019-06-30')

# COMMAND ----------

drfe_forecast_baseline_unit_table_2019_06_30.count()

# COMMAND ----------

display(drfe_forecast_baseline_unit_table_2019_06_30)

# COMMAND ----------

# LOOK UP HASH KEYS
databaseName = '{}_{}_dv'.format(RETAILER.lower(), CLIENT.lower())
itemMasterTableName = '{}.hub_retailer_item'.format(databaseName)
storeMasterTableName = '{}.hub_organization_unit'.format(databaseName)

converted_dataframe = drfe_forecast_baseline_unit_table_2019_06_30.alias('PRDF') \
    .join(sqlContext.read.table(storeMasterTableName).alias('OUH'),
          pyf.col('OUH.HUB_ORGANIZATION_UNIT_HK') == pyf.col('PRDF.HUB_ORGANIZATION_UNIT_HK'), 'inner') \
    .join(sqlContext.read.table(itemMasterTableName).alias('RIH'),
          pyf.col('RIH.HUB_RETAILER_ITEM_HK') == pyf.col('PRDF.HUB_RETAILER_ITEM_HK'), 'inner') \
    .select('PRDF.*', 'OUH.ORGANIZATION_UNIT_NUM', 'RIH.RETAILER_ITEM_ID')

# COMMAND ----------

display(converted_dataframe)

# COMMAND ----------

converted_dataframe.printSchema()

# COMMAND ----------

display(converted_dataframe.select('ORGANIZATION_UNIT_NUM').distinct())

# COMMAND ----------

drfe_forecast_baseline_unit_table_one_hash_key = drfe_forecast_baseline_unit_table.filter(drfe_forecast_baseline_unit_table.HUB_ORGANIZATION_UNIT_HK == 'eccbc87e4b5ce2fe28308fd9f2a7baf3').filter(drfe_forecast_baseline_unit_table.HUB_RETAILER_ITEM_HK == '129ecfb03baaacf23ce00321142d5dbf')

# COMMAND ----------

display(drfe_forecast_baseline_unit_table_one_hash_key.orderBy('SALES_DT', ascending=False))

# COMMAND ----------

drfe_forecast_baseline_unit_table_2019_06_14 = drfe_forecast_baseline_unit_table.filter(drfe_forecast_baseline_unit_table.SALES_DT == '2019-06-14')

# COMMAND ----------

drfe_forecast_baseline_unit_table_2019_06_14.count()

# COMMAND ----------

display(drfe_forecast_baseline_unit_table_2019_06_14)

# COMMAND ----------

drfe_forecast_baseline_unit_table_one_hash_key_2 = drfe_forecast_baseline_unit_table.filter(drfe_forecast_baseline_unit_table.HUB_ORGANIZATION_UNIT_HK == '88bade49e98db8790df275fcebb37a13').filter(drfe_forecast_baseline_unit_table.HUB_RETAILER_ITEM_HK == 'aeca9427bd3bbba00a54bbb919599792')

# COMMAND ----------

display(drfe_forecast_baseline_unit_table_one_hash_key_2)

# COMMAND ----------

drfe_forecast_baseline_unit_table_one_hash_key_3 = drfe_forecast_baseline_unit_table.filter(drfe_forecast_baseline_unit_table.HUB_ORGANIZATION_UNIT_HK == '244edd7e85dc81602b7615cd705545f5').filter(drfe_forecast_baseline_unit_table.HUB_RETAILER_ITEM_HK == '961ec739426913a8232c36830dbc0af0')

# COMMAND ----------

display(drfe_forecast_baseline_unit_table_one_hash_key_3)

# COMMAND ----------


