# Databricks notebook source
import uuid
import warnings
from datetime import datetime

# from pyspark.sql.functions import *
from pyspark.sql import types as pyt
from pyspark.sql import functions as pyf
import acosta
from acosta.alerting.preprocessing import read_pos_data

print(acosta.__version__)

# COMMAND ----------

retailer = 'tesco'
client = 'nestlecore'
country_code = 'uk'

# COMMAND ----------

database = '{retailer}_{client}_{country_code}_dv'.format(
    retailer=retailer.strip().lower(),
    client=client.strip().lower(),
    country_code=country_code.strip().lower()
)

print(database)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESC tesco_nestlecore_uk_dv.vw_sat_link_epos_summary

# COMMAND ----------

def read_pos_data(retailer, client, country_code, sql_context):
    """
    Reads in POS data from the Data Vault and returns a DataFrame

    The output DataFrame is suitable for use in the pos_to_training_data function.

    Note that when reading from the data vault, we use a pre-defined view that will
    sort out the restatement records for us.  This means we don't have to write our
    own logic for handling duplicate rows within the source dataset.

    :param string retailer: the name of the retailer to pull
    :param string client: the name of the client to pull
    :param string country_code: the two character name of the country code to pull
    :param pyspark.sql.context.HiveContext sql_context: PySparkSQL context that can be used to connect to the Data Vault

    :return DataFrame:
    """

    retailer = retailer.strip().upper()
    client = client.strip().upper()
    country_code = country_code.strip().upper()

    database = '{retailer}_{client}_{country_code}_dv'.format(
        retailer=retailer.strip().lower(),
        client=client.strip().lower(),
        country_code=country_code.strip().lower()
    )

    sql_statement = """
        SELECT
              \'{retailer}\' AS RETAILER,
              \'{client}\' AS CLIENT,
              \'{country_code}\' AS COUNTRY_CODE,
              HRI.RETAILER_ITEM_ID,
              HOU.ORGANIZATION_UNIT_NUM,
              VSLES.SALES_DT,
              VSLES.POS_ITEM_QTY,
              VSLES.POS_AMT,
              VSLES.ON_HAND_INVENTORY_QTY,
              VSLES.POS_AMT / VSLES.POS_ITEM_QTY AS UNIT_PRICE,
              'DATA VAULT' AS ROW_ORIGIN,
              'COMPUTED' AS PRICE_ORIGIN,
              'IGNORE' AS TRAINING_ROLE
           FROM {database}.vw_sat_link_epos_summary VSLES
           LEFT OUTER JOIN {database}.hub_retailer_item HRI
                ON HRI.HUB_RETAILER_ITEM_HK = VSLES.HUB_RETAILER_ITEM_HK
           LEFT OUTER JOIN {database}.hub_organization_unit HOU
                ON HOU.HUB_ORGANIZATION_UNIT_HK = VSLES.HUB_ORGANIZATION_UNIT_HK
          WHERE VSLES.SALES_DT >= '2020-11-25'
    """

    sql_statement = sql_statement.format(database=database, retailer=retailer, client=client, country_code=country_code)

    return sql_context.sql(sql_statement)

# COMMAND ----------

# Read POS data
data_vault_data1 = read_pos_data(retailer, client, country_code, sqlContext).repartition('SALES_DT')

# COMMAND ----------

display(data_vault_data1)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_UK_retail_analytics_im;
# MAGIC
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_UK_retail_analytics_im;
# MAGIC
# MAGIC SHOW COLUMNS IN vw_fact_question_response

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_UK_retail_analytics_im;
# MAGIC
# MAGIC SHOW COLUMNS IN vw_dimension_itemlevel

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use acosta_UK_retail_analytics_im;
# MAGIC
# MAGIC select distinct item_dimension_id, upc from vw_dimension_itemlevel where client_id = 16320 and category_description = 'Cereal'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use BoBv2;
# MAGIC
# MAGIC SHOW COLUMNS IN BoBv2.vw_BOBv2_Product

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from BoBv2.Company where FullName like '%estle%'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select ProductId, RefExternal from BoBv2.vw_BOBv2_Product where CompanyId = 609

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from BoBv2.Product where CompanyId = 609

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *
# MAGIC from BoBv2.vw_BOBv2_Product a 
# MAGIC where a.CompanyId = 609 -- nestlecore

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *
# MAGIC from BoBv2.Product b
# MAGIC where b.CompanyId = 609 -- nestlecore

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select a.ProductId, a.RefExternal, lpad(substr(b.UniversalProductCode, 0, 12), 12, 0) as UPC
# MAGIC from BoBv2.vw_BOBv2_Product a 
# MAGIC join BoBv2.Product b 
# MAGIC on a.ProductId = b.ProductId
# MAGIC where a.CompanyId = 609 -- nestlecore

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from acosta_UK_retail_analytics_im.vw_dimension_itemlevel where client_id = 16320 and category_description = 'Cereal'

# COMMAND ----------

query = f'''
    select a.ProductId, a.RefExternal, lpad(substr(b.UniversalProductCode, 0, 12), 12, 0) as UPC
    from BoBv2.vw_BOBv2_Product a 
    join BoBv2.Product b 
    on a.ProductId = b.ProductId
    where a.CompanyId = 609 -- nestlecore
    ''' 

product_mapping_nestlecore = spark.sql(query)

# COMMAND ----------

print(product_mapping_nestlecore.count())

# COMMAND ----------

temp_query = f'''
    select a.ProductId, a.RefExternal, lpad(substr(b.UniversalProductCode, 0, 12), 12, 0) as UPC
    from BoBv2.vw_BOBv2_Product a 
    join BoBv2.Product b 
    on a.ProductId = b.ProductId
    where a.CompanyId = 609 -- nestlecore
and UniversalProductCode like '%761303845329%'
    ''' 

temp_product_mapping_nestlecore = spark.sql(temp_query)

# COMMAND ----------

display(temp_product_mapping_nestlecore)

# COMMAND ----------

query3 = f'''
    select distinct item_dimension_id, upc, upc_description from acosta_UK_retail_analytics_im.vw_dimension_itemlevel where client_id = 16320 and category_description = 'Cereal'
and upc like '%761303845329%'
    ''' 

item_level_nestlecore_cereals = spark.sql(query3)

# COMMAND ----------

display(item_level_nestlecore_cereals)

# COMMAND ----------

data_vault_data_nestlecore = data_vault_data1.join(product_mapping_nestlecore, data_vault_data1['RETAILER_ITEM_ID'] == product_mapping_nestlecore['RefExternal'])

# print(data_vault_data_nestlecore.cache().count())

# COMMAND ----------

print(data_vault_data_nestlecore.count())

# COMMAND ----------

query3 = f'''
    select distinct item_dimension_id, upc from acosta_UK_retail_analytics_im.vw_dimension_itemlevel where client_id = 16320 and category_description = 'Cereal'

    ''' 

item_level_nestlecore_cereals = spark.sql(query3)

# COMMAND ----------

print(item_level_nestlecore_cereals.cache().count())

# COMMAND ----------

item_level_nestlecore_cereals.printSchema()

# COMMAND ----------

display(item_level_nestlecore_cereals)

# COMMAND ----------

data_vault_data_nestlecore.printSchema()

# COMMAND ----------

display(data_vault_data_nestlecore)

# COMMAND ----------

display(item_level_nestlecore_cereals)

# COMMAND ----------

data_vault_data_nestlecore_cereals = data_vault_data_nestlecore.join(item_level_nestlecore_cereals, data_vault_data_nestlecore['UPC'] == item_level_nestlecore_cereals['upc'])

print(data_vault_data_nestlecore_cereals.cache().count())

# COMMAND ----------

display(data_vault_data_nestlecore_cereals.cache())

# COMMAND ----------

display(data_vault_data_nestlecore_cereals.filter(data_vault_data_nestlecore_cereals.POS_ITEM_QTY > 0))

# COMMAND ----------


