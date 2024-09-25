# Databricks notebook source
# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC desc vw_BOBv2_Product

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC desc Product

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc BOBv2.chain

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from BOBv2.chain

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select a.CompanyId, a.ParentChainId, a.RefExternal, b.*, c.*
# MAGIC from 
# MAGIC BOBv2.vw_BOBv2_Product a 
# MAGIC join 
# MAGIC BOBv2.Product b
# MAGIC on a.ProductId = b.ProductId
# MAGIC join
# MAGIC mdm_raw.item c
# MAGIC on b.UniversalProductCode = concat (c.UPC, c.UPCCheckDigit)
# MAGIC where a.CompanyId = 609 -- Nestle Core
# MAGIC and a.ParentChainId = 23 -- Asda
# MAGIC and a.RefExternal in
# MAGIC (5510017,
# MAGIC 5826568, 
# MAGIC 9220190, 
# MAGIC 6019058, 
# MAGIC 5733204)
# MAGIC and c.ClientID = 16320 -- Nestle UK

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc asda_nestlecore_uk_dv.sat_link_supply_chain

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from asda_nestlecore_uk_dv.sat_link_supply_chain

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc asda_nestlecore_uk_dv.link_supply_chain

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select a.CompanyId, a.ParentChainId, a.RefExternal, b.*, c.*
# MAGIC from 
# MAGIC BOBv2.vw_BOBv2_Product a 
# MAGIC join 
# MAGIC BOBv2.Product b
# MAGIC on a.ProductId = b.ProductId
# MAGIC join
# MAGIC mdm.item c
# MAGIC on b.UniversalProductCode = concat (c.UPC, c.UPCCheckDigit)
# MAGIC where a.CompanyId = 609 -- Nestle Core
# MAGIC and a.ParentChainId = 23 -- Asda
# MAGIC and a.RefExternal in
# MAGIC (5510017,
# MAGIC 5826568, 
# MAGIC 9220190, 
# MAGIC 6019058, 
# MAGIC 5733204)
# MAGIC and c.ClientID = 16320 -- Nestle UK

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC select count(*) from mdm_raw.hp_client

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from mdm_raw.hp_client
# MAGIC where CountryID = 30

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc mdm_raw.item

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct ClientID from mdm_raw.item
# MAGIC order by ClientID

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from mdm_raw.item

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from mdm.item

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from mdm_raw.item where 
# MAGIC ClientID = 16320 -- Nestle UK
# MAGIC and
# MAGIC concat (UPC, UPCCheckDigit) in
# MAGIC (
# MAGIC '5011476100885',
# MAGIC '8717405008433',
# MAGIC '5011546415505',
# MAGIC '8715000998630',
# MAGIC '7613034919625'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from mdm_raw.item where PreviousUPC in
# MAGIC (
# MAGIC '5011476100885',
# MAGIC '8717405008433',
# MAGIC '5011546415505',
# MAGIC '8715000998630',
# MAGIC '7613034919625'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC use BOBv2;
# MAGIC
# MAGIC select * from Product  
# MAGIC where CompanyId = 609 -- Nestle Core
# MAGIC -- and ParentChainId = 23 -- Asda
# MAGIC and ProductId in
# MAGIC (191346,
# MAGIC 191450, 
# MAGIC 191469, 
# MAGIC 191883, 
# MAGIC 191955)

# COMMAND ----------

import uuid
import warnings
import datetime

import numpy as np
import pandas as pd

from pyspark.sql import Window
from pyspark.sql import types as pyt
from pyspark.sql import functions as pyf
from pyspark.sql import SQLContext

import acosta
from acosta.alerting.preprocessing import read_pos_data

print(acosta.__version__)

# COMMAND ----------

todays_date = datetime.date.today().strftime(format='%Y-%m-%d')

# COMMAND ----------

sqlContext = SQLContext(sc)

# COMMAND ----------

RETAILER = 'asda'
CLIENT = 'nestlecore'
COUNTRY_CODE = 'uk'

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
    """

    sql_statement = sql_statement.format(database=database, retailer=retailer, client=client, country_code=country_code)

    return sql_context.sql(sql_statement)

# COMMAND ----------

# Read POS data
data_vault_data = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')

# COMMAND ----------

data_vault_data1 = data_vault_data.where((pyf.col("SALES_DT") <= pyf.lit("2022-10-31")) & (pyf.col("SALES_DT") >= pyf.lit("2022-10-01")))

print(data_vault_data1.cache().count())

# COMMAND ----------

data_vault_data2 = data_vault_data1.where((pyf.col("RETAILER_ITEM_ID").isin(
5510017,
5826568, 
9220190, 
6019058, 
5733204, 
6524742)))

print(data_vault_data2.cache().count())

# COMMAND ----------

display(data_vault_data2)

# COMMAND ----------

display(data_vault_data2.select("RETAILER_ITEM_ID").distinct())

# COMMAND ----------

data_vault_data3 = data_vault_data.where((pyf.col("SALES_DT") <= pyf.lit("2022-09-30")) & (pyf.col("SALES_DT") >= pyf.lit("2022-09-01")))

print(data_vault_data3.cache().count())

# COMMAND ----------

data_vault_data4 = data_vault_data3.where((pyf.col("RETAILER_ITEM_ID").isin(
5510017,
5826568, 
9220190, 
6019058, 
5733204)))

print(data_vault_data4.cache().count())

# COMMAND ----------

display(data_vault_data4)

# COMMAND ----------


