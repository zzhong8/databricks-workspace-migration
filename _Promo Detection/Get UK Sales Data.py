# Databricks notebook source
import uuid
import warnings
import numpy as np
import pandas as pd
from datetime import datetime
from pyspark.sql import Window
# from pyspark.sql.functions import *
from pyspark.sql import types as pyt
from pyspark.sql import functions as pyf
import acosta
from acosta.alerting.preprocessing import read_pos_data

print(acosta.__version__)

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

    database = '{retailer}_{client}_{country_code}_dv'.format(retailer=retailer.strip().lower(),
                                                              client=client.strip().lower(),
                                                              country_code=country_code.strip().lower())

    sql_statement = """
        SELECT
              \'{retailer}\' AS RETAILER,
              \'{client}\' AS CLIENT,
              \'{country_code}\' AS COUNTRY_CODE,
              HRI.RETAILER_ITEM_ID,
              SRI.RETAILER_ITEM_DESC, 
              HOU.ORGANIZATION_UNIT_NUM,
              SOU.ORGANIZATION_UNIT_NM,
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
           LEFT OUTER JOIN {database}.sat_retailer_item SRI   
                ON SRI.HUB_RETAILER_ITEM_HK = VSLES.HUB_RETAILER_ITEM_HK
           LEFT OUTER JOIN {database}.hub_organization_unit HOU 
                ON HOU.HUB_ORGANIZATION_UNIT_HK = VSLES.HUB_ORGANIZATION_UNIT_HK
           LEFT OUTER JOIN {database}.sat_organization_unit SOU 
                ON SOU.HUB_ORGANIZATION_UNIT_HK = VSLES.HUB_ORGANIZATION_UNIT_HK
        """

    sql_statement = sql_statement.format(database=database, retailer=retailer, client=client, country_code=country_code)

    return sql_context.sql(sql_statement)


# COMMAND ----------

RETAILER = 'asda'
CLIENT = 'nestlecore'
COUNTRY_CODE = 'uk'

# COMMAND ----------

# Read POS data
data_vault_data_asda_nestlecore = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')

# COMMAND ----------

display(data_vault_data_asda_nestlecore)

# COMMAND ----------

data_vault_data_asda_nestlecore_2020 = data_vault_data_asda_nestlecore.where((pyf.col("SALES_DT") < pyf.lit("2020-08-11")) & (pyf.col("SALES_DT") >= pyf.lit("2020-01-01")))

# Save the data set
data_vault_data_asda_nestlecore_2020.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/asda-nestlecore-20200101-to-20200810')

# COMMAND ----------

data_vault_data_asda_nestlecore_2019 = data_vault_data_asda_nestlecore.where((pyf.col("SALES_DT") < pyf.lit("2020-01-01")) & (pyf.col("SALES_DT") >= pyf.lit("2019-01-01")))

# Save the data set
data_vault_data_asda_nestlecore_2019.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/asda-nestlecore-20190101-to-20191231')

# COMMAND ----------

data_vault_data_asda_nestlecore_2018 = data_vault_data_asda_nestlecore.where((pyf.col("SALES_DT") < pyf.lit("2019-01-01")) & (pyf.col("SALES_DT") >= pyf.lit("2018-01-01")))

# Save the data set
data_vault_data_asda_nestlecore_2018.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/asda-nestlecore-20180101-to-20181231')

# COMMAND ----------

data_vault_data_asda_nestlecore_2017 = data_vault_data_asda_nestlecore.where((pyf.col("SALES_DT") < pyf.lit("2018-01-01")) & (pyf.col("SALES_DT") >= pyf.lit("2017-01-01")))

# Save the data set
data_vault_data_asda_nestlecore_2017.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/asda-nestlecore-20170101-to-20171231')

# COMMAND ----------

data_vault_data_asda_nestlecore_2016 = data_vault_data_asda_nestlecore.where((pyf.col("SALES_DT") < pyf.lit("2017-01-01")) & (pyf.col("SALES_DT") >= pyf.lit("2016-01-01")))

# Save the data set
data_vault_data_asda_nestlecore_2016.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/asda-nestlecore-20160101-to-20161231')

# COMMAND ----------

RETAILER = 'tesco'
CLIENT = 'nestlecore'
COUNTRY_CODE = 'uk'

# COMMAND ----------

# Read POS data
data_vault_data_tesco_nestlecore = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')

# COMMAND ----------

display(data_vault_data_tesco_nestlecore)

# COMMAND ----------

data_vault_data_tesco_nestlecore_2020 = data_vault_data_tesco_nestlecore.where((pyf.col("SALES_DT") < pyf.lit("2020-08-11")) & (pyf.col("SALES_DT") >= pyf.lit("2020-01-01")))

# Save the data set
data_vault_data_tesco_nestlecore_2020.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/tesco-nestlecore-20200101-to-20200810')

# COMMAND ----------

data_vault_data_tesco_nestlecore_2019 = data_vault_data_tesco_nestlecore.where((pyf.col("SALES_DT") < pyf.lit("2020-01-01")) & (pyf.col("SALES_DT") >= pyf.lit("2019-01-01")))

# Save the data set
data_vault_data_tesco_nestlecore_2019.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/tesco-nestlecore-20190101-to-20191231')

# COMMAND ----------

data_vault_data_tesco_nestlecore_2018 = data_vault_data_tesco_nestlecore.where((pyf.col("SALES_DT") < pyf.lit("2019-01-01")) & (pyf.col("SALES_DT") >= pyf.lit("2018-01-01")))

# Save the data set
data_vault_data_tesco_nestlecore_2018.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/tesco-nestlecore-20180101-to-20181231')

# COMMAND ----------

data_vault_data_tesco_nestlecore_2017 = data_vault_data_tesco_nestlecore.where((pyf.col("SALES_DT") < pyf.lit("2018-01-01")) & (pyf.col("SALES_DT") >= pyf.lit("2017-01-01")))

# Save the data set
data_vault_data_tesco_nestlecore_2017.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/tesco-nestlecore-20170101-to-20171231')

# COMMAND ----------

data_vault_data_tesco_nestlecore_2016 = data_vault_data_tesco_nestlecore.where((pyf.col("SALES_DT") < pyf.lit("2017-01-01")) & (pyf.col("SALES_DT") >= pyf.lit("2016-01-01")))

# Save the data set
data_vault_data_tesco_nestlecore_2016.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/tesco-nestlecore-20160101-to-20161231')

# COMMAND ----------

RETAILER = 'morrisons'
CLIENT = 'nestlecore'
COUNTRY_CODE = 'uk'

# COMMAND ----------

# Read POS data
data_vault_data_morrisons_nestlecore = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')

# COMMAND ----------

display(data_vault_data_morrisons_nestlecore)

# COMMAND ----------

data_vault_data_morrisons_nestlecore_2020 = data_vault_data_morrisons_nestlecore.where((pyf.col("SALES_DT") < pyf.lit("2020-08-13")) & (pyf.col("SALES_DT") >= pyf.lit("2020-01-01")))

# Save the data set
data_vault_data_morrisons_nestlecore_2020.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/morrisons-nestlecore-20200101-to-20200812')

# COMMAND ----------

data_vault_data_morrisons_nestlecore_2019 = data_vault_data_morrisons_nestlecore.where((pyf.col("SALES_DT") < pyf.lit("2020-01-01")) & (pyf.col("SALES_DT") >= pyf.lit("2019-01-01")))

# Save the data set
data_vault_data_morrisons_nestlecore_2019.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/morrisons-nestlecore-20190101-to-20191231')

# COMMAND ----------

data_vault_data_morrisons_nestlecore_2018 = data_vault_data_morrisons_nestlecore.where((pyf.col("SALES_DT") < pyf.lit("2019-01-01")) & (pyf.col("SALES_DT") >= pyf.lit("2018-01-01")))

# Save the data set
data_vault_data_morrisons_nestlecore_2018.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/morrisons-nestlecore-20180101-to-20181231')

# COMMAND ----------

data_vault_data_morrisons_nestlecore_2017 = data_vault_data_morrisons_nestlecore.where((pyf.col("SALES_DT") < pyf.lit("2018-01-01")) & (pyf.col("SALES_DT") >= pyf.lit("2017-01-01")))

# Save the data set
data_vault_data_morrisons_nestlecore_2017.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/morrisons-nestlecore-20170101-to-20171231')

# COMMAND ----------

RETAILER = 'morrisons'
CLIENT = 'generalmills'
COUNTRY_CODE = 'uk'

# COMMAND ----------

# Read POS data
data_vault_data_morrisons_generalmills = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')

# COMMAND ----------

display(data_vault_data_morrisons_generalmills)

# COMMAND ----------

data_vault_data_morrisons_generalmills_2020 = data_vault_data_morrisons_generalmills.where((pyf.col("SALES_DT") < pyf.lit("2020-08-14")) & (pyf.col("SALES_DT") >= pyf.lit("2020-01-01")))

# Save the data set
data_vault_data_morrisons_generalmills_2020.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/morrisons-generalmills-20200101-to-20200813')

# COMMAND ----------

data_vault_data_morrisons_generalmills_2019 = data_vault_data_morrisons_generalmills.where((pyf.col("SALES_DT") < pyf.lit("2020-01-01")) & (pyf.col("SALES_DT") >= pyf.lit("2019-01-01")))

# Save the data set
data_vault_data_morrisons_generalmills_2019.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/morrisons-generalmills-20190101-to-20191231')

# COMMAND ----------

data_vault_data_morrisons_generalmills_2018 = data_vault_data_morrisons_generalmills.where((pyf.col("SALES_DT") < pyf.lit("2019-01-01")) & (pyf.col("SALES_DT") >= pyf.lit("2018-01-01")))

# Save the data set
data_vault_data_morrisons_generalmills_2018.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/morrisons-generalmills-20180101-to-20181231')

# COMMAND ----------

data_vault_data_morrisons_generalmills_2017 = data_vault_data_morrisons_generalmills.where((pyf.col("SALES_DT") < pyf.lit("2018-01-01")) & (pyf.col("SALES_DT") >= pyf.lit("2017-01-01")))

# Save the data set
data_vault_data_morrisons_generalmills_2017.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/morrisons-generalmills-20170101-to-20171231')

# COMMAND ----------

data_vault_data_morrisons_generalmills_2016 = data_vault_data_morrisons_generalmills.where((pyf.col("SALES_DT") < pyf.lit("2017-01-01")) & (pyf.col("SALES_DT") >= pyf.lit("2016-01-01")))

# Save the data set
data_vault_data_morrisons_generalmills_2016.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/morrisons-generalmills-20160101-to-20161231')

# COMMAND ----------


