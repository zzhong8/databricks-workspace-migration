# Databricks notebook source
from pprint import pprint
import warnings
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pyspark.sql import Window
import pyspark.sql.functions as pyf
import acosta
import pyarrow
from acosta.alerting.preprocessing import read_pos_data, pos_to_training_data
from acosta.alerting.helpers import check_path_exists

print(acosta.__version__)
print(pyarrow.__version__)

current_timestamp = datetime.now().strftime('%Y-%m-%d')

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
              SRI.RETAILER_ITEM_DESC
           FROM {database}.vw_sat_link_epos_summary VSLES
           LEFT OUTER JOIN {database}.hub_retailer_item HRI
                ON HRI.HUB_RETAILER_ITEM_HK = VSLES.HUB_RETAILER_ITEM_HK
           LEFT OUTER JOIN {database}.sat_retailer_item SRI
                ON SRI.HUB_RETAILER_ITEM_HK = VSLES.HUB_RETAILER_ITEM_HK
           LEFT OUTER JOIN {database}.hub_organization_unit HOU
                ON HOU.HUB_ORGANIZATION_UNIT_HK = VSLES.HUB_ORGANIZATION_UNIT_HK
    """

    sql_statement = sql_statement.format(database=database, retailer=retailer, client=client, country_code=country_code)

    return sql_context.sql(sql_statement)

# COMMAND ----------

RETAILER = 'walmart'
CLIENT = 'milos'
COUNTRY_CODE = 'us'
SOURCE_SYSTEM = 'retaillink'

# COMMAND ----------

data_vault_data_walmart_milos_us = read_pos_data(SOURCE_SYSTEM, RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
df_all_sales_dates = data_vault_data_walmart_milos_us.select("SALES_DT").distinct()

display(df_all_sales_dates)

# COMMAND ----------

RETAILER = 'target'
CLIENT = 'campbells'
COUNTRY_CODE = 'us'
SOURCE_SYSTEM = 'bigred'

# COMMAND ----------

data_vault_data_target_campbells_us = read_pos_data(SOURCE_SYSTEM, RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
df = data_vault_data_target_campbells_us.select("ORGANIZATION_UNIT_NUM").distinct()

df.count()

# COMMAND ----------

RETAILER = 'tesco'
CLIENT = 'nestlecore'
COUNTRY_CODE = 'uk'

# COMMAND ----------

data_vault_data_tesco_nestlecore_uk = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
df = data_vault_data_tesco_nestlecore_uk.select("RETAILER_ITEM_ID", "RETAILER_ITEM_DESC", "SALES_DT").distinct()

# COMMAND ----------

df_pivot_table = df.groupBy("RETAILER_ITEM_ID").agg(pyf.max("SALES_DT").alias("MAX_SALES_DT"), pyf.max("RETAILER_ITEM_DESC").alias("RETAILER_ITEM_DESC"))

# COMMAND ----------

display(df_pivot_table)

# COMMAND ----------

df_pivot_table2 = df.groupBy("RETAILER_ITEM_ID").agg(pyf.max("SALES_DT").alias("MAX_SALES_DT"), pyf.min("RETAILER_ITEM_DESC").alias("RETAILER_ITEM_DESC"))

# COMMAND ----------

display(df_pivot_table2)

# COMMAND ----------

RETAILER = 'morrisons'
CLIENT = 'nestlecore'
COUNTRY_CODE = 'uk'

# COMMAND ----------

data_vault_data_morrisons_nestlecore_uk = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).filter('POS_ITEM_QTY > 0').repartition('SALES_DT')
df = data_vault_data_morrisons_nestlecore_uk.select("SALES_DT").distinct()

# COMMAND ----------

df.count()

# COMMAND ----------

display(df)

# COMMAND ----------

RETAILER = 'walmart'
CLIENT = 'nestlewaters'
COUNTRY_CODE = 'us'
SOURCE_SYSTEM = 'retaillink'

# COMMAND ----------

data_vault_data_walmart_milos_us = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).filter('POS_ITEM_QTY > 0').repartition('SALES_DT')
df = data_vault_data_walmart_milos_us.select("ORGANIZATION_UNIT_NUM").distinct()

# COMMAND ----------

display(df)

# COMMAND ----------

CLIENT = 'atkinsnutritional'
COUNTRY_CODE = 'us'

# COMMAND ----------

data_vault_data_walmart_atkinsnutritional_us = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).filter('POS_ITEM_QTY > 0').repartition('SALES_DT')
data_vault_data_walmart_atkinsnutritional_us.select("ORGANIZATION_UNIT_NUM","RETAILER_ITEM_ID").distinct().count()

# COMMAND ----------

CLIENT = 'tysonhillshire'
COUNTRY_CODE = 'us'

# COMMAND ----------


data_vault_data_walmart_tysonhillshire_us = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).filter('POS_ITEM_QTY > 0').repartition('SALES_DT')
data_vault_data_walmart_tysonhillshire_us.select("ORGANIZATION_UNIT_NUM","RETAILER_ITEM_ID").distinct().count()

# COMMAND ----------

CLIENT = 'clorox'
COUNTRY_CODE = 'us'

# COMMAND ----------

data_vault_data_walmart_clorox_us = read_pos_data(SOURCE_SYSTEM, RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data_walmart_clorox_us.select("ORGANIZATION_UNIT_NUM","RETAILER_ITEM_ID").distinct().count()

# COMMAND ----------

data_vault_data_walmart_clorox_us = read_pos_data(SOURCE_SYSTEM, RETAILER, CLIENT, COUNTRY_CODE, sqlContext).filter('POS_ITEM_QTY > 0').repartition('SALES_DT')
data_vault_data_walmart_clorox_us.select("ORGANIZATION_UNIT_NUM","RETAILER_ITEM_ID").distinct().count()

# COMMAND ----------

CLIENT = 'minutemaidcompany'
COUNTRY_CODE = 'us'

# COMMAND ----------

data_vault_data_walmart_minutemaidcompany_us = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).filter('POS_ITEM_QTY > 0').repartition('SALES_DT')
data_vault_data_walmart_minutemaidcompany_us.select("ORGANIZATION_UNIT_NUM","RETAILER_ITEM_ID").distinct().count()

# COMMAND ----------

CLIENT = 'nestlewaters'
COUNTRY_CODE = 'us'

# COMMAND ----------

data_vault_data_walmart_nestlewaters_us = read_pos_data(SOURCE_SYSTEM, RETAILER, CLIENT, COUNTRY_CODE, sqlContext).filter('POS_ITEM_QTY > 0').repartition('SALES_DT')
data_vault_data_walmart_nestlewaters_us.select("ORGANIZATION_UNIT_NUM","RETAILER_ITEM_ID").distinct().count()

# COMMAND ----------

CLIENT = 'bluetritonbrands'
COUNTRY_CODE = 'us'

# COMMAND ----------

data_vault_data_walmart_bluetritonbrands_us = read_pos_data(SOURCE_SYSTEM, RETAILER, CLIENT, COUNTRY_CODE, sqlContext).filter('POS_ITEM_QTY > 0').repartition('SALES_DT')
data_vault_data_walmart_bluetritonbrands_us.select("ORGANIZATION_UNIT_NUM","RETAILER_ITEM_ID").distinct().count()

# COMMAND ----------

data_vault_data_walmart_bluetritonbrands_us = read_pos_data(SOURCE_SYSTEM, RETAILER, CLIENT, COUNTRY_CODE, sqlContext).repartition('SALES_DT')
data_vault_data_walmart_bluetritonbrands_us.select("ORGANIZATION_UNIT_NUM","RETAILER_ITEM_ID").distinct().count()

# COMMAND ----------

data_vault_data_walmart_bluetritonbrands_us.select("ORGANIZATION_UNIT_NUM").distinct().count()

# COMMAND ----------

data_vault_data_walmart_bluetritonbrands_us.select("RETAILER_ITEM_ID").distinct().count()

# COMMAND ----------

data_vault_data_walmart_minutemaidcompany_us.select("ORGANIZATION_UNIT_NUM","RETAILER_ITEM_ID").distinct().count()

# COMMAND ----------

data_vault_data_walmart_nestlewaters_us.printSchema()

# COMMAND ----------

data_vault_data_walmart_nestlewaters_us_store_item_combinations = data_vault_data_walmart_nestlewaters_us.select("ORGANIZATION_UNIT_NUM","RETAILER_ITEM_ID").distinct()

# COMMAND ----------

df_agg = data_vault_data_walmart_nestlewaters_us_store_item_combinations.groupBy('ORGANIZATION_UNIT_NUM').agg(pyf.count("*").alias('TOTAL_NUM_ITEMS'))

# COMMAND ----------

display(df_agg)

# COMMAND ----------

df_agg.describe('TOTAL_NUM_ITEMS').show()

# COMMAND ----------

df_agg.approxQuantile('TOTAL_NUM_ITEMS', [0.0, 0.25, 0.5, 0.75, 1.0], 0.0)

# COMMAND ----------

df_agg.approxQuantile('TOTAL_NUM_ITEMS', [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0], 0.0)

# COMMAND ----------

# # import pyspark_dist_explore
# from pyspark_dist_explore import hist
# import matplotlib.pyplot as plt

# fig, ax = plt.subplots()
# hist(ax, df_agg, bins = 10, color=['red'])

# COMMAND ----------


