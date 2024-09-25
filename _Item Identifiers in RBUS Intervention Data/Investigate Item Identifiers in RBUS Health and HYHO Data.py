# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

# RUN ME
import datetime
import pyspark
from pyspark.sql.types import ArrayType
from pyspark.sql.types import DateType
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.functions import min

# COMMAND ----------

df_rbusahygiene = spark.sql('select distinct RefExternal from walmart_rbusahygiene_us_retail_alert_im.vw_gc_ds_input_rsv_measurement')

display(df_rbusahygiene)

# COMMAND ----------

df_rbusahealth = spark.sql('select distinct RefExternal from walmart_rbusahealth_us_retail_alert_im.vw_gc_ds_input_rsv_measurement')

display(df_rbusahealth)

# COMMAND ----------

# #temporary

# df_asda_nestlecore = spark.sql('select distinct RefExternal from asda_nestlecore_uk_retail_alert_im.vw_gc_ds_input_rsv_measurement')

# display(df_asda_nestlecore)

# COMMAND ----------

retailer_item_ids_df_iv_rbusahealth = spark.read.format('delta').load('/mnt/processed/temp/intervention_data-rbusahealth-walmart-retailer_item_ids')

retailer_item_ids_df_joined_rbusahealth = spark.read.format('delta').load('/mnt/processed/temp/rbusahealth_walmart_inv_epos-retailer_item_ids')

# COMMAND ----------

display(retailer_item_ids_df_iv_rbusahealth)

# COMMAND ----------

retailer_item_ids_df_iv_rbusahealth.count()

# COMMAND ----------

display(retailer_item_ids_df_joined_rbusahealth)

# COMMAND ----------

retailer_item_ids_df_joined_rbusahealth.count()

# COMMAND ----------

non_joins_rbusahealth = retailer_item_ids_df_iv_rbusahealth\
    .join(retailer_item_ids_df_joined_rbusahealth, retailer_item_ids_df_iv_rbusahealth.RefExternal == retailer_item_ids_df_joined_rbusahealth.RefExternal, 'left_anti')

# COMMAND ----------

display(non_joins_rbusahealth)

# COMMAND ----------

non_joins_rbusahealth.count()

# COMMAND ----------

retailer_item_ids_df_iv_rbusahygiene = spark.read.format('delta').load('/mnt/processed/temp/intervention_data-rbusahygiene-walmart-retailer_item_ids')

retailer_item_ids_df_joined_rbusahygiene = spark.read.format('delta').load('/mnt/processed/temp/rbusahygiene_walmart_inv_epos-retailer_item_ids')

# COMMAND ----------

display(retailer_item_ids_df_iv_rbusahygiene)

# COMMAND ----------

retailer_item_ids_df_iv_rbusahygiene.count()

# COMMAND ----------

display(retailer_item_ids_df_joined_rbusahygiene)

# COMMAND ----------

retailer_item_ids_df_joined_rbusahygiene.count()

# COMMAND ----------

non_joins_rbusahygiene = retailer_item_ids_df_iv_rbusahygiene\
    .join(retailer_item_ids_df_joined_rbusahygiene, retailer_item_ids_df_iv_rbusahygiene.RefExternal == retailer_item_ids_df_joined_rbusahygiene.RefExternal, 'left_anti')

# COMMAND ----------

display(non_joins_rbusahygiene)

# COMMAND ----------

non_joins_rbusahygiene.count()

# COMMAND ----------

joins_rbusahygiene = retailer_item_ids_df_iv_rbusahygiene\
    .join(retailer_item_ids_df_joined_rbusahygiene, retailer_item_ids_df_iv_rbusahygiene.RefExternal == retailer_item_ids_df_joined_rbusahygiene.RefExternal, 'inner')

# COMMAND ----------

display(joins_rbusahygiene)

# COMMAND ----------

joins_rbusahygiene.count()

# COMMAND ----------

sql = "SELECT \
          * \
        FROM BOBv2.ProductMapChain";

df_ProductMapChain = spark.sql(sql);

# COMMAND ----------

df_ProductMapChain.count()

# COMMAND ----------

# from pyspark.sql.functions import *
from pyspark.sql import types as pyt
from pyspark.sql import functions as pyf

# COMMAND ----------

non_joins_rbusahygiene_df_ProductMapChain_joined = non_joins_rbusahygiene\
    .join(df_ProductMapChain, 'RefExternal', 'inner')\
    .where((pyf.col("ChainId") == 950))

# & (pyf.col("Active") == pyf.lit("true"))

# COMMAND ----------

non_joins_rbusahygiene_df_ProductMapChain_joined.dtypes

# COMMAND ----------

display(non_joins_rbusahygiene_df_ProductMapChain_joined)

# COMMAND ----------

non_joins_rbusahygiene_df_ProductMapChain_joined.count()

# COMMAND ----------

df1 = non_joins_rbusahygiene_df_ProductMapChain_joined.select('RefExternal', 'ProductId', 'ChainId', 'IsPrimary', 'LastChanged', 'Active')

# COMMAND ----------

display(df1)

# COMMAND ----------

non_joins_rbusahealth_df_ProductMapChain_joined = non_joins_rbusahealth\
    .join(df_ProductMapChain, 'RefExternal', 'inner')\
    .where((pyf.col("ChainId") == 950))

# COMMAND ----------

df2 = non_joins_rbusahealth_df_ProductMapChain_joined.select('RefExternal', 'ProductId', 'ChainId', 'IsPrimary', 'LastChanged', 'Active')

# COMMAND ----------

df2.count()

# COMMAND ----------

display(df2)

# COMMAND ----------


