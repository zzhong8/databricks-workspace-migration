# Databricks notebook source
import numpy as np
import pandas as pd

import matplotlib.pyplot as plt
import seaborn as sns

import math
import re
from scipy import stats
import datetime

from pyspark.sql import functions as pyf
from pyspark.sql import types as pyt
from pyspark.sql import Window

import pyspark
import datetime

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
# from pyspark.sql.window import Window
# from pyspark.sql.functions import count, ltrim, rtrim, col, rank, regexp_replace, desc, when

# COMMAND ----------

interventions_path = f"/mnt/processed/measurement/cache/16320-30-3257-7743-processed"
df = spark.read.format('delta').load(interventions_path)
print(f'N: {df.cache().count():,}')

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run new Query

# COMMAND ----------

uk_dla_epos_dvs = [
  "asda_nestlecore_uk_dv",
  "morrisons_nestlecore_uk_dv",
  "sainsburys_nestlecore_uk_dv",
  "tesco_nestlecore_uk_dv"
]

# COMMAND ----------

sales_attributes_by_product_schema = StructType([
  StructField('platform', StringType(), True),
  
  StructField('parent_chain_id', IntegerType(), True),
  StructField('parent_chain_name', StringType(), True),
  
  StructField('company_id', IntegerType(), True),
  StructField('company_name', StringType(), True),
  
  StructField('manufacturer_id', IntegerType(), True),
  StructField('manufacturer_name', StringType(), True),
  
  StructField('product_id', IntegerType(), True),
  StructField('product_name', StringType(), True),
  StructField('universal_product_code', StringType(), True),
  
  StructField('retailer_item_id', StringType(), True),
  ])

# COMMAND ----------

uk_dla_sales_attributes_by_product = spark.createDataFrame([], sales_attributes_by_product_schema)

for uk_dla_epos_dv in uk_dla_epos_dvs:
  uk_dla_sql_sales_attributes_by_product = """
  select
    'uk' as platform,
    pc.ChainId as parent_chain_id,
    pc.FullName as parent_chain_name,    
    
    c.CompanyId as company_id,
    c.FullName as company_name,
    
    bp.ManufacturerId as manufacturer_id,
    '' as manufacturer_name,
    
    p.ProductId as product_id,
    p.FullName as product_name,
    p.UniversalProductCode as universal_product_code,

    hri.retailer_item_id as retailer_item_id
    
  from
    """ + uk_dla_epos_dv + """.vw_sat_link_epos_summary vsles
    
    join
    """ + uk_dla_epos_dv + """.hub_retailer_item hri
    ON
    hri.hub_retailer_item_hk = vsles.hub_retailer_item_hk
    
    join
    """ + uk_dla_epos_dv + """.hub_organization_unit hou
    ON 
    hou.hub_organization_unit_hk = vsles.hub_organization_unit_hk
    
    join 
    bobv2.vw_bobv2_product bp
    on
    ltrim('0', ltrim(rtrim(hri.retailer_item_id))) = ltrim('0', ltrim(rtrim(bp.RefExternal)))

    join
    bobv2.chain pc
    on
    bp.ParentChainId = pc.ChainId  
    
    join
    bobv2.company c
    on
    bp.CompanyId = c.CompanyId
    
    join    
    bobv2.product p
    on
    bp.ProductId = p.ProductId  

  where
    c.CompanyId = 609 -- Nestle Grocery
    and
    vsles.sales_dt >= '2022-01-01'
    
  group by
    parent_chain_id,
    parent_chain_name,    
    company_id,
    company_name,
    manufacturer_id,
    manufacturer_name,
    product_id,
    product_name,
    universal_product_code,
    retailer_item_id

  order by
    company_id,
    manufacturer_id
  
  """

  uk_dla_df_sales_attributes_by_product = spark.sql(uk_dla_sql_sales_attributes_by_product)
  
  uk_dla_sales_attributes_by_product = uk_dla_sales_attributes_by_product.union(uk_dla_df_sales_attributes_by_product)
print(f'N: {uk_dla_sales_attributes_by_product.cache().count():,}')

# COMMAND ----------

display(uk_dla_sales_attributes_by_product)

# COMMAND ----------

df = df.join(uk_dla_sales_attributes_by_product, df['RETAILER_ITEM_ID'] == uk_dla_sales_attributes_by_product['retailer_item_id'], how='inner')
print(f'N: {df.cache().count():,}')

# COMMAND ----------

df_itemlevel = spark.sql("""select * from acosta_retail_analytics_im.vw_dimension_itemlevel where client_id = 16320 and category_description = 'Cereal'""")
print(f'N: {df_itemlevel.cache().count():,}')

# COMMAND ----------

df = df.join(df_itemlevel, df['universal_product_code'] == df_itemlevel['upc'], how='inner')
print(f'N: {df.cache().count():,}')

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Get the average values for
# MAGIC 1) BASELINE
# MAGIC 2) EXPECTED
# MAGIC 3) INTERVENTION_EFFECT by the year-month of the SALES_DT (i.e. the 2022-10)

# COMMAND ----------

# create year_month column
df = df.withColumn('year_month', pyf.date_format('SALES_DT', 'yyyy-MM'))

# COMMAND ----------

df_2002_03_onwards = df.filter(pyf.col('SALES_DT') >= '2022-03-01')

# COMMAND ----------

# group by year_month and calculate average sales
df_agg_sales = df_2002_03_onwards.groupBy('year_month').agg({'POS_ITEM_QTY': 'avg', 'PRICE': 'avg', 'BASELINE': 'avg', 'EXPECTED': 'avg', 'INTERVENTION_EFFECT': 'avg'}).orderBy('year_month')

# COMMAND ----------

display(df_agg_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tesco Cereals

# COMMAND ----------

interventions_path = f"/mnt/processed/measurement/cache/16320-30-3257-7746-processed"
df2 = spark.read.format('delta').load(interventions_path)
print(f'N: {df2.cache().count():,}')

# COMMAND ----------

df2 = df2.join(uk_dla_sales_attributes_by_product, df2['RETAILER_ITEM_ID'] == uk_dla_sales_attributes_by_product['retailer_item_id'], how='inner')
print(f'N: {df2.cache().count():,}')
df2 = df2.join(df_itemlevel, df2['universal_product_code'] == df_itemlevel['upc'], how='inner')
print(f'N: {df2.cache().count():,}')

# COMMAND ----------

# create year_month column
df2 = df2.withColumn('year_month', pyf.date_format('SALES_DT', 'yyyy-MM'))
df2_2002_03_onwards = df2.filter(pyf.col('SALES_DT') >= '2022-03-01')
# group by year_month and calculate average sales
df2_agg_sales = df2_2002_03_onwards.groupBy('year_month').agg({'POS_ITEM_QTY': 'avg', 'PRICE': 'avg', 'BASELINE': 'avg', 'EXPECTED': 'avg', 'INTERVENTION_EFFECT': 'avg'}).orderBy('year_month')

# COMMAND ----------

display(df2_agg_sales)
