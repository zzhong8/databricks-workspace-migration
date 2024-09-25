# Databricks notebook source
import pyspark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql.functions import ltrim, rtrim, col, regexp_replace

# COMMAND ----------

sql_walmart_harrys_bobv2_UPCs = """
select distinct
  p.UniversalProductCode as bobv2_universal_product_code
  from
  bobv2.product p
  where
  CompanyId = 567
  and
  ManufacturerId = 247
  order by bobv2_universal_product_code
"""

# COMMAND ----------

df_walmart_harrys_bobv2_UPCs = spark.sql(sql_walmart_harrys_bobv2_UPCs);

# COMMAND ----------

display(df_walmart_harrys_bobv2_UPCs)

# COMMAND ----------

sql_walmart_harrys_analytics_im_UPCs = """
select distinct
  il.upc as analytics_im_universal_product_code,
  il.item_dimension_description
  from acosta_retail_analytics_im.vw_dimension_itemlevel il
  where
  il.client_id = 16540 -- Harrys Inc
order by analytics_im_universal_product_code
"""

# COMMAND ----------

df_walmart_harrys_analytics_im_UPCs = spark.sql(sql_walmart_harrys_analytics_im_UPCs);

# COMMAND ----------

display(df_walmart_harrys_analytics_im_UPCs)

# COMMAND ----------

df_walmart_harrys_bobv2_UPCs = df_walmart_harrys_bobv2_UPCs.withColumn('upc', regexp_replace(ltrim(rtrim(col("bobv2_universal_product_code"))), r'^[0]*', ''))

df_walmart_harrys_analytics_im_UPCs = df_walmart_harrys_analytics_im_UPCs.withColumn('upc', regexp_replace(ltrim(rtrim(col("analytics_im_universal_product_code"))), r'^[0]*', ''))

# COMMAND ----------

display(df_walmart_harrys_bobv2_UPCs)

# COMMAND ----------

df_union = df_walmart_harrys_bobv2_UPCs.join(df_walmart_harrys_analytics_im_UPCs, "upc", "inner")

# COMMAND ----------

display(df_union)

# COMMAND ----------


