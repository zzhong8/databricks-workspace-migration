# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

# schema = StructType([
#     StructField('CLIENT', StringType(), False),\
#     StructField('RETAILER', StringType(), False),\
#     StructField('ORGANIZATION_UNIT_NUM', IntegerType(), False),\
#     StructField('RETAILER_ITEM_ID', StringType(), False),\
#     StructField('BASELINE_POS_ITEM_QTY', DoubleType(), False),\
#     StructField('SALES_DT', DateType(), False)])

# COMMAND ----------

df = spark.read.format('csv').option('header', 'true').load('/mnt/artifacts/hugh/kraftheinz-all-predictions-190101-to-190514/*.csv')

# COMMAND ----------

df1 = df.selectExpr('CLIENT',
                    'RETAILER',
                    'ORGANIZATION_UNIT_NUM',
                    'RETAILER_ITEM_ID',
                    'BASELINE_POS_ITEM_QTY',
                    'SALES_DT')

# COMMAND ----------

df1.dtypes

# COMMAND ----------

df1.count()

# COMMAND ----------

display(df1)

# COMMAND ----------

# Save the data set
df1.coalesce(1) \
    .write.mode('overwrite').format('csv').option('header', 'true') \
    .save('/mnt/artifacts/hugh/kraftheinz-all-predictions-190101-to-190514')

# COMMAND ----------


