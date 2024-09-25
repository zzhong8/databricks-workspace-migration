# Databricks notebook source
import acosta
from acosta.alerting.preprocessing import pos_to_training_data, read_pos_data

from pyspark.sql import functions as pyf
from pyspark.sql.types import *

# COMMAND ----------

RETAILER = 'walmart'

# COMMAND ----------

# CLIENT = 'dole'
# data_vault_data_dole = read_pos_data(RETAILER, CLIENT, sqlContext).repartition('SALES_DT')

# COMMAND ----------

CLIENT = 'clorox'
data_vault_data = read_pos_data(RETAILER, CLIENT, sqlContext).repartition('SALES_DT')

# COMMAND ----------

display(data_vault_data.describe())

# COMMAND ----------

print(data_vault_data.columns)

# COMMAND ----------

df_agg = data_vault_data.groupBy('SALES_DT', 'ORGANIZATION_UNIT_NUM').agg(pyf.count("*").alias('TOTAL_NUM_ITEMS'), pyf.sum("POS_ITEM_QTY").alias('TOTAL_POS_ITEM_QTY'), pyf.avg("POS_ITEM_QTY").alias('AVG_POS_ITEM_QTY')).orderBy(["ORGANIZATION_UNIT_NUM", "SALES_DT"], ascending=True)

# COMMAND ----------

# df_agg_sum = display(data_vault_data.groupBy("DISCOUNT_PCT_PERCENTAGE").count().orderBy("DISCOUNT_PCT_PERCENTAGE", ascending=True))

# COMMAND ----------

df_agg.show(100)

# COMMAND ----------

# df_agg.repartition(1).write.format('csv')\
#     .mode("overwrite")\
#     .options(header='true', inferSchema='true')\
#     .save('/mnt/artifacts/walmart-dole-agg-POS_ITEM_QTY', sep=',')

# COMMAND ----------

df_agg.repartition(1).write.format('csv')\
    .mode("overwrite")\
    .options(header='true', inferSchema='true')\
    .save('/mnt/artifacts/walmart-clorox-agg-POS_ITEM_QTY', sep=',')
