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

RETAILER = 'walmart'
CLIENT = 'nestlewaters'
COUNTRY_CODE = 'us'

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

data_vault_data_walmart_clorox_us = read_pos_data(RETAILER, CLIENT, COUNTRY_CODE, sqlContext).filter('POS_ITEM_QTY > 0').repartition('SALES_DT')
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
SOURCE_SYSTEM = 'retaillink'

# COMMAND ----------

data_vault_data_walmart_nestlewaters_us = read_pos_data(SOURCE_SYSTEM, RETAILER, CLIENT, COUNTRY_CODE, sqlContext).filter('POS_ITEM_QTY > 0').repartition('SALES_DT')

# COMMAND ----------

data_vault_data_walmart_nestlewaters_us.printSchema()

# COMMAND ----------

display(data_vault_data_walmart_nestlewaters_us)

# COMMAND ----------

CLIENT = 'bluetritonbrands'
COUNTRY_CODE = 'us'
SOURCE_SYSTEM = 'retaillink'

# COMMAND ----------

data_vault_data_walmart_bluetritonbrands_us = read_pos_data(SOURCE_SYSTEM, RETAILER, CLIENT, COUNTRY_CODE, sqlContext).filter('POS_ITEM_QTY > 0').repartition('SALES_DT')

# COMMAND ----------

data_vault_data_walmart_bluetritonbrands_us.printSchema()

# COMMAND ----------

display(data_vault_data_walmart_bluetritonbrands_us)

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


