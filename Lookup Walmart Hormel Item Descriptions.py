# Databricks notebook source
from pyspark.sql import functions as pyf
from pyspark.sql.types import *
import pandas as pd
import datetime

# COMMAND ----------

RETAILER = 'WALMART'
CLIENT = 'HORMEL'

# COMMAND ----------

walmart_hormel_top_5_selling_item_list = [551801636, 550387850, 9271128, 553484334, 9232464]

# COMMAND ----------

from pyspark.sql import Row
R = Row('ID', 'RETAILER_ITEM_ID')

# use enumerate to add the ID column
spark.createDataFrame([R(i, x) for i, x in enumerate(walmart_hormel_top_5_selling_item_list)]).show() 

# COMMAND ----------

#LOOK UP HASH KEYS
databaseName = "{}_{}_dv".format(RETAILER.lower(), CLIENT.lower())
itemMasterTableName = "{}.hub_retailer_item".format(databaseName)
itemDescriptionTableName = "{}.sat_retailer_item".format(databaseName)

topFiveSellingItemsHormel = sqlContext.read.table(itemMasterTableName).alias("HRI")\
                            .join(sqlContext.read.table(itemDescriptionTableName).alias("SRI"), 
                                  pyf.col("HRI.HUB_RETAILER_ITEM_HK") == pyf.col("SRI.HUB_RETAILER_ITEM_HK"), "inner")\
                            .select("HRI.RETAILER_ITEM_ID", "SRI.RETAILER_ITEM_DESC")\
                            .filter("HRI.RETAILER_ITEM_ID in (551801636, 550387850, 9271128, 553484334, 9232464)")

display(topFiveSellingItemsHormel)

# COMMAND ----------


