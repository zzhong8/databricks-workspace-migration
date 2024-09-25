# Databricks notebook source
from pyspark.sql import functions as pyf
from pyspark.sql.types import *
import pandas as pd
import datetime

# COMMAND ----------

sample_UPCs = spark.read.format('csv')\
    .options(header='true', inferSchema='true')\
    .load('/mnt/prod-ro/artifacts/reference/Sample_UPCs.csv')

sample_UPCs.count()

# COMMAND ----------

# sample_retail_item_IDs = spark.read.format('csv')\
#     .options(header='false', inferSchema='true')\
#     .load('/mnt/prod-ro/artifacts/reference/tysonhillshire_missing_item_list.csv')

# sample_retail_item_IDs.count()

# COMMAND ----------

# Look up join keys
productTableName = "bobv2.product"
productViewName = "bobv2.vw_bobv2_product"

# COMMAND ----------

sample_retail_item_IDs = sample_UPCs\
                         .join(sqlContext.read.table(productTableName).alias("PTN"),
                         pyf.col("UPC") == pyf.col("PTN.InnerBarCode"), "inner")\
                            .select("UPC", "PTN.ProductId")

sample_retail_item_IDs = sample_retail_item_IDs\
                         .join(sqlContext.read.table(productViewName).alias("PVN"),
                         pyf.col("PTN.ProductId") == pyf.col("PVN.ProductId"), "inner")\
                            .select("UPC", "PVN.RefExternal").distinct()

sample_retail_item_IDs.count()

# COMMAND ----------

display(sample_retail_item_IDs)

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

CLIENT = 'CLOROX'

#LOOK UP HASH KEYS
databaseName = "{}_{}_dv".format(RETAILER.lower(), CLIENT.lower())
itemMasterTableName = "{}.hub_retailer_item".format(databaseName)
itemDescriptionTableName = "{}.sat_retailer_item".format(databaseName)

sampleCloroxItem = sqlContext.read.table(itemMasterTableName).alias("HRI")\
                   .join(sqlContext.read.table(itemDescriptionTableName).alias("SRI"), 
                         pyf.col("HRI.HUB_RETAILER_ITEM_HK") == pyf.col("SRI.HUB_RETAILER_ITEM_HK"), "inner")\
                   .select("HRI.RETAILER_ITEM_ID", "SRI.RETAILER_ITEM_DESC")\
                   .filter("HRI.RETAILER_ITEM_ID in (557188137)")

display(sampleCloroxItem)

# COMMAND ----------



# COMMAND ----------

CLIENT = 'TYSONHILLSHIRE'

#LOOK UP HASH KEYS
databaseName = "{}_{}_dv".format(RETAILER.lower(), CLIENT.lower())
itemMasterTableName = "{}.hub_retailer_item".format(databaseName)
itemDescriptionTableName = "{}.sat_retailer_item".format(databaseName)

sampleTysonHillshireItems = sqlContext.read.table(itemMasterTableName).alias("HRI")\
                            .join(sqlContext.read.table(itemDescriptionTableName).alias("SRI"), 
                                  pyf.col("HRI.HUB_RETAILER_ITEM_HK") == pyf.col("SRI.HUB_RETAILER_ITEM_HK"), "inner")\
                            .select("HRI.RETAILER_ITEM_ID", "SRI.RETAILER_ITEM_DESC")\
                            .filter("HRI.RETAILER_ITEM_ID in (551706167, 9169421)")

display(sampleTysonHillshireItems)

# COMMAND ----------

CLIENT = 'CAMPBELLSSNACK'

#LOOK UP HASH KEYS
databaseName = "{}_{}_dv".format(RETAILER.lower(), CLIENT.lower())
itemMasterTableName = "{}.hub_retailer_item".format(databaseName)
itemDescriptionTableName = "{}.sat_retailer_item".format(databaseName)

sampleCampbellsSnackItems = sqlContext.read.table(itemMasterTableName).alias("HRI")\
                            .join(sqlContext.read.table(itemDescriptionTableName).alias("SRI"), 
                                  pyf.col("HRI.HUB_RETAILER_ITEM_HK") == pyf.col("SRI.HUB_RETAILER_ITEM_HK"), "inner")\
                            .select("HRI.RETAILER_ITEM_ID", "SRI.RETAILER_ITEM_DESC")\
                            .filter("HRI.RETAILER_ITEM_ID in (490737, 486566, 5100001467, 5100002421)")

display(sampleCampbellsSnackItems)

# COMMAND ----------


