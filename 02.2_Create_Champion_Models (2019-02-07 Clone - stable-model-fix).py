# Databricks notebook source
'''
The code change is pretty easy.  Instead of doing something like "df.write.format("parquet").mode("overwrite").save(...)",
we would just do:  df.insertInto(hiveTableName, , overwrite = False)
'''

# COMMAND ----------

from pyspark.sql import functions as pyf
from pyspark.sql.types import *
from pyspark.sql.window import Window
import datetime as dtm
import uuid

#CONSTANTS
current_datetime = dtm.datetime.now()

# COMMAND ----------

# Loading all variables and constants
dbutils.widgets.text("RETAILER_PARAM", "WALMART", "Retailer(s)")
dbutils.widgets.text("CLIENT_PARAM", "", "Client(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Rules
# MAGIC <p>
# MAGIC * You can run all clients for any one retailer
# MAGIC * You can run all clients for all retailers
# MAGIC * You can NOT list multiple retailers and multiple clients
# MAGIC * To run multiple clients you must have only ONE retailer

# COMMAND ----------

RETAILER_PARAM = dbutils.widgets.get("RETAILER_PARAM").upper().strip()
CLIENT_PARAM = [client.strip() for client in dbutils.widgets.get("CLIENT_PARAM").upper().split(",") if client.strip() != ""]

retailer_client_list = []

if RETAILER_PARAM != ""  and len(CLIENT_PARAM) > 0:
    # Create list of retailer/client combinations and run these combinations
    retailer_client_list.extend([(RETAILER_PARAM, client) for client in CLIENT_PARAM])
    
elif RETAILER_PARAM == ""  and len(CLIENT_PARAM) > 0:
    raise Exception("You must have a retailer specified for your client(s)")
    
elif RETAILER_PARAM == ""  and len(CLIENT_PARAM) == 0:
    # Get a list of all the clients under each retailer in the training_results directory in order to run every retailer/client combination
    all_training_results = spark.read.parquet("/mnt/artifacts/training_results/")
    retailer_client_list.extend([(row.RETAILER, row.CLIENT) for row in all_training_results.select("RETAILER","CLIENT").distinct().collect()])

elif RETAILER_PARAM != ""  and len(CLIENT_PARAM) == 0:
    # Get a list of all the clients under this retailer in order to run them
    all_training_results = spark.read.parquet("/mnt/artifacts/training_results/").filter("RETAILER == '{}'".format(RETAILER_PARAM))
    retailer_client_list.extend([(row.RETAILER, row.CLIENT) for row in all_training_results.select("RETAILER","CLIENT").distinct().collect()])

else:
    raise Exception("Something went wrong here. Call Hugh!")

print(retailer_client_list)

# COMMAND ----------

# Collect all current champions and extract the schema for controlling the fields made available
current_champions = spark.sql("SELECT * FROM retail_forecast_engine.champion_models limit 1")
schema_champion = [element.name for element in current_champions.schema if element.name != "MODEL_PATH"]

# Collect schema column names to filter the model training results table
# Omit the MODEL_PATH variable as it doesn't exist in the model training results table

# COMMAND ----------

def create_champion_logic(schema_champion, retailer, client):
    working_data = spark.read.parquet("/mnt/artifacts/training_results/")\
    .filter("RETAILER = '{}' and CLIENT = '{}'".format(retailer, client))\
    .withColumn("MADE_CHAMPION_DATE", pyf.lit(current_datetime))\
    .withColumn("TEST_SET_MSE_PERFORMANCE", pyf.col("METRICS.mse_test"))\
    .withColumn("COLUMN_NAMES", pyf.array_join("COLUMN_NAMES",","))\
    .filter("MODEL_ID is not null")\
    .select(*schema_champion)\
    .withColumn("RETAILER", pyf.upper(pyf.col("RETAILER")))\
    .withColumn("CLIENT", pyf.upper(pyf.col("CLIENT")))

    # Business Logic for 
    window_mse_rank = Window.partitionBy().orderBy("TEST_SET_MSE_PERFORMANCE")
    window_count_rank = Window.partitionBy().orderBy("TEST_DAYS_COUNT")

    window_in_parition_weighted_rank = Window.partitionBy("RETAILER_ITEM_ID","ORGANIZATION_UNIT_NUM","RETAILER","CLIENT")\
      .orderBy(pyf.col("TEST_DAYS_COUNT").desc())

    non_zero_error = 0.000001
    df_mse_ranked = working_data.withColumn("mse_rank", 1 - (pyf.percent_rank().over(window_mse_rank) + non_zero_error))
    df_counts_ranked = df_mse_ranked.withColumn("count_rank", (pyf.percent_rank().over(window_count_rank) + non_zero_error))

    weight_mse = 0.5
    weight_count = 0.5

    df_final_rank = df_counts_ranked.withColumn( "weighted_rank", 
                                                (pyf.col("mse_rank") * weight_mse)\
                                                + (pyf.col("count_rank") * weight_count)
                                               )

    df_new_champion = df_final_rank\
      .withColumn("in_partition_rank", pyf.row_number().over(window_in_parition_weighted_rank))\
      .filter("in_partition_rank == 1")\
      .select(*schema_champion)\
      .alias("new")
    
    return df_new_champion

# COMMAND ----------

for retailer_client_combination in retailer_client_list:
    retailer = retailer_client_combination[0]
    client = retailer_client_combination[1]

    df_new_champions = create_champion_logic(schema_champion, retailer, client)
    # TODO: Do we enable auditing - i.e. recording which champions were added, changed, or deleted?
    df_new_champions\
      .coalesce(25)\
      .write\
      .mode("overwrite")\
      .format("parquet")\
      .save("/mnt/artifacts/champion_models/retailer=walmart/client=starbucks_test")
    

# COMMAND ----------

display(df_new_champions.select("COLUMN_NAMES").distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Logging Replacements

# COMMAND ----------

# # Write out those that aren't the best and log them
# # TODO: Consider writing out those that are completely gone from champion model
# # TODO: Consider writing out those that are new to the champions file
# champions_being_replaced = current_champions.select(
#   "RETAILER_ITEM_ID","ORGANIZATION_UNIT_NUM","RETAILER","CLIENT", pyf.col("MADE_CHAMPION_DATE").alias("MADE_CHAMPION_DATE_DROPPED"), pyf.col("MODEL_ID").alias("MODEL_ID_DROPPED")
#   )\
#   .join(
#     df_new_champion.select("RETAILER_ITEM_ID","ORGANIZATION_UNIT_NUM","RETAILER","CLIENT", pyf.col("MODEL_ID").alias("MODEL_ID_REPLACEMENT")),
#     ["RETAILER_ITEM_ID","ORGANIZATION_UNIT_NUM","RETAILER","CLIENT"],
#     "left"
#   )\
#   .filter("MODEL_ID_REPLACEMENT != MODEL_ID_DROPPED")

# champions_being_replaced\
#   .write\
#   .mode("append")\
#   .format("parquet")\
#   .save("/mnt/artifacts/audit/champion_changes/{year}/{month}/{day}/{hour}/{minute}".format(
#     year = current_datetime.year, month = current_datetime.month, day = current_datetime.day, 
#     hour = current_datetime.hour, minute = current_datetime.minute
#   ))

# COMMAND ----------

display(df_new_champions.describe())

# COMMAND ----------


