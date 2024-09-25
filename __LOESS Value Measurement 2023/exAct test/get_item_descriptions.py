# Databricks notebook source
# MAGIC %md
# MAGIC # get_item_descriptions

# COMMAND ----------

from pyspark.sql.functions import lit
from functools import reduce

# COMMAND ----------

# Set up the configuration lookup
query = f"""
    SELECT 
    mdm_country_id,
    mdm_holding_id,
    mdm_banner_id,
    mdm_client_id,
    mdm_client_nm,
    mdm_banner_nm,
    epos_datavault_db_nm
    FROM acosta_retail_analytics_im.interventions_retailer_client_config 
    where mdm_country_nm = 'uk'
"""
df_stats = spark.sql(query).toPandas()
df_stats

# COMMAND ----------

# Filter for valid configurations
mask = (~df_stats["mdm_client_nm"].isin(["GeneralMillsUK"])) & (df_stats["epos_datavault_db_nm"]!="horizon_sainsburys_perfettivanmelle_uk_dv")
db_list = df_stats.loc[mask, "epos_datavault_db_nm"].unique()

df_list = []

for db in db_list:
    try:
        if "nestle" in db:
            query = f"""
                select hri.RETAILER_ITEM_ID as retailer_item_id,
                sri.RETAILER_ITEM_DESC as retailer_item_desc
                from {db}.sat_retailer_item as sri
                inner join {db}.hub_retailer_item as hri ON sri.HUB_RETAILER_ITEM_HK = hri.HUB_RETAILER_ITEM_HK
                """
            df = spark.sql(query)
        else:
            query = f"""
                SELECT retailer_item_id,
                retailer_item_desc 
                FROM {db}.sat_retailer_item
                """
            df = spark.sql(query)
        df = df.withColumn("epos_datavault_db_nm", lit(db))
        df_list.append(df)
    except:
        print("Unable to retrieve data from {}".format(db))

final_df = reduce(lambda x, y: x.union(y), df_list)

final_df.show()

# COMMAND ----------

final_df.count()

# COMMAND ----------

final_df.write.format('delta')\
    .mode('overwrite')\
    .option('mergeSchema', 'true')\
    .save(f'/mnt/processed/davis/value_measurement/uk_item_descriptions')
